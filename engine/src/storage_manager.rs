//! Coordinated entry points for invocation commits and administrative DDL.

use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::authorization::{
    Effect, MAX_MEMBERSHIP_EDGES, Membership, Operation, Subject, resolve_role_closure,
};
use crate::catalog::{
    CatalogField, CatalogRecord, PolicyTarget, WireType, decode_builtin_scalar_type,
    decode_policy_target, decode_registered_envelope, encode_envelope, encode_policy_target,
    family_schema,
};
use crate::commit::{
    CatalogWrite, CommitError, IndexWrite, RowWrite, TableTruncateCommitRequest,
    commit_authorized_column_drop_ddl_with_fences, commit_authorized_column_rename_ddl_with_fences,
    commit_authorized_constraint_ddl_with_fences, commit_authorized_foreign_key_ddl_with_fences,
    commit_authorized_identity_ddl_with_fences, commit_authorized_index_ddl_with_fences,
    commit_authorized_membership_ddl_with_fences, commit_authorized_object_grant_ddl_with_fences,
    commit_authorized_policy_ddl_with_fences, commit_authorized_schema_create_ddl_with_fences,
    commit_authorized_schema_drop_ddl_with_fences, commit_authorized_schema_owner_ddl_with_fences,
    commit_authorized_schema_rename_ddl_with_fences,
    commit_authorized_system_privilege_ddl_with_fences,
    commit_authorized_table_create_ddl_with_fences, commit_authorized_table_drop_ddl_with_fences,
    commit_authorized_table_owner_ddl_with_fences, commit_authorized_table_rename_ddl_with_fences,
    commit_authorized_table_truncate_ddl_with_fences, commit_catalog_and_audit_with_fences,
    record_aborted_outcome, validate_table_truncate_ddl_authority,
};
use crate::credentials::{
    MAX_TOKEN_LABEL_BYTES, TOKEN_SECRET_BYTES, decode_verifier_record, encode_new_verifier_record,
    encode_revoked_verifier_record, now_unix_micros,
};
use crate::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use crate::index_locator::IndexLocator;
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::metadata_discovery::{
    InformationSchemaColumnsCursor, InformationSchemaColumnsScanPage,
    InformationSchemaSchemataPage, InformationSchemaTablesCursor, InformationSchemaTablesScanPage,
};
use crate::outcome::{InvocationOutcome, lookup_outcome};
use crate::row_record::RowRecord;
use crate::schema::{
    AUDIT, CATALOG, ENGINE_META, INDEXES, INTERACTIVE_TXN_RECOVERY_MAGIC,
    INTERACTIVE_TXN_RECOVERY_PREFIX, OUTCOMES, ROW_INDEXES, ROWS, STATS,
};
use crate::snapshot::{MAX_CATALOG_FAMILY_SCAN, RowSnapshot};
use crate::startup::{StartupError, validate_persisted_records};
use crate::statistics::{
    COMMITTED_POPULATION_KEY_PREFIX, CommittedPopulationRecord, DEFAULT_ESTIMATE_CANDIDATES,
    MAX_COMMITTED_POPULATION_ROWS, MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS,
    MAX_CONFIGURABLE_ESTIMATE_CANDIDATES, StatisticsCollectionOutcome, StatisticsMaintenanceError,
    StatisticsRecordError,
};
use crate::transaction::{
    DEFAULT_PLANNER_ESTIMATE_REQUESTS, InvocationCoordination, InvocationTransaction,
    MAX_PLANNER_ESTIMATE_REQUESTS, TransactionError,
};
use crate::tuple::{ColumnSchema, ScalarType, encode_tuple};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableHandle};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use zeroize::Zeroize;

const TOKEN_REQUEST_WINDOW: Duration = Duration::from_secs(60);
const MAX_TRACKED_TOKEN_REQUEST_ACTORS: usize = 4096;
const MAX_AUTH_FAILURE_AUDITS_PER_TOKEN_PER_MINUTE: usize = 4;
const SUPERVISOR_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_OWN_INVOCATION_PAGE: usize = 128;
pub const MAX_ADMIN_INVOCATION_PAGE: usize = 128;
pub const MAX_ADMIN_AUDIT_PAGE: usize = 128;
/// Maximum number of schema records returned by one authenticated discovery
/// snapshot. Clients can request a fresh snapshot when this bound is reached.
pub const MAX_DISCOVERED_SCHEMAS: usize = 4096;
const MAX_DISCOVERED_SCHEMA_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_RELATIONS: usize = 4096;
const MAX_DISCOVERED_RELATION_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_COLUMNS: usize = 4096;
const MAX_DISCOVERED_COLUMN_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_INDEXES: usize = 4096;
const MAX_DISCOVERED_INDEX_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_CONSTRAINTS: usize = 4096;
const MAX_DISCOVERED_CONSTRAINT_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_GRANTS: usize = 4096;
const MAX_DISCOVERED_GRANT_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_POLICIES: usize = 4096;
const MAX_DISCOVERED_POLICY_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_DEPENDENCIES: usize = 4096;
pub const INTERACTIVE_TRANSACTION_MAX_PACKAGES: usize = 128;
pub const INTERACTIVE_TRANSACTION_MAX_ROWS: usize = 65_536;
pub const INTERACTIVE_TRANSACTION_MAX_WRITE_BYTES: usize = 64 * 1024 * 1024;
pub const INTERACTIVE_TRANSACTION_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const MAX_RETAINED_INTERACTIVE_TRANSACTION_RECORDS: usize = 4096;
const MAX_DISCOVERED_DEPENDENCY_BYTES: usize = 1024 * 1024;
const MAX_TABLE_TRUNCATE_ROWS: usize = 65_536;
const MAX_TABLE_TRUNCATE_INDEX_ENTRIES: usize = 65_536;
const MAX_TABLE_TRUNCATE_BYTES: usize = 64 * 1024 * 1024;
static TOKEN_REQUEST_LIMITERS: OnceLock<Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>> =
    OnceLock::new();
static ADMIN_CANCELLATION_LIMITERS: OnceLock<Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>> =
    OnceLock::new();
static AUTH_FAILURE_AUDIT_LIMITERS: OnceLock<Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>> =
    OnceLock::new();
static INVOCATION_REGISTRIES: OnceLock<Mutex<HashMap<usize, Weak<InvocationAdmissionState>>>> =
    OnceLock::new();
static UNCERTAIN_INVOCATION_REGISTRIES: OnceLock<
    Mutex<HashMap<usize, Arc<InvocationAdmissionState>>>,
> = OnceLock::new();

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
    pub max_active_analytical_invocations: usize,
    pub max_invocation_age: Duration,
    /// Maximum catalog records and row candidates scanned in one invocation.
    pub max_invocation_scan_work: usize,
    /// Cumulative storage-estimate allowance for Operational planner sessions.
    pub max_planner_estimate_requests: usize,
    /// Optional Analytical planner-estimate allowance; omission inherits the
    /// Operational value.
    pub max_analytical_planner_estimate_requests: Option<usize>,
    /// Maximum row candidates inspected by each Operational planner estimate.
    pub max_planner_estimate_candidates: usize,
    /// Optional Analytical per-estimate candidate limit.
    pub max_analytical_planner_estimate_candidates: Option<usize>,
    /// Maximum committed row keys inspected by one statistics maintenance pass.
    pub max_statistics_collection_rows: usize,
    pub max_token_lifecycle_requests_per_minute: usize,
    pub max_admin_cancellation_requests_per_minute: usize,
}

impl StorageManagerConfig {
    pub fn max_planner_estimate_requests_for(
        &self,
        workload_class: crate::supervisor::WorkloadClass,
    ) -> usize {
        match workload_class {
            crate::supervisor::WorkloadClass::Operational => self.max_planner_estimate_requests,
            crate::supervisor::WorkloadClass::Analytical => self
                .max_analytical_planner_estimate_requests
                .unwrap_or(self.max_planner_estimate_requests),
        }
    }

    pub fn max_planner_estimate_candidates_for(
        &self,
        workload_class: crate::supervisor::WorkloadClass,
    ) -> usize {
        match workload_class {
            crate::supervisor::WorkloadClass::Operational => self.max_planner_estimate_candidates,
            crate::supervisor::WorkloadClass::Analytical => self
                .max_analytical_planner_estimate_candidates
                .unwrap_or(self.max_planner_estimate_candidates),
        }
    }
}

#[cfg(test)]
mod statistics_collection_tests {
    use super::{
        DEFAULT_ESTIMATE_CANDIDATES, DEFAULT_PLANNER_ESTIMATE_REQUESTS, StorageManager,
        StorageManagerConfig,
    };
    use crate::catalog::{CatalogField, WireType, encode_envelope, family_schema};
    use crate::metadata::{EngineMetadata, Lifecycle};
    use crate::schema::{CATALOG, ENGINE_META, ROWS, STATS, create_tables};
    use crate::snapshot::RowSnapshot;
    use crate::statistics::{
        COMMITTED_POPULATION_KEY_PREFIX, CommittedPopulationRecord, StatisticsCollectionOutcome,
    };
    use redb::{Database, ReadableDatabase};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn planner_estimate_budget_is_class_scoped_and_inherits_by_default() {
        let defaults = StorageManagerConfig::default();
        assert_eq!(
            defaults
                .max_planner_estimate_requests_for(crate::supervisor::WorkloadClass::Operational),
            DEFAULT_PLANNER_ESTIMATE_REQUESTS
        );
        assert_eq!(
            defaults
                .max_planner_estimate_requests_for(crate::supervisor::WorkloadClass::Analytical),
            DEFAULT_PLANNER_ESTIMATE_REQUESTS
        );
        assert_eq!(
            defaults
                .max_planner_estimate_candidates_for(crate::supervisor::WorkloadClass::Operational),
            DEFAULT_ESTIMATE_CANDIDATES
        );
        assert_eq!(
            defaults
                .max_planner_estimate_candidates_for(crate::supervisor::WorkloadClass::Analytical),
            DEFAULT_ESTIMATE_CANDIDATES
        );
        let elevated = StorageManagerConfig {
            max_planner_estimate_requests: 32,
            max_analytical_planner_estimate_requests: Some(1024),
            max_planner_estimate_candidates: 1024,
            max_analytical_planner_estimate_candidates: Some(65_536),
            max_statistics_collection_rows: 65_536,
            ..defaults
        };
        assert_eq!(
            elevated
                .max_planner_estimate_requests_for(crate::supervisor::WorkloadClass::Operational),
            32
        );
        assert_eq!(
            elevated
                .max_planner_estimate_requests_for(crate::supervisor::WorkloadClass::Analytical),
            1024
        );
        assert_eq!(
            elevated
                .max_planner_estimate_candidates_for(crate::supervisor::WorkloadClass::Operational),
            1024
        );
        assert_eq!(
            elevated
                .max_planner_estimate_candidates_for(crate::supervisor::WorkloadClass::Analytical),
            65_536
        );
        assert_eq!(elevated.max_statistics_collection_rows, 65_536);
    }

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    fn database_with_rows(
        row_ids: &[u128],
    ) -> Result<(std::path::PathBuf, Database), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!(
            "prdb-statistics-{}-{}.redb",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        let database = Database::create(&path)?;
        let table_id = [0x44; 16];
        let write = database.begin_write()?;
        create_tables(&write)?;
        {
            let key = [5u16.to_be_bytes().as_slice(), &[1], &table_id].concat();
            let fields = [
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: [0x55; 16].to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ];
            let payload = encode_envelope(
                &family_schema(5).ok_or("table family schema missing")?,
                &key,
                &fields,
            )?;
            write
                .open_table(CATALOG)?
                .insert(key.as_slice(), payload.as_slice())?;
            let mut rows = write.open_table(ROWS)?;
            for row_id in row_ids {
                let mut key = Vec::with_capacity(32);
                key.extend_from_slice(&table_id);
                key.extend_from_slice(&row_id.to_be_bytes());
                rows.insert(key.as_slice(), b"opaque row payload".as_slice())?;
            }
            let metadata = EngineMetadata {
                database_id: [0x66; 16],
                catalog_version: 2,
                expression_version: 1,
                builtin_semantics_version: 1,
                commit_sequence: 1,
                lifecycle: Lifecycle::Ready,
            }
            .encode()?;
            write
                .open_table(ENGINE_META)?
                .insert(&b"format"[..], metadata.as_slice())?;
        }
        write.commit()?;
        Ok((path, database))
    }

    #[test]
    fn committed_population_scheduler_cycles_table_catalog_without_listing_all_tables()
    -> Result<(), Box<dyn std::error::Error>> {
        let (path, database) = database_with_rows(&[])?;
        let storage =
            StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
        let table_id = [0x44; 16];
        let mut cursor = None;
        assert_eq!(
            crate::supervisor_ipc::run_statistics_maintenance_tick(&storage, &mut cursor)?,
            crate::supervisor_ipc::StatisticsMaintenanceTickOutcome::Collected(
                StatisticsCollectionOutcome::Exact
            )
        );
        assert_eq!(cursor, Some(table_id));
        let mut stats_key = COMMITTED_POPULATION_KEY_PREFIX.to_vec();
        stats_key.extend_from_slice(&table_id);
        let read = database.begin_read()?;
        let stats = read.open_table(STATS)?;
        let value = stats
            .get(stats_key.as_slice())?
            .ok_or("scheduled statistics record missing")?;
        assert_eq!(
            CommittedPopulationRecord::decode_record(table_id, value.value())?,
            CommittedPopulationRecord::Exact {
                table_id,
                snapshot_sequence: 1,
                rows: 0,
            }
        );
        drop(value);
        drop(stats);
        drop(read);
        assert_eq!(
            crate::supervisor_ipc::run_statistics_maintenance_tick(&storage, &mut cursor)?,
            crate::supervisor_ipc::StatisticsMaintenanceTickOutcome::FreshRecord,
            "a fresh persisted population avoids another full bounded collection"
        );
        assert_eq!(
            storage.next_committed_population_table_id(cursor)?,
            Some(table_id),
            "the cursor wraps after the last table"
        );
        drop(storage);
        drop(database);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn pinned_snapshot_accepts_only_its_fresh_committed_population_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_id = [0x44; 16];
        let (path, database) = database_with_rows(&[1, 2, 3])?;
        let storage =
            StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
        assert_eq!(
            storage.collect_committed_table_population(table_id)?,
            StatisticsCollectionOutcome::Exact
        );

        let pinned = RowSnapshot::new(database.begin_read()?);
        assert_eq!(
            pinned.committed_population_record(table_id)?,
            Some(CommittedPopulationRecord::Exact {
                table_id,
                snapshot_sequence: 1,
                rows: 3,
            })
        );

        let write = database.begin_write()?;
        let metadata = EngineMetadata {
            database_id: [0x66; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 2,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        write
            .open_table(ENGINE_META)?
            .insert(&b"format"[..], metadata.as_slice())?;
        write.commit()?;

        assert_eq!(
            pinned.committed_population_record(table_id)?,
            Some(CommittedPopulationRecord::Exact {
                table_id,
                snapshot_sequence: 1,
                rows: 3,
            }),
            "a retained invocation snapshot keeps its own fresh record"
        );
        let current = RowSnapshot::new(database.begin_read()?);
        assert_eq!(
            current.committed_population_record(table_id)?,
            None,
            "a later engine commit makes the old global record unavailable"
        );
        drop(current);
        drop(pinned);
        drop(storage);
        drop(database);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn committed_population_collection_is_bounded_and_snapshot_tagged()
    -> Result<(), Box<dyn std::error::Error>> {
        let (path, database) = database_with_rows(&[1, 2])?;
        let storage =
            StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
        storage
            .admissions
            .statistics_collection_limit
            .store(3, Ordering::Release);
        let table_id = [0x44; 16];
        assert_eq!(
            storage.collect_committed_table_population(table_id)?,
            StatisticsCollectionOutcome::Exact
        );
        assert_eq!(
            storage.current_committed_population_record(table_id)?,
            Some(CommittedPopulationRecord::Exact {
                table_id,
                snapshot_sequence: 1,
                rows: 2,
            }),
            "a record is reusable only at its source commit sequence"
        );
        let mut stats_key = COMMITTED_POPULATION_KEY_PREFIX.to_vec();
        stats_key.extend_from_slice(&table_id);
        {
            let read = database.begin_read()?;
            let stats = read.open_table(STATS)?;
            let value = stats
                .get(stats_key.as_slice())?
                .ok_or("statistics record missing")?;
            assert_eq!(&value.value()[..4], b"CPR1");
            assert_eq!(value.value()[12], 1);
            assert_eq!(
                CommittedPopulationRecord::decode_record(table_id, value.value())?,
                CommittedPopulationRecord::Exact {
                    table_id,
                    snapshot_sequence: 1,
                    rows: 2,
                }
            );
        }

        let write = database.begin_write()?;
        {
            let mut rows = write.open_table(ROWS)?;
            for row_id in [3u128, 4] {
                let mut key = Vec::with_capacity(32);
                key.extend_from_slice(&table_id);
                key.extend_from_slice(&row_id.to_be_bytes());
                rows.insert(key.as_slice(), b"opaque row payload".as_slice())?;
            }
            let metadata = EngineMetadata {
                database_id: [0x66; 16],
                catalog_version: 2,
                expression_version: 1,
                builtin_semantics_version: 1,
                commit_sequence: 2,
                lifecycle: Lifecycle::Ready,
            }
            .encode()?;
            write
                .open_table(ENGINE_META)?
                .insert(&b"format"[..], metadata.as_slice())?;
        }
        write.commit()?;
        assert_eq!(
            storage.current_committed_population_record(table_id)?,
            None,
            "a later committed snapshot makes the stored population stale"
        );
        let prior_record = {
            let read = database.begin_read()?;
            let stats = read.open_table(STATS)?;
            let value = stats
                .get(stats_key.as_slice())?
                .ok_or("prior statistics record missing")?;
            CommittedPopulationRecord::decode_record(table_id, value.value())?
        };
        assert_eq!(prior_record.snapshot_sequence(), 1);
        assert_eq!(
            EngineMetadata::decode(
                database
                    .begin_read()?
                    .open_table(ENGINE_META)?
                    .get(&b"format"[..])?
                    .ok_or("engine metadata missing")?
                    .value()
            )?
            .commit_sequence,
            2,
            "the tagged record must now be stale"
        );
        assert_eq!(
            storage.collect_committed_table_population(table_id)?,
            StatisticsCollectionOutcome::Unknown
        );
        assert_eq!(
            storage.current_committed_population_record(table_id)?,
            Some(CommittedPopulationRecord::Unknown {
                table_id,
                snapshot_sequence: 2,
            }),
            "a fresh over-limit record is reusable as Unknown, never as a partial count"
        );
        {
            let read = database.begin_read()?;
            let stats = read.open_table(STATS)?;
            let value = stats
                .get(stats_key.as_slice())?
                .ok_or("unknown record missing")?;
            assert_eq!(value.value()[12], 0, "partial counts must not be persisted");
            assert_eq!(
                CommittedPopulationRecord::decode_record(table_id, value.value())?,
                CommittedPopulationRecord::Unknown {
                    table_id,
                    snapshot_sequence: 2,
                }
            );
        }
        drop(storage);
        drop(database);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn committed_population_collection_uses_its_configured_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let rows = (1u64..=16_385).map(u128::from).collect::<Vec<_>>();
        let (path, database) = database_with_rows(&rows)?;
        let storage = StorageManager::from_validated_database(
            &database,
            StorageManagerConfig {
                max_statistics_collection_rows: 32_768,
                ..StorageManagerConfig::default()
            },
        )?;
        let table_id = [0x44; 16];

        assert_eq!(
            storage.collect_committed_table_population(table_id)?,
            StatisticsCollectionOutcome::Exact
        );
        assert_eq!(
            storage.current_committed_population_record(table_id)?,
            Some(CommittedPopulationRecord::Exact {
                table_id,
                snapshot_sequence: 1,
                rows: 16_385,
            })
        );

        drop(storage);
        drop(database);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn committed_population_collection_limit_rejects_invalid_configuration()
    -> Result<(), Box<dyn std::error::Error>> {
        let (path, database) = database_with_rows(&[])?;
        for limit in [
            0,
            crate::statistics::MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS as usize + 1,
        ] {
            let result = StorageManager::from_validated_database(
                &database,
                StorageManagerConfig {
                    max_statistics_collection_rows: limit,
                    ..StorageManagerConfig::default()
                },
            );
            assert!(matches!(
                result,
                Err(super::StorageManagerInitError::Config(_))
            ));
        }
        drop(database);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn statistics_collection_admission_is_shared_across_manager_clones()
    -> Result<(), Box<dyn std::error::Error>> {
        let (path, database) = database_with_rows(&[])?;
        let storage =
            StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
        let clone = storage.clone();
        let active = storage
            .admissions
            .statistics_collection_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
        assert!(active.is_ok());
        assert!(matches!(
            clone.collect_committed_table_population([0x44; 16]),
            Err(crate::statistics::StatisticsMaintenanceError::Busy)
        ));
        storage
            .admissions
            .statistics_collection_active
            .store(false, Ordering::Release);
        assert!(matches!(
            clone.collect_committed_table_population([0x44; 16])?,
            StatisticsCollectionOutcome::Exact
        ));
        drop(clone);
        drop(storage);
        drop(database);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn operational_row_commit_completes_while_population_collection_is_admitted()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::commit::{RowWrite, commit_rows};
        use crate::row_record::RowRecord;
        use std::time::{Duration, Instant};

        let elevated_maintenance_limit = 65_536;
        let row_ids = (1..=elevated_maintenance_limit as u128).collect::<Vec<_>>();
        let (path, database) = database_with_rows(&row_ids)?;
        let storage = StorageManager::from_validated_database(
            &database,
            StorageManagerConfig {
                max_statistics_collection_rows: elevated_maintenance_limit,
                ..StorageManagerConfig::default()
            },
        )?;
        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let collector = storage.clone();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let collector_barrier = std::sync::Arc::clone(&barrier);
            let collection = scope.spawn(move || {
                collector_barrier.wait();
                collector.collect_committed_table_population([0x44; 16])
            });
            barrier.wait();

            let deadline = Instant::now() + Duration::from_secs(2);
            while !storage
                .admissions
                .statistics_collection_active
                .load(Ordering::Acquire)
                && Instant::now() < deadline
            {
                std::thread::yield_now();
            }
            assert!(
                storage
                    .admissions
                    .statistics_collection_active
                    .load(Ordering::Acquire),
                "maintenance did not acquire its independent admission slot"
            );

            let completed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let completed_during_collection =
                std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            std::thread::scope(
                |writers| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let start_writers = std::sync::Arc::new(std::sync::Barrier::new(9));
                    let mut handles = Vec::new();
                    for worker in 0..8u64 {
                        let start_writers = std::sync::Arc::clone(&start_writers);
                        let completed = std::sync::Arc::clone(&completed);
                        let completed_during_collection =
                            std::sync::Arc::clone(&completed_during_collection);
                        let storage = &storage;
                        let database = &database;
                        handles.push(writers.spawn(
                            move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                                start_writers.wait();
                                for offset in 0..24u64 {
                                    let sequence = worker * 24 + offset;
                                    let row_id = 70_000u128 + sequence as u128;
                                    let mut row_key = [0; 32];
                                    row_key[..16].copy_from_slice(&[0x44; 16]);
                                    row_key[16..].copy_from_slice(&row_id.to_be_bytes());
                                    let row_payload = RowRecord {
                                        row_version: 1,
                                        payload: Vec::new(),
                                    }
                                    .encode()?;
                                    let mut invocation_id = [0x71; 16];
                                    invocation_id[..8].copy_from_slice(&sequence.to_be_bytes());
                                    commit_rows(
                                        database,
                                        invocation_id,
                                        [0x72; 16],
                                        i64::MAX,
                                        &[RowWrite {
                                            key: row_key,
                                            expected_version: None,
                                            payload: Some(row_payload),
                                        }],
                                    )?;
                                    completed.fetch_add(1, Ordering::Relaxed);
                                    if storage
                                        .admissions
                                        .statistics_collection_active
                                        .load(Ordering::Acquire)
                                    {
                                        completed_during_collection.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Ok(())
                            },
                        ));
                    }
                    start_writers.wait();
                    for handle in handles {
                        handle.join().map_err(|_| "operational writer panicked")??;
                    }
                    Ok(())
                },
            )
            .map_err(|error| std::io::Error::other(error.to_string()))?;
            assert_eq!(completed.load(Ordering::Relaxed), 192);
            assert!(
                completed_during_collection.load(Ordering::Relaxed) > 0,
                "no operational commit completed while elevated maintenance was admitted"
            );
            let result = collection
                .join()
                .map_err(|_| "statistics collector panicked")??;
            assert!(matches!(
                result,
                StatisticsCollectionOutcome::Exact | StatisticsCollectionOutcome::Unknown
            ));
            Ok(())
        })?;

        drop(storage);
        drop(database);
        std::fs::remove_file(path)?;
        Ok(())
    }
}

impl Default for StorageManagerConfig {
    fn default() -> Self {
        Self {
            ddl_fence_timeout: Duration::from_secs(5),
            max_active_invocations: 128,
            max_active_analytical_invocations: 8,
            max_invocation_age: Duration::from_secs(15 * 60),
            max_invocation_scan_work: crate::transaction::MAX_INVOCATION_SCAN_WORK,
            max_planner_estimate_requests: DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_analytical_planner_estimate_requests: None,
            max_planner_estimate_candidates: DEFAULT_ESTIMATE_CANDIDATES,
            max_analytical_planner_estimate_candidates: None,
            max_statistics_collection_rows: MAX_COMMITTED_POPULATION_ROWS as usize,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageManagerConfigError {
    ZeroActiveInvocationLimit,
    ZeroAnalyticalInvocationLimit,
    ZeroInvocationAge,
    ZeroInvocationScanWorkLimit,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationStatusConsistency {
    BestEffort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationStatus {
    Active {
        started_at_us: i64,
        deadline_at_us: Option<i64>,
        cancellation_requested: bool,
    },
    Committing {
        started_at_us: i64,
        deadline_at_us: Option<i64>,
        cancellation_requested: bool,
    },
    Terminal(InvocationOutcome),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationStatusSnapshot {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub status: InvocationStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnInvocationStatus {
    pub invocation_id: [u8; 16],
    pub status: InvocationStatus,
    pub staged_rows: usize,
    pub staged_value_bytes: usize,
}

/// Opaque continuation for one owner's live invocation page sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationListCursor {
    principal_id: [u8; 16],
    owner_generation: u128,
    after_invocation_id: [u8; 16],
}

impl InvocationListCursor {
    /// Serialize the owner-bound live-invocation cursor as lowercase hex.
    pub fn to_wire(&self) -> String {
        let mut bytes = [0; 49];
        bytes[0] = 1;
        bytes[1..17].copy_from_slice(&self.principal_id);
        bytes[17..33].copy_from_slice(&self.owner_generation.to_be_bytes());
        bytes[33..49].copy_from_slice(&self.after_invocation_id);
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
        output
    }

    /// Parse a canonical wire cursor. The owner and generation are checked
    /// again by `list_own_invocations` on every request.
    pub fn from_wire(value: &str) -> Option<Self> {
        if value.len() != 98 {
            return None;
        }
        let mut bytes = [0; 49];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = lower_hex_value(pair[0])?;
            let low = lower_hex_value(pair[1])?;
            bytes[index] = high << 4 | low;
        }
        if bytes[0] != 1 {
            return None;
        }
        let principal_id = bytes[1..17].try_into().ok()?;
        let owner_generation = u128::from_be_bytes(bytes[17..33].try_into().ok()?);
        let after_invocation_id = bytes[33..49].try_into().ok()?;
        if principal_id == [0; 16] || owner_generation == 0 || after_invocation_id == [0; 16] {
            return None;
        }
        Some(Self {
            principal_id,
            owner_generation,
            after_invocation_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationStatusPage {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub invocations: Vec<OwnInvocationStatus>,
    pub continuation: Option<InvocationListCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationStatus {
    pub invocation_id: [u8; 16],
    pub transaction_id: [u8; 16],
    pub principal_id: [u8; 16],
    pub workload_class: crate::supervisor::WorkloadClass,
    pub status: InvocationStatus,
    pub age_us: u64,
    pub staged_rows: usize,
    pub staged_value_bytes: usize,
    pub snapshot_pin_count: u8,
    pub cancellation_reason_code: Option<u16>,
}

/// Opaque continuation for an administrator's live runtime page sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationCursor {
    administrator_id: [u8; 16],
    registry_generation: u128,
    after_invocation_id: [u8; 16],
}

impl AdministrativeInvocationCursor {
    /// Serialize the process-local page cursor as lowercase hexadecimal.
    pub fn to_wire(&self) -> String {
        let mut bytes = [0; 49];
        bytes[0] = 1;
        bytes[1..17].copy_from_slice(&self.administrator_id);
        bytes[17..33].copy_from_slice(&self.registry_generation.to_be_bytes());
        bytes[33..49].copy_from_slice(&self.after_invocation_id);
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
        output
    }

    /// Parse a canonical wire cursor. Authorization and generation freshness
    /// are still checked by `list_runtime_invocations` on every page request.
    pub fn from_wire(value: &str) -> Option<Self> {
        if value.len() != 98 {
            return None;
        }
        let mut bytes = [0; 49];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = lower_hex_value(pair[0])?;
            let low = lower_hex_value(pair[1])?;
            bytes[index] = high << 4 | low;
        }
        if bytes[0] != 1 {
            return None;
        }
        let administrator_id = bytes[1..17].try_into().ok()?;
        let registry_generation = u128::from_be_bytes(bytes[17..33].try_into().ok()?);
        let after_invocation_id = bytes[33..49].try_into().ok()?;
        if administrator_id == [0; 16] || registry_generation == 0 || after_invocation_id == [0; 16]
        {
            return None;
        }
        Some(Self {
            administrator_id,
            registry_generation,
            after_invocation_id,
        })
    }
}

fn lower_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationPage {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub invocations: Vec<AdministrativeInvocationStatus>,
    pub pinned_snapshot_count: usize,
    pub oldest_snapshot_pin_age_us: u64,
    pub invocations_admitted_total: u64,
    pub invocation_conflicts_total: u64,
    pub admin_cancellations_accepted_total: u64,
    pub admin_cancellations_denied_total: u64,
    pub invocation_policy_denials_total: u64,
    pub invocation_staged_value_bytes_total: u64,
    pub invocation_commit_latency_buckets: [u64; 6],
    pub invocation_redb_write_acquisition_buckets: [u64; 6],
    pub outcome_maintenance_passes_total: u64,
    pub outcome_maintenance_failures_total: u64,
    pub outcomes_pruned_total: u64,
    pub fence_waits: Vec<crate::ddl_fence::DdlFenceWaitStatus>,
    pub fence_waits_truncated: bool,
    pub continuation: Option<AdministrativeInvocationCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationStatusError {
    InvalidRequest,
    InvalidPageLimit,
    InvalidCursor,
    StaleCursor,
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnerCancellationResult {
    Accepted,
    TooLate,
    Terminal(InvocationOutcome),
    Unknown,
}

impl std::fmt::Display for InvocationStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invocation status lookup failed: {self:?}")
    }
}

impl std::error::Error for InvocationStatusError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeInvocationListError {
    InvalidRequest,
    InvalidPageLimit,
    InvalidCursor,
    StaleCursor,
    Unauthorized,
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeAuditCursor {
    administrator_id: [u8; 16],
    after_event_id: [u8; 16],
}

impl AdministrativeAuditCursor {
    pub fn to_wire(&self) -> String {
        let mut output = String::with_capacity(66);
        for byte in std::iter::once(1)
            .chain(self.administrator_id)
            .chain(self.after_event_id)
        {
            output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
        output
    }

    pub fn from_wire(value: &str) -> Option<Self> {
        if value.len() != 66 {
            return None;
        }
        let mut bytes = [0; 33];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = lower_hex_value(pair[0])?;
            let low = lower_hex_value(pair[1])?;
            bytes[index] = high << 4 | low;
        }
        if bytes[0] != 1 {
            return None;
        }
        let administrator_id = bytes[1..17].try_into().ok()?;
        let after_event_id = bytes[17..33].try_into().ok()?;
        (administrator_id != [0; 16] && after_event_id != [0; 16]).then_some(Self {
            administrator_id,
            after_event_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeAuditEvent {
    pub event_id: [u8; 16],
    pub actor_id: Option<[u8; 16]>,
    pub correlation_id: [u8; 16],
    pub occurred_at_us: i64,
    pub action: AuditAction,
    pub result: AuditResult,
    pub target_ids: Vec<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeAuditPage {
    pub events: Vec<AdministrativeAuditEvent>,
    pub continuation: Option<AdministrativeAuditCursor>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdministrativeAuditListError {
    InvalidRequest,
    InvalidPageLimit,
    InvalidCursor,
    Unauthorized,
    Storage,
}

impl std::fmt::Display for AdministrativeAuditListError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "administrative audit listing failed: {self:?}")
    }
}

impl std::error::Error for AdministrativeAuditListError {}

impl std::fmt::Display for AdministrativeInvocationListError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "administrative invocation listing failed: {self:?}"
        )
    }
}

impl std::error::Error for AdministrativeInvocationListError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeCancellationResult {
    Accepted,
    TooLate,
    Terminal(InvocationOutcome),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeCancellationError {
    InvalidRequest,
    InvalidAudit,
    Unauthorized,
    RateLimited,
    AuditCapacityExceeded,
    Storage,
}

impl std::fmt::Display for AdministrativeCancellationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "administrative cancellation failed: {self:?}")
    }
}

impl std::error::Error for AdministrativeCancellationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TokenLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    TokenIdGenerationFailed,
    TokenUnavailable,
    EncodingFailed,
    Storage(String),
    RateLimited,
    Commit(CommitError),
}

impl std::fmt::Display for TokenLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "token lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for TokenLifecycleError {}

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
    fn catalog_code(self) -> u16 {
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

fn schema_lifecycle_from_table_error(error: TableLifecycleError) -> SchemaLifecycleError {
    match error {
        TableLifecycleError::Commit(error) => SchemaLifecycleError::Commit(error),
        TableLifecycleError::Storage => SchemaLifecycleError::Storage,
        _ => SchemaLifecycleError::Commit(CommitError::InvalidCatalog),
    }
}

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
pub struct TableColumnDefinition {
    pub name: String,
    pub scalar_type: ScalarType,
    pub nullable: bool,
    pub generated_uuid_identity: bool,
    pub default: Option<TableColumnDefault>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableColumnDefault {
    Literal(Option<crate::tuple::ScalarValue>),
    CurrentPrincipalId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub schema_id: [u8; 16],
    pub name: String,
    pub columns: Vec<TableColumnDefinition>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableForeignKeyDefinition {
    pub source_columns: Vec<String>,
    pub target_constraint_id: [u8; 16],
    /// Local referenced key columns for an atomic self-reference.
    pub target_columns: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableCheckDefinition {
    pub name: Option<String>,
    pub expression: crate::authorization::Expr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateForeignKeyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub source_table_id: [u8; 16],
    pub source_column_ids: Vec<[u8; 16]>,
    pub target_constraint_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateUniqueConstraintRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePrimaryKeyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
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
    fn code(self) -> u16 {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateCheckConstraintRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub name: Option<String>,
    pub expression: crate::authorization::Expr,
    pub dependency_column_ids: Vec<[u8; 16]>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyKind {
    Row,
    Cell,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePolicyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub name: String,
    pub kind: PolicyKind,
    pub operation: Operation,
    pub effect: Effect,
    pub recipients: Vec<Subject>,
    pub target_column_ids: Vec<[u8; 16]>,
    pub using_expression: Option<crate::authorization::Expr>,
    pub with_check_expression: Option<crate::authorization::Expr>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedPolicy {
    pub policy_id: [u8; 16],
    pub using_expression_id: Option<[u8; 16]>,
    pub with_check_expression_id: Option<[u8; 16]>,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropPolicyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub policy_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

struct PolicyDropWriteSet {
    table_id: [u8; 16],
    kind: u16,
    writes: Vec<CatalogWrite>,
    expression_ids: Vec<[u8; 16]>,
}

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
pub struct CreatedTable {
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedForeignKey {
    pub foreign_key_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedUniqueConstraint {
    pub constraint_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedPrimaryKey {
    pub constraint_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedCheckConstraint {
    pub constraint_id: [u8; 16],
    pub expression_id: [u8; 16],
    pub commit_sequence: u64,
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

fn table_lifecycle_commit_error(error: TableLifecycleError) -> CommitError {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoleMembershipChange {
    pub member_id: [u8; 16],
    pub role_id: [u8; 16],
    pub grant: bool,
}

#[derive(Clone, Copy)]
enum NewPrincipalKind {
    User,
    UserNoLogin,
    Role,
}

#[derive(Clone, Debug)]
pub struct TokenIssueRequest<'a> {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub user_id: [u8; 16],
    pub label: &'a str,
    pub expires_at_us: i64,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug)]
pub struct TokenRotationRequest<'a> {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub old_token_id: [u8; 16],
    pub label: &'a str,
    pub expires_at_us: i64,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

/// Non-secret metadata for one issued token. Verifier bytes are never exposed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenMetadata {
    pub token_id: [u8; 16],
    pub label: String,
    pub issued_at_us: i64,
    pub expires_at_us: i64,
    pub revoked: bool,
    pub revoked_at_us: Option<i64>,
}

/// A bounded page of token metadata ordered by token ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenMetadataPage {
    pub tokens: Vec<TokenMetadata>,
    pub next_after_token_id: Option<[u8; 16]>,
}

/// Non-secret identity context established by read-only gateway AUTH.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedQuerySession {
    connection_id: [u8; 16],
    token_id: [u8; 16],
    principal_id: [u8; 16],
    principal_name: String,
    authority_epoch: u64,
    role_ids: HashSet<[u8; 16]>,
}

struct ResolvedQueryIdentity {
    principal_id: [u8; 16],
    principal_name: String,
    authority_epoch: u64,
    role_ids: HashSet<[u8; 16]>,
}

impl AuthenticatedQuerySession {
    /// Opaque server-generated session identifier returned to the gateway
    /// client after AUTH succeeds.
    pub fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    pub fn principal_id(&self) -> [u8; 16] {
        self.principal_id
    }

    pub fn principal_name(&self) -> &str {
        &self.principal_name
    }

    pub fn authority_epoch(&self) -> u64 {
        self.authority_epoch
    }

    pub fn role_ids(&self) -> &HashSet<[u8; 16]> {
        &self.role_ids
    }

    pub(crate) fn token_id(&self) -> [u8; 16] {
        self.token_id
    }

    /// Create the staging owner from this authenticated identity. The
    /// connection ID is generated by storage and cannot be supplied by a
    /// network client.
    pub fn staging_owner(&self) -> crate::supervisor::StagingOwner {
        crate::supervisor::StagingOwner::authenticated(self.connection_id, self.principal_id)
    }
}

/// Newly issued bearer token. Call [`Self::take_secret`] once to transfer the
/// secret to the protected caller; any secret still held here is zeroized on
/// drop. This type intentionally has no `Debug` implementation.
pub struct IssuedToken {
    pub token_id: [u8; 16],
    pub commit_sequence: u64,
    secret: Option<[u8; TOKEN_SECRET_BYTES]>,
}

impl IssuedToken {
    pub fn take_secret(&mut self) -> Option<[u8; TOKEN_SECRET_BYTES]> {
        self.secret.take()
    }
}

impl Drop for IssuedToken {
    fn drop(&mut self) {
        if let Some(secret) = &mut self.secret {
            secret.zeroize();
        }
    }
}

/// Shares one per-table DDL fence manager across row and catalog operations for
/// a database handle.
#[derive(Clone)]
pub struct StorageManager<'db> {
    database: &'db Database,
    ddl_fences: DdlFenceCoordinator,
    max_active_invocations: usize,
    max_active_analytical_invocations: usize,
    admissions: Arc<InvocationAdmissionState>,
    token_requests: Arc<TokenRequestLimiter>,
    admin_cancellation_requests: Arc<TokenRequestLimiter>,
    authentication_failure_audits: Arc<TokenRequestLimiter>,
    max_invocation_age: Duration,
    max_invocation_scan_work: usize,
    max_planner_estimate_requests: usize,
    max_analytical_planner_estimate_requests: Option<usize>,
    max_planner_estimate_candidates: usize,
    max_analytical_planner_estimate_candidates: Option<usize>,
    interactive_transactions: InteractiveTransactionMap<'db>,
    interactive_begin_requests: InteractiveBeginRequestMap,
}

type InteractiveTransactionMap<'db> =
    Arc<Mutex<HashMap<[u8; 16], Arc<Mutex<InteractiveTransactionEntry<'db>>>>>>;
type InteractiveBeginRequestMap =
    Arc<Mutex<HashMap<([u8; 16], [u8; 16]), InteractiveBeginRequestRecord>>>;

#[derive(Clone, Copy)]
struct InteractiveBeginRequestRecord {
    transaction_id: [u8; 16],
    retain_until_us: i64,
}

struct InteractiveTransactionEntry<'db> {
    transaction_id: [u8; 16],
    controller_id: [u8; 16],
    outcome_retention_deadline_us: i64,
    transaction: Arc<Mutex<Option<InvocationTransaction<'db>>>>,
    state: InteractiveTransactionState,
    started_at: Instant,
    last_activity: Instant,
    packages: HashMap<[u8; 16], InteractivePackageRecord>,
}

struct InteractivePackageRecord {
    digest: [u8; 32],
    state: InteractivePackageState,
    result_lost: bool,
    actor: Option<InteractivePackageActorRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InteractivePackageActorRecord {
    token_id: [u8; 16],
    principal_id: [u8; 16],
    principal_name: String,
    authority_epoch: u64,
    role_ids: HashSet<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractivePackageState {
    Running,
    Completed,
    Failed,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InteractivePackageStatus {
    pub state: InteractivePackageState,
    pub result_lost: bool,
}

pub struct InteractivePackageRequest<'a> {
    pub token_id: [u8; 16],
    pub token_secret: &'a [u8; TOKEN_SECRET_BYTES],
    pub transaction_id: [u8; 16],
    pub package_id: [u8; 16],
    pub package_digest: [u8; 32],
    pub requested_scope: HashSet<(Operation, crate::authorization::AccessTarget)>,
    pub actor: Option<InteractivePackageActorProof<'a>>,
}

#[derive(Clone, Copy)]
pub struct InteractivePackageActorProof<'a> {
    pub expected_name: &'a str,
    pub token_id: [u8; 16],
    pub token_secret: &'a [u8; TOKEN_SECRET_BYTES],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractiveTransactionState {
    Active,
    AbortOnly,
    Committing,
    Committed,
    Aborted,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InteractiveTransactionBegin {
    pub transaction_id: [u8; 16],
    pub state: InteractiveTransactionState,
    pub idle_timeout_ms: u64,
    pub lifetime_ms: u64,
    pub max_package_bytes: u64,
    pub max_packages: u64,
}

#[derive(Debug)]
pub enum InteractiveTransactionError {
    Transaction(TransactionError),
    InvalidRequest,
    NotFound,
    NotOwner,
    Expired,
    AbortOnly,
    PackageLimit,
    DuplicatePackage,
    PackageConflict,
    InvalidActor,
    ConcurrentOperation,
    RegistryLimit,
    RegistryPoisoned,
}

impl From<TransactionError> for InteractiveTransactionError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl std::fmt::Display for InteractiveTransactionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "interactive transaction error: {self:?}")
    }
}

impl std::error::Error for InteractiveTransactionError {}

#[derive(Debug)]
pub enum InteractivePackageExecutionError<E> {
    Transaction(InteractiveTransactionError),
    Execution(E),
    Panicked,
}

/// One token-authenticated invocation owned by the trusted supervisor-facing
/// storage service. The transaction cannot be finalized through executor or
/// planner methods and is aborted if this owner drops it without a decision.
pub struct SupervisorInvocation<'db> {
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    role_ids: HashSet<[u8; 16]>,
    transaction: Arc<Mutex<Option<InvocationTransaction<'db>>>>,
    channels: Arc<Mutex<SupervisorChannelState>>,
    deadline: Option<std::time::Instant>,
    abort_on_drop: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct SupervisorAdmissionRequest {
    pub invocation_id: [u8; 16],
    pub token_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub deadline: Option<std::time::Instant>,
    pub workload_class: crate::supervisor::WorkloadClass,
}

#[derive(Default)]
struct SupervisorChannelState {
    active_worker_channels: usize,
    terminal_requested: bool,
    worker_exit_observing: bool,
    worker_exit_succeeded: bool,
}

struct SupervisorWorkerChannelLease {
    channels: Arc<Mutex<SupervisorChannelState>>,
}

#[derive(Debug)]
pub enum SupervisorInvocationError {
    Closed,
    WorkerChannelsActive,
    WorkerExitNotObserved,
    WorkerExitObservationActive,
    WorkerChannelIo(std::io::Error),
    WorkerProcessIo(std::io::Error),
    WorkerCgroupIo(std::io::Error),
    WorkerExited(std::process::ExitStatus),
    WorkerHandlerPanicked,
    Deadline(TransactionError),
    Transaction(TransactionError),
    Executor(crate::executor_ipc::ExecutorConnectionError),
    Planner(crate::planner_ipc::PlannerConnectionError),
    Supervisor(crate::supervisor_ipc::SupervisorConnectionError),
}

impl std::fmt::Display for SupervisorInvocationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "supervisor invocation failed: {self:?}")
    }
}

impl std::error::Error for SupervisorInvocationError {}

impl Drop for SupervisorWorkerChannelLease {
    fn drop(&mut self) {
        let mut channels = self
            .channels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        channels.active_worker_channels = channels.active_worker_channels.saturating_sub(1);
    }
}

impl SupervisorInvocation<'_> {
    pub fn invocation_id(&self) -> [u8; 16] {
        self.invocation_id
    }

    pub fn principal_id(&self) -> [u8; 16] {
        self.principal_id
    }

    pub fn role_ids(&self) -> &HashSet<[u8; 16]> {
        &self.role_ids
    }

    fn with_owned_transaction<T>(
        &self,
        operation: impl FnOnce(&mut InvocationTransaction<'_>) -> Result<T, TransactionError>,
    ) -> Result<T, SupervisorInvocationError> {
        let mut owner = match self.transaction.lock() {
            Ok(owner) => owner,
            Err(poisoned) => {
                let mut owner = poisoned.into_inner();
                if let Some(mut transaction) = owner.take() {
                    let _ = transaction.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        let transaction = owner.as_mut().ok_or(SupervisorInvocationError::Closed)?;
        operation(transaction).map_err(SupervisorInvocationError::Transaction)
    }

    /// Read one invocation-pinned page of schema metadata through the trusted
    /// storage discovery API. Executor channels remain separate and opaque.
    pub fn scan_information_schema_schemata(
        &self,
        page_limit: usize,
    ) -> Result<InformationSchemaSchemataPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_schemata(page_limit, None)
        })
    }

    /// Read one invocation-pinned page of table metadata through trusted
    /// storage discovery.
    pub fn scan_information_schema_tables(
        &self,
        page_limit: usize,
        continuation: Option<InformationSchemaTablesCursor>,
    ) -> Result<InformationSchemaTablesScanPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_tables(page_limit, continuation)
        })
    }

    /// Read one invocation-pinned page of column metadata through trusted
    /// storage discovery.
    pub fn scan_information_schema_columns(
        &self,
        page_limit: usize,
        continuation: Option<InformationSchemaColumnsCursor>,
    ) -> Result<InformationSchemaColumnsScanPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_columns(page_limit, continuation)
        })
    }

    fn enter_worker_channel(
        &self,
    ) -> Result<SupervisorWorkerChannelLease, SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        if channels.terminal_requested
            || channels.worker_exit_observing
            || channels.worker_exit_succeeded
        {
            return Err(SupervisorInvocationError::Closed);
        }
        channels.active_worker_channels = channels
            .active_worker_channels
            .checked_add(1)
            .ok_or(SupervisorInvocationError::Closed)?;
        Ok(SupervisorWorkerChannelLease {
            channels: Arc::clone(&self.channels),
        })
    }

    fn abort_owned_invocation(&self) {
        let mut owner = self
            .transaction
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut transaction) = owner.take() {
            let _ = transaction.abort();
        }
    }

    /// Poll cancellation and transaction age while worker channels are idle.
    /// A failed check durably aborts the invocation through its transaction.
    pub fn check_deadline(&self) -> Result<(), SupervisorInvocationError> {
        let mut owner = match self.transaction.lock() {
            Ok(owner) => owner,
            Err(poisoned) => {
                let mut owner = poisoned.into_inner();
                if let Some(mut transaction) = owner.take() {
                    let _ = transaction.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        let transaction = owner.as_mut().ok_or(SupervisorInvocationError::Closed)?;
        transaction
            .check_deadline()
            .map_err(SupervisorInvocationError::Deadline)?;
        if self
            .deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            transaction
                .abort()
                .map_err(SupervisorInvocationError::Deadline)?;
            return Err(SupervisorInvocationError::Deadline(
                TransactionError::Expired,
            ));
        }
        Ok(())
    }

    /// Check only hard age limits while a worker is draining cooperative
    /// cancellation. The executor gets a bounded window to poll cancellation
    /// and exit before the launcher forcefully reaps it.
    pub fn check_worker_deadline(&self) -> Result<(), SupervisorInvocationError> {
        let mut owner = match self.transaction.lock() {
            Ok(owner) => owner,
            Err(poisoned) => {
                let mut owner = poisoned.into_inner();
                if let Some(mut transaction) = owner.take() {
                    let _ = transaction.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        let transaction = owner.as_mut().ok_or(SupervisorInvocationError::Closed)?;
        transaction
            .check_worker_deadline()
            .map_err(SupervisorInvocationError::Deadline)?;
        if self
            .deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            transaction
                .abort()
                .map_err(SupervisorInvocationError::Deadline)?;
            return Err(SupervisorInvocationError::Deadline(
                TransactionError::Expired,
            ));
        }
        Ok(())
    }

    /// Whether cancellation has been accepted, including after an executor
    /// poll has already aborted and released the storage transaction.
    pub fn cancellation_requested(&self) -> Result<bool, SupervisorInvocationError> {
        let owner = self
            .transaction
            .lock()
            .map_err(|_| SupervisorInvocationError::Closed)?;
        let transaction = owner.as_ref().ok_or(SupervisorInvocationError::Closed)?;
        Ok(transaction.cancellation_requested())
    }

    /// Observe and reap a worker after its planner and executor channels have
    /// drained. A failed exit or process-inspection error durably aborts the
    /// invocation; the caller may request commit only after this returns Ok.
    pub fn observe_worker_exit(
        &self,
        worker: &mut std::process::Child,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        self.observe_worker_exit_inner(worker, None)
    }

    /// Observe a worker whose process was placed in `cgroup` before exec.
    /// The cgroup is killed and verified empty before successful exit can
    /// authorize terminal storage decisions.
    pub fn observe_worker_exit_in_cgroup(
        &self,
        worker: &mut std::process::Child,
        cgroup: crate::linux_sandbox::WorkerCgroup,
        cleanup_timeout: Duration,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        self.observe_worker_exit_inner(worker, Some((cgroup, cleanup_timeout)))
    }

    fn observe_worker_exit_inner(
        &self,
        worker: &mut std::process::Child,
        mut cgroup: Option<(crate::linux_sandbox::WorkerCgroup, Duration)>,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        {
            let mut channels = match self.channels.lock() {
                Ok(channels) => channels,
                Err(poisoned) => {
                    let mut channels = poisoned.into_inner();
                    channels.terminal_requested = true;
                    self.abort_owned_invocation();
                    return Err(SupervisorInvocationError::Closed);
                }
            };
            if channels.terminal_requested {
                return Err(SupervisorInvocationError::Closed);
            }
            if channels.worker_exit_succeeded {
                return Err(SupervisorInvocationError::Closed);
            }
            if channels.active_worker_channels != 0 {
                return Err(SupervisorInvocationError::WorkerChannelsActive);
            }
            if channels.worker_exit_observing {
                return Err(SupervisorInvocationError::WorkerExitObservationActive);
            }
            channels.worker_exit_observing = true;
        }

        loop {
            match worker.try_wait() {
                Ok(Some(status)) if status.success() => {
                    if let Err(error) = Self::cleanup_worker_cgroup(&mut cgroup) {
                        self.finish_worker_exit_observation(false)?;
                        self.abort_owned_invocation();
                        return Err(error);
                    }
                    self.finish_worker_exit_observation(true)?;
                    return Ok(status);
                }
                Ok(Some(status)) => {
                    let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                    self.finish_worker_exit_observation(false)?;
                    self.abort_owned_invocation();
                    if let Some(error) = cleanup_error {
                        return Err(error);
                    }
                    return Err(SupervisorInvocationError::WorkerExited(status));
                }
                Ok(None) => {
                    if let Err(error) = self.check_deadline() {
                        let _ = worker.kill();
                        let _ = worker.wait();
                        let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                        self.finish_worker_exit_observation(false)?;
                        if let Some(cleanup_error) = cleanup_error {
                            self.abort_owned_invocation();
                            return Err(cleanup_error);
                        }
                        return Err(error);
                    }
                    std::thread::sleep(SUPERVISOR_IDLE_POLL_INTERVAL);
                }
                Err(error) => {
                    let _ = worker.kill();
                    let _ = worker.wait();
                    let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                    self.finish_worker_exit_observation(false)?;
                    self.abort_owned_invocation();
                    if let Some(cleanup_error) = cleanup_error {
                        return Err(cleanup_error);
                    }
                    return Err(SupervisorInvocationError::WorkerProcessIo(error));
                }
            }
        }
    }

    fn cleanup_worker_cgroup(
        cgroup: &mut Option<(crate::linux_sandbox::WorkerCgroup, Duration)>,
    ) -> Result<(), SupervisorInvocationError> {
        if let Some((cgroup, timeout)) = cgroup.take() {
            cgroup
                .cleanup(timeout)
                .map_err(SupervisorInvocationError::WorkerCgroupIo)?;
        }
        Ok(())
    }

    fn finish_worker_exit_observation(
        &self,
        succeeded: bool,
    ) -> Result<(), SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.worker_exit_observing = false;
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        channels.worker_exit_observing = false;
        channels.worker_exit_succeeded = succeeded;
        if !succeeded {
            channels.terminal_requested = true;
        }
        Ok(())
    }

    /// Serve executor, executor-planner, and optimizer-planner Unix streams
    /// concurrently against this invocation. Poll deadlines while streams are
    /// idle, stop every peer after handler failure, and drain all handlers.
    pub fn serve_worker_channels(
        &self,
        planner_stream: std::os::unix::net::UnixStream,
        executor_stream: std::os::unix::net::UnixStream,
        optimizer_planner_stream: std::os::unix::net::UnixStream,
    ) -> Result<(), SupervisorInvocationError> {
        self.serve_worker_channels_with_optimizer_planner_observer(
            planner_stream,
            executor_stream,
            optimizer_planner_stream,
            |_, _| {},
        )
    }

    /// Serve the production worker channels while observing optimizer planner
    /// requests immediately before storage dispatch. The callback runs on the
    /// optimizer-planner handler thread without holding the invocation lock.
    #[doc(hidden)]
    pub fn serve_worker_channels_with_optimizer_planner_observer<F>(
        &self,
        planner_stream: std::os::unix::net::UnixStream,
        executor_stream: std::os::unix::net::UnixStream,
        optimizer_planner_stream: std::os::unix::net::UnixStream,
        observer: F,
    ) -> Result<(), SupervisorInvocationError>
    where
        F: Fn(u16, u64) + Send + Sync,
    {
        self.serve_worker_channels_with_optimizer_planner_timing_observer(
            planner_stream,
            executor_stream,
            optimizer_planner_stream,
            observer,
            |_, _, _| {},
            false,
        )
    }

    /// Variant that also reports the duration of each optimizer planner
    /// request's trusted storage dispatch for confined workload profiling.
    #[doc(hidden)]
    pub fn serve_worker_channels_with_optimizer_planner_timing_observer<F, T>(
        &self,
        mut planner_stream: std::os::unix::net::UnixStream,
        mut executor_stream: std::os::unix::net::UnixStream,
        mut optimizer_planner_stream: std::os::unix::net::UnixStream,
        observer: F,
        timing_observer: T,
        measure_dispatch: bool,
    ) -> Result<(), SupervisorInvocationError>
    where
        F: Fn(u16, u64) + Send + Sync,
        T: Fn(u16, u64, Duration) + Send + Sync,
    {
        use std::net::Shutdown;
        let stop_streams = || -> std::io::Result<(
            std::os::unix::net::UnixStream,
            std::os::unix::net::UnixStream,
            std::os::unix::net::UnixStream,
        )> {
            Ok((
                planner_stream.try_clone()?,
                executor_stream.try_clone()?,
                optimizer_planner_stream.try_clone()?,
            ))
        };
        let (stop_planner, stop_executor, stop_optimizer) = match stop_streams() {
            Ok(streams) => streams,
            Err(error) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let planner_peers = match (stop_executor.try_clone(), stop_optimizer.try_clone()) {
            (Ok(executor), Ok(optimizer)) => (executor, optimizer),
            (Err(error), _) | (_, Err(error)) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let executor_peers = match (stop_planner.try_clone(), stop_optimizer.try_clone()) {
            (Ok(planner), Ok(optimizer)) => (planner, optimizer),
            (Err(error), _) | (_, Err(error)) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let optimizer_peers = match (stop_planner.try_clone(), stop_executor.try_clone()) {
            (Ok(planner), Ok(executor)) => (planner, executor),
            (Err(error), _) | (_, Err(error)) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };

        std::thread::scope(|scope| {
            let (completed_tx, completed_rx) = std::sync::mpsc::channel();
            let planner_completed = completed_tx.clone();
            let planner = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.serve_planner(&mut planner_stream)
                }))
                .unwrap_or(Err(SupervisorInvocationError::WorkerHandlerPanicked));
                if result.is_err() {
                    let _ = planner_peers.0.shutdown(Shutdown::Both);
                    let _ = planner_peers.1.shutdown(Shutdown::Both);
                }
                let _ = planner_completed.send((0u8, result));
            });
            let executor_completed = completed_tx.clone();
            let executor = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.serve_executor(&mut executor_stream)
                }))
                .unwrap_or(Err(SupervisorInvocationError::WorkerHandlerPanicked));
                if result.is_err() {
                    let _ = executor_peers.0.shutdown(Shutdown::Both);
                    let _ = executor_peers.1.shutdown(Shutdown::Both);
                }
                let _ = executor_completed.send((1u8, result));
            });
            let optimizer_completed = completed_tx.clone();
            let optimizer = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _lease = self.enter_worker_channel()?;
                    let mut observe_and_check = |operation, request_id| {
                        observer(operation, request_id);
                        self.check_worker_deadline().map_err(|error| match error {
                            SupervisorInvocationError::Deadline(transaction_error) => {
                                crate::planner_ipc::PlannerConnectionError::Request(
                                    crate::planner_ipc::PlannerRequestError::Transaction(
                                        transaction_error,
                                    ),
                                )
                            }
                            _ => crate::planner_ipc::PlannerConnectionError::SharedInvocationUnavailable,
                        })
                    };
                    let mut observe_dispatch = |operation, request_id, duration| {
                        timing_observer(operation, request_id, duration);
                    };
                    if measure_dispatch {
                        crate::planner_ipc::serve_planner_shared_with_timing_observer(
                            &mut optimizer_planner_stream,
                            &self.transaction,
                            &mut observe_and_check,
                            &mut observe_dispatch,
                            true,
                        )
                    } else {
                        crate::planner_ipc::serve_planner_shared_with_observer(
                            &mut optimizer_planner_stream,
                            &self.transaction,
                            &mut observe_and_check,
                        )
                    }
                    .map_err(SupervisorInvocationError::Planner)
                }))
                .unwrap_or(Err(SupervisorInvocationError::WorkerHandlerPanicked));
                if result.is_err() {
                    let _ = optimizer_peers.0.shutdown(Shutdown::Both);
                    let _ = optimizer_peers.1.shutdown(Shutdown::Both);
                }
                let _ = optimizer_completed.send((2u8, result));
            });
            drop(completed_tx);

            let mut results = [None, None, None];
            let mut completed = 0;
            let mut deadline_error = None;
            while completed < 3 {
                match completed_rx.recv_timeout(SUPERVISOR_IDLE_POLL_INTERVAL) {
                    Ok((index, result)) => {
                        completed += 1;
                        results[index as usize] = Some(result);
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(error) = self.check_worker_deadline() {
                            deadline_error = Some(error);
                            let _ = stop_planner.shutdown(Shutdown::Both);
                            let _ = stop_executor.shutdown(Shutdown::Both);
                            let _ = stop_optimizer.shutdown(Shutdown::Both);
                            break;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            while completed < 3 {
                let Ok((index, result)) = completed_rx.recv() else {
                    break;
                };
                completed += 1;
                results[index as usize] = Some(result);
            }

            let planner_panicked = planner.join().is_err();
            let executor_panicked = executor.join().is_err();
            let optimizer_panicked = optimizer.join().is_err();
            if planner_panicked || executor_panicked || optimizer_panicked {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerHandlerPanicked);
            }
            if let Some(error) = deadline_error {
                return Err(error);
            }
            if results.iter().flatten().any(|result| {
                matches!(
                    result,
                    Err(SupervisorInvocationError::WorkerHandlerPanicked)
                )
            }) {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerHandlerPanicked);
            }
            for result in results {
                result.unwrap_or(Err(SupervisorInvocationError::Closed))?;
            }
            Ok(())
        })
    }

    pub fn serve_planner<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let _lease = self.enter_worker_channel()?;
        crate::planner_ipc::serve_planner_shared(stream, &self.transaction)
            .map_err(SupervisorInvocationError::Planner)
    }

    pub fn serve_executor<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let _lease = self.enter_worker_channel()?;
        crate::executor_ipc::serve_executor_shared(stream, &self.transaction)
            .map_err(SupervisorInvocationError::Executor)
    }

    pub fn serve_terminal<S: std::io::Read + std::io::Write>(
        &mut self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        if channels.terminal_requested {
            return Err(SupervisorInvocationError::Closed);
        }
        if channels.active_worker_channels != 0 {
            return Err(SupervisorInvocationError::WorkerChannelsActive);
        }
        if channels.worker_exit_observing {
            return Err(SupervisorInvocationError::WorkerExitObservationActive);
        }
        if !channels.worker_exit_succeeded {
            return Err(SupervisorInvocationError::WorkerExitNotObserved);
        }
        channels.terminal_requested = true;
        drop(channels);
        let mut transaction = match self.transaction.lock() {
            Ok(transaction) => transaction,
            Err(poisoned) => {
                let mut transaction = poisoned.into_inner();
                if let Some(mut invocation) = transaction.take() {
                    let _ = invocation.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        crate::supervisor_ipc::serve_supervisor(stream, &mut transaction)
            .map_err(SupervisorInvocationError::Supervisor)
    }

    pub fn serve_control<S: std::io::Read + std::io::Write>(
        &self,
        manager: &StorageManager<'_>,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        crate::supervisor_ipc::serve_supervisor_control(stream, manager, self.principal_id)
            .map_err(SupervisorInvocationError::Supervisor)
    }
}

impl Drop for SupervisorInvocation<'_> {
    fn drop(&mut self) {
        if !self.abort_on_drop {
            return;
        }
        let mut owner = self
            .transaction
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut transaction) = owner.take() {
            let _ = transaction.abort();
        }
    }
}

struct TokenRequestLimiter {
    limit: AtomicUsize,
    requests: Mutex<HashMap<[u8; 16], VecDeque<Instant>>>,
}

impl TokenRequestLimiter {
    fn new(limit: usize) -> Self {
        Self {
            limit: AtomicUsize::new(limit),
            requests: Mutex::new(HashMap::new()),
        }
    }

    fn consume(&self, actor_id: [u8; 16]) -> Result<(), TokenLifecycleError> {
        let now = Instant::now();
        let mut requests = self
            .requests
            .lock()
            .map_err(|_| TokenLifecycleError::RateLimited)?;
        requests.retain(|_, history| {
            while history.front().is_some_and(|oldest| {
                now.saturating_duration_since(*oldest) >= TOKEN_REQUEST_WINDOW
            }) {
                history.pop_front();
            }
            !history.is_empty()
        });
        if !requests.contains_key(&actor_id) {
            if requests.len() >= MAX_TRACKED_TOKEN_REQUEST_ACTORS {
                return Err(TokenLifecycleError::RateLimited);
            }
            requests.insert(actor_id, VecDeque::new());
        }
        let history = requests
            .get_mut(&actor_id)
            .ok_or(TokenLifecycleError::RateLimited)?;
        if history.len() >= self.limit.load(Ordering::Relaxed) {
            return Err(TokenLifecycleError::RateLimited);
        }
        history.push_back(now);
        Ok(())
    }
}

fn shared_token_request_limiter(
    database: &Database,
    configured_limit: usize,
) -> Arc<TokenRequestLimiter> {
    let database_key = database as *const Database as usize;
    let registry = TOKEN_REQUEST_LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, limiter| limiter.strong_count() != 0);
    if let Some(limiter) = registry.get(&database_key).and_then(Weak::upgrade) {
        limiter.limit.fetch_min(configured_limit, Ordering::Relaxed);
        return limiter;
    }
    let limiter = Arc::new(TokenRequestLimiter::new(configured_limit));
    registry.insert(database_key, Arc::downgrade(&limiter));
    limiter
}

fn shared_admin_cancellation_limiter(
    database: &Database,
    configured_limit: usize,
) -> Arc<TokenRequestLimiter> {
    let database_key = database as *const Database as usize;
    let registry = ADMIN_CANCELLATION_LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, limiter| limiter.strong_count() != 0);
    if let Some(limiter) = registry.get(&database_key).and_then(Weak::upgrade) {
        limiter.limit.fetch_min(configured_limit, Ordering::Relaxed);
        return limiter;
    }
    let limiter = Arc::new(TokenRequestLimiter::new(configured_limit));
    registry.insert(database_key, Arc::downgrade(&limiter));
    limiter
}

fn shared_auth_failure_audit_limiter(database: &Database) -> Arc<TokenRequestLimiter> {
    let database_key = database as *const Database as usize;
    let registry = AUTH_FAILURE_AUDIT_LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, limiter| limiter.strong_count() != 0);
    if let Some(limiter) = registry.get(&database_key).and_then(Weak::upgrade) {
        return limiter;
    }
    let limiter = Arc::new(TokenRequestLimiter::new(
        MAX_AUTH_FAILURE_AUDITS_PER_TOKEN_PER_MINUTE,
    ));
    registry.insert(database_key, Arc::downgrade(&limiter));
    limiter
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveInvocationPhase {
    Active,
    CancellationPending,
    Committing,
}

#[derive(Clone, Copy)]
struct LiveInvocation {
    transaction_id: [u8; 16],
    principal_id: [u8; 16],
    workload_class: crate::supervisor::WorkloadClass,
    started_at_us: i64,
    deadline_at_us: Option<i64>,
    phase: LiveInvocationPhase,
    // Keep ambiguous commit outcomes visible as COMMITTING until a reopened
    // Database handle resolves them from redb's recovered durable state.
    commit_uncertain: bool,
    staged_rows: usize,
    staged_value_bytes: usize,
    snapshot_pin_count: u8,
    snapshot_pinned_at: Option<Instant>,
    cancellation_reason_code: Option<u16>,
    owner_generation: u128,
}

struct InvocationAdmissionState {
    database_key: usize,
    limit: AtomicUsize,
    analytical_limit: AtomicUsize,
    statistics_collection_limit: AtomicUsize,
    statistics_collection_active: AtomicBool,
    database_uncertain: AtomicBool,
    live: Mutex<LiveInvocationRegistry>,
    changed: Condvar,
}

struct StatisticsMaintenancePermit<'a>(&'a AtomicBool);

impl Drop for StatisticsMaintenancePermit<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct LiveInvocationRegistry {
    generation: u128,
    invocations_admitted_total: u64,
    invocation_conflicts_total: u64,
    admin_cancellations_accepted_total: u64,
    admin_cancellations_denied_total: u64,
    invocation_policy_denials_total: u64,
    invocation_staged_value_bytes_total: u64,
    invocation_commit_latency_buckets: [u64; 6],
    invocation_redb_write_acquisition_buckets: [u64; 6],
    outcome_maintenance_passes_total: u64,
    outcome_maintenance_failures_total: u64,
    outcomes_pruned_total: u64,
    entries: HashMap<[u8; 16], LiveInvocation>,
}

fn bounded_invocation_ids(
    ids: impl IntoIterator<Item = [u8; 16]>,
    after: Option<[u8; 16]>,
    limit: usize,
) -> Vec<[u8; 16]> {
    if limit == 0 {
        return Vec::new();
    }
    let capacity = limit.saturating_add(1);
    let mut selected = std::collections::BinaryHeap::with_capacity(capacity);
    for id in ids {
        if after.is_some_and(|after| id <= after) {
            continue;
        }
        if selected.len() < capacity {
            selected.push(id);
        } else if selected.peek().is_some_and(|largest| id < *largest) {
            selected.pop();
            selected.push(id);
        }
    }
    selected.into_sorted_vec()
}

struct AdministrativeInvocationProjection {
    invocations: Vec<AdministrativeInvocationStatus>,
    continuation: Option<AdministrativeInvocationCursor>,
    pinned_snapshot_count: usize,
    oldest_snapshot_pin_age_us: u64,
    invocations_admitted_total: u64,
    invocation_conflicts_total: u64,
    admin_cancellations_accepted_total: u64,
    admin_cancellations_denied_total: u64,
    invocation_policy_denials_total: u64,
    invocation_staged_value_bytes_total: u64,
    invocation_commit_latency_buckets: [u64; 6],
    invocation_redb_write_acquisition_buckets: [u64; 6],
    outcome_maintenance_passes_total: u64,
    outcome_maintenance_failures_total: u64,
    outcomes_pruned_total: u64,
}

impl LiveInvocationRegistry {
    fn record_admin_cancellation(&mut self) {
        self.admin_cancellations_accepted_total =
            self.admin_cancellations_accepted_total.saturating_add(1);
    }

    fn record_admin_cancellation_denial(&mut self) {
        self.admin_cancellations_denied_total =
            self.admin_cancellations_denied_total.saturating_add(1);
    }

    fn record_invocation_policy_denial(&mut self) {
        self.invocation_policy_denials_total =
            self.invocation_policy_denials_total.saturating_add(1);
    }

    fn record_invocation_staged_value_bytes(&mut self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.invocation_staged_value_bytes_total = self
            .invocation_staged_value_bytes_total
            .saturating_add(bytes);
    }

    fn record_invocation_commit_latency(&mut self, elapsed: Duration) {
        let bucket = if elapsed <= Duration::from_micros(1_000) {
            0
        } else if elapsed <= Duration::from_micros(5_000) {
            1
        } else if elapsed <= Duration::from_micros(10_000) {
            2
        } else if elapsed <= Duration::from_micros(50_000) {
            3
        } else if elapsed <= Duration::from_micros(100_000) {
            4
        } else {
            5
        };
        self.invocation_commit_latency_buckets[bucket] =
            self.invocation_commit_latency_buckets[bucket].saturating_add(1);
    }

    fn record_invocation_redb_write_acquisition(&mut self, elapsed: Duration) {
        let bucket = if elapsed <= Duration::from_micros(1_000) {
            0
        } else if elapsed <= Duration::from_micros(5_000) {
            1
        } else if elapsed <= Duration::from_micros(10_000) {
            2
        } else if elapsed <= Duration::from_micros(50_000) {
            3
        } else if elapsed <= Duration::from_micros(100_000) {
            4
        } else {
            5
        };
        self.invocation_redb_write_acquisition_buckets[bucket] =
            self.invocation_redb_write_acquisition_buckets[bucket].saturating_add(1);
    }

    fn record_outcome_maintenance(
        &mut self,
        result: &Result<
            crate::outcome::OutcomePruneResult,
            crate::outcome::OutcomeMaintenanceError,
        >,
    ) {
        self.outcome_maintenance_passes_total =
            self.outcome_maintenance_passes_total.saturating_add(1);
        match result {
            Ok(result) => {
                self.outcomes_pruned_total = self
                    .outcomes_pruned_total
                    .saturating_add(u64::try_from(result.removed).unwrap_or(u64::MAX));
            }
            Err(_) => {
                self.outcome_maintenance_failures_total =
                    self.outcome_maintenance_failures_total.saturating_add(1);
            }
        }
    }

    fn advance_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1).max(1);
    }

    fn advance_owner_generation(&mut self, principal_id: [u8; 16]) {
        self.advance_generation();
        let generation = self.generation;
        for invocation in self.entries.values_mut() {
            if invocation.principal_id == principal_id {
                invocation.owner_generation = generation;
            }
        }
    }
}

pub(crate) struct InvocationAdmissionPermit {
    state: Arc<InvocationAdmissionState>,
    invocation_id: [u8; 16],
}

#[derive(Clone)]
pub(crate) struct InvocationAdmissionMetrics {
    state: Weak<InvocationAdmissionState>,
}

impl InvocationAdmissionMetrics {
    pub(crate) fn record_policy_denial(&self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut live = state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.record_invocation_policy_denial();
    }

    pub(crate) fn record_commit_latency(&self, elapsed: Duration) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut live = state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.record_invocation_commit_latency(elapsed);
    }

    pub(crate) fn record_write_acquisition(&self, elapsed: Duration) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut live = state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.record_invocation_redb_write_acquisition(elapsed);
    }
}

impl InvocationAdmissionPermit {
    pub(crate) fn metrics(&self) -> InvocationAdmissionMetrics {
        InvocationAdmissionMetrics {
            state: Arc::downgrade(&self.state),
        }
    }

    pub(crate) fn record_staged_value_bytes(&self, bytes: usize) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.record_invocation_staged_value_bytes(bytes);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionError {
    Limit,
    Duplicate,
    DatabaseUncertain,
    RandomnessUnavailable,
}

impl InvocationAdmissionState {
    fn acquire(
        self: &Arc<Self>,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        workload_class: crate::supervisor::WorkloadClass,
        started_at_us: i64,
        max_age: Duration,
    ) -> Result<InvocationAdmissionPermit, AdmissionError> {
        let mut live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.database_uncertain.load(Ordering::Acquire) {
            return Err(AdmissionError::DatabaseUncertain);
        }
        if live.entries.contains_key(&invocation_id) {
            return Err(AdmissionError::Duplicate);
        }
        if workload_class == crate::supervisor::WorkloadClass::Analytical
            && live
                .entries
                .values()
                .filter(|invocation| {
                    invocation.workload_class == crate::supervisor::WorkloadClass::Analytical
                })
                .count()
                >= self.analytical_limit.load(Ordering::Acquire)
        {
            return Err(AdmissionError::Limit);
        }
        if live.entries.len() >= self.limit.load(Ordering::Acquire) {
            return Err(AdmissionError::Limit);
        }
        let transaction_id = loop {
            let mut candidate = [0; 16];
            if getrandom::fill(&mut candidate).is_err() {
                return Err(AdmissionError::RandomnessUnavailable);
            }
            if candidate != [0; 16]
                && !live
                    .entries
                    .values()
                    .any(|entry| entry.transaction_id == candidate)
            {
                break candidate;
            }
        };
        let deadline_at_us = i64::try_from(max_age.as_micros())
            .ok()
            .and_then(|delta| started_at_us.checked_add(delta));
        live.entries.insert(
            invocation_id,
            LiveInvocation {
                transaction_id,
                principal_id,
                workload_class,
                started_at_us,
                deadline_at_us,
                phase: LiveInvocationPhase::Active,
                commit_uncertain: false,
                staged_rows: 0,
                staged_value_bytes: 0,
                snapshot_pin_count: 0,
                snapshot_pinned_at: None,
                cancellation_reason_code: None,
                owner_generation: 0,
            },
        );
        live.invocations_admitted_total = live.invocations_admitted_total.saturating_add(1);
        live.advance_owner_generation(principal_id);
        Ok(InvocationAdmissionPermit {
            state: Arc::clone(self),
            invocation_id,
        })
    }

    fn status(&self, invocation_id: [u8; 16]) -> Option<LiveInvocation> {
        self.live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&invocation_id)
            .copied()
    }

    fn own_page(
        &self,
        principal_id: [u8; 16],
        limit: usize,
        cursor: Option<&InvocationListCursor>,
    ) -> Result<(Vec<OwnInvocationStatus>, Option<InvocationListCursor>), InvocationStatusError>
    {
        let live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner_generation = live
            .entries
            .values()
            .filter(|invocation| invocation.principal_id == principal_id)
            .map(|invocation| invocation.owner_generation)
            .max();
        if let Some(cursor) = cursor {
            if cursor.principal_id != principal_id {
                return Err(InvocationStatusError::InvalidCursor);
            }
            if owner_generation != Some(cursor.owner_generation) {
                return Err(InvocationStatusError::StaleCursor);
            }
        }
        let mut ids = bounded_invocation_ids(
            live.entries
                .iter()
                .filter(|(_, invocation)| invocation.principal_id == principal_id)
                .map(|(invocation_id, _)| *invocation_id),
            cursor.map(|cursor| cursor.after_invocation_id),
            limit,
        );
        let has_more = ids.len() > limit;
        ids.truncate(limit);
        let entries = ids
            .into_iter()
            .filter_map(|invocation_id| {
                let invocation = live.entries.get(&invocation_id)?;
                Some(OwnInvocationStatus {
                    invocation_id,
                    staged_rows: invocation.staged_rows,
                    staged_value_bytes: invocation.staged_value_bytes,
                    status: match invocation.phase {
                        LiveInvocationPhase::Active => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                        LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: false,
                        },
                        LiveInvocationPhase::Committing => InvocationStatus::Committing {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                    },
                })
            })
            .collect::<Vec<_>>();
        let continuation = if has_more {
            let owner_generation = owner_generation.ok_or(InvocationStatusError::InvalidRequest)?;
            entries.last().map(|entry| InvocationListCursor {
                principal_id,
                owner_generation,
                after_invocation_id: entry.invocation_id,
            })
        } else {
            None
        };
        Ok((entries, continuation))
    }

    fn administrative_page(
        &self,
        administrator_id: [u8; 16],
        now_us: i64,
        observed_at: Instant,
        limit: usize,
        cursor: Option<&AdministrativeInvocationCursor>,
    ) -> Result<AdministrativeInvocationProjection, AdministrativeInvocationListError> {
        let live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(cursor) = cursor {
            if cursor.administrator_id != administrator_id {
                return Err(AdministrativeInvocationListError::InvalidCursor);
            }
            if cursor.registry_generation != live.generation {
                return Err(AdministrativeInvocationListError::StaleCursor);
            }
        }
        let pinned_snapshot_count = live
            .entries
            .values()
            .filter(|invocation| invocation.snapshot_pin_count != 0)
            .count();
        let oldest_snapshot_pin_age_us = live
            .entries
            .values()
            .filter_map(|invocation| invocation.snapshot_pinned_at)
            .map(|pinned_at| {
                u64::try_from(observed_at.saturating_duration_since(pinned_at).as_micros())
                    .unwrap_or(u64::MAX)
            })
            .max()
            .unwrap_or(0);
        let mut ids = bounded_invocation_ids(
            live.entries.keys().copied(),
            cursor.map(|cursor| cursor.after_invocation_id),
            limit,
        );
        let has_more = ids.len() > limit;
        ids.truncate(limit);
        let entries = ids
            .into_iter()
            .filter_map(|invocation_id| {
                let invocation = live.entries.get(&invocation_id)?;
                Some(AdministrativeInvocationStatus {
                    invocation_id,
                    transaction_id: invocation.transaction_id,
                    principal_id: invocation.principal_id,
                    workload_class: invocation.workload_class,
                    status: match invocation.phase {
                        LiveInvocationPhase::Active => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                        LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: false,
                        },
                        LiveInvocationPhase::Committing => InvocationStatus::Committing {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                    },
                    age_us: u64::try_from(now_us.saturating_sub(invocation.started_at_us))
                        .unwrap_or(0),
                    staged_rows: invocation.staged_rows,
                    staged_value_bytes: invocation.staged_value_bytes,
                    snapshot_pin_count: invocation.snapshot_pin_count,
                    cancellation_reason_code: invocation.cancellation_reason_code,
                })
            })
            .collect::<Vec<_>>();
        let continuation = if has_more {
            entries.last().map(|entry| AdministrativeInvocationCursor {
                administrator_id,
                registry_generation: live.generation,
                after_invocation_id: entry.invocation_id,
            })
        } else {
            None
        };
        Ok(AdministrativeInvocationProjection {
            invocations: entries,
            continuation,
            pinned_snapshot_count,
            oldest_snapshot_pin_age_us,
            invocations_admitted_total: live.invocations_admitted_total,
            invocation_conflicts_total: live.invocation_conflicts_total,
            admin_cancellations_accepted_total: live.admin_cancellations_accepted_total,
            admin_cancellations_denied_total: live.admin_cancellations_denied_total,
            invocation_policy_denials_total: live.invocation_policy_denials_total,
            invocation_staged_value_bytes_total: live.invocation_staged_value_bytes_total,
            invocation_commit_latency_buckets: live.invocation_commit_latency_buckets,
            invocation_redb_write_acquisition_buckets: live
                .invocation_redb_write_acquisition_buckets,
            outcome_maintenance_passes_total: live.outcome_maintenance_passes_total,
            outcome_maintenance_failures_total: live.outcome_maintenance_failures_total,
            outcomes_pruned_total: live.outcomes_pruned_total,
        })
    }
}

impl InvocationAdmissionPermit {
    pub(crate) fn update_snapshot_pin_count(&self, snapshot_pin_count: u8) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(invocation) = live.entries.get_mut(&self.invocation_id)
            && invocation.snapshot_pin_count != snapshot_pin_count
        {
            invocation.snapshot_pin_count = snapshot_pin_count;
            invocation.snapshot_pinned_at = if snapshot_pin_count == 0 {
                None
            } else {
                Some(Instant::now())
            };
            live.advance_generation();
        }
    }

    pub(crate) fn record_conflict(&self) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.invocation_conflicts_total = live.invocation_conflicts_total.saturating_add(1);
    }

    pub(crate) fn update_write_set_usage(&self, staged_rows: usize, staged_value_bytes: usize) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let principal_id = live
            .entries
            .get(&self.invocation_id)
            .and_then(|invocation| {
                (invocation.staged_rows != staged_rows
                    || invocation.staged_value_bytes != staged_value_bytes)
                    .then_some(invocation.principal_id)
            });
        if let Some(principal_id) = principal_id {
            let invocation = live
                .entries
                .get_mut(&self.invocation_id)
                .expect("invocation was observed while the registry lock was held");
            invocation.staged_rows = staged_rows;
            invocation.staged_value_bytes = staged_value_bytes;
            live.advance_owner_generation(principal_id);
        }
    }

    pub(crate) fn mark_commit_uncertain(&self) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.state.database_uncertain.store(true, Ordering::Release);
        if let Some(invocation) = live.entries.get_mut(&self.invocation_id) {
            invocation.phase = LiveInvocationPhase::Committing;
            invocation.commit_uncertain = true;
            let principal_id = invocation.principal_id;
            live.advance_owner_generation(principal_id);
        }
        self.state.changed.notify_all();
        retain_uncertain_registry(self.state.database_key, Arc::clone(&self.state));
    }

    pub(crate) fn may_continue(&self) -> bool {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            let phase = live
                .entries
                .get(&self.invocation_id)
                .map(|invocation| (invocation.phase, invocation.cancellation_reason_code));
            match phase {
                Some((LiveInvocationPhase::Active, None)) => return true,
                Some((LiveInvocationPhase::CancellationPending, _)) => {
                    live = self
                        .state
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                _ => return false,
            }
        }
    }

    pub(crate) fn cancellation_requested(&self) -> bool {
        let live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.entries
            .get(&self.invocation_id)
            .is_some_and(|invocation| invocation.cancellation_reason_code.is_some())
    }

    pub(crate) fn try_mark_committing(&self) -> bool {
        self.try_mark_committing_then(|| {})
    }

    fn try_mark_committing_then(&self, after_transition: impl FnOnce()) -> bool {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut after_transition = Some(after_transition);
        loop {
            let phase = live
                .entries
                .get(&self.invocation_id)
                .map(|invocation| (invocation.phase, invocation.cancellation_reason_code));
            match phase {
                Some((LiveInvocationPhase::Active, None)) => {
                    let Some(invocation) = live.entries.get_mut(&self.invocation_id) else {
                        return false;
                    };
                    invocation.phase = LiveInvocationPhase::Committing;
                    let principal_id = invocation.principal_id;
                    live.advance_owner_generation(principal_id);
                    self.state.changed.notify_all();
                    drop(live);
                    after_transition
                        .take()
                        .expect("commit transition hook is consumed once")();
                    return true;
                }
                Some((LiveInvocationPhase::CancellationPending, _)) => {
                    live = self
                        .state
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                _ => return false,
            }
        }
    }
}

impl Drop for InvocationAdmissionPermit {
    fn drop(&mut self) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let should_remove = live
            .entries
            .get(&self.invocation_id)
            .is_some_and(|invocation| !invocation.commit_uncertain);
        if should_remove && let Some(invocation) = live.entries.remove(&self.invocation_id) {
            live.advance_owner_generation(invocation.principal_id);
            self.state.changed.notify_all();
        }
    }
}

fn invocation_registry(
    database: &Database,
    configured_limit: usize,
    configured_analytical_limit: usize,
    configured_statistics_collection_limit: usize,
) -> Arc<InvocationAdmissionState> {
    let database_key = database as *const Database as usize;
    let uncertain = UNCERTAIN_INVOCATION_REGISTRIES.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(invocations) = uncertain
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&database_key)
        .cloned()
    {
        invocations
            .limit
            .fetch_min(configured_limit, Ordering::Relaxed);
        invocations
            .analytical_limit
            .fetch_min(configured_analytical_limit, Ordering::Relaxed);
        invocations
            .statistics_collection_limit
            .fetch_min(configured_statistics_collection_limit, Ordering::Relaxed);
        return invocations;
    }
    let registry = INVOCATION_REGISTRIES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, invocations| invocations.strong_count() != 0);
    if let Some(invocations) = registry.get(&database_key).and_then(Weak::upgrade) {
        invocations
            .limit
            .fetch_min(configured_limit, Ordering::Relaxed);
        invocations
            .analytical_limit
            .fetch_min(configured_analytical_limit, Ordering::Relaxed);
        invocations
            .statistics_collection_limit
            .fetch_min(configured_statistics_collection_limit, Ordering::Relaxed);
        return invocations;
    }
    let invocations = Arc::new(InvocationAdmissionState {
        database_key,
        limit: AtomicUsize::new(configured_limit),
        analytical_limit: AtomicUsize::new(configured_analytical_limit),
        statistics_collection_limit: AtomicUsize::new(configured_statistics_collection_limit),
        statistics_collection_active: AtomicBool::new(false),
        database_uncertain: AtomicBool::new(false),
        live: Mutex::new(LiveInvocationRegistry {
            generation: 1,
            invocations_admitted_total: 0,
            invocation_conflicts_total: 0,
            admin_cancellations_accepted_total: 0,
            admin_cancellations_denied_total: 0,
            invocation_policy_denials_total: 0,
            invocation_staged_value_bytes_total: 0,
            invocation_commit_latency_buckets: [0; 6],
            invocation_redb_write_acquisition_buckets: [0; 6],
            outcome_maintenance_passes_total: 0,
            outcome_maintenance_failures_total: 0,
            outcomes_pruned_total: 0,
            entries: HashMap::new(),
        }),
        changed: Condvar::new(),
    });
    registry.insert(database_key, Arc::downgrade(&invocations));
    invocations
}

fn shared_invocation_registry(
    database: &Database,
    configured_limit: usize,
    configured_analytical_limit: usize,
    configured_statistics_collection_limit: usize,
) -> Arc<InvocationAdmissionState> {
    invocation_registry(
        database,
        configured_limit,
        configured_analytical_limit,
        configured_statistics_collection_limit,
    )
}

fn retain_uncertain_registry(database_key: usize, state: Arc<InvocationAdmissionState>) {
    UNCERTAIN_INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(database_key, state);
}

fn has_live_invocation_registry(database: &Database) -> bool {
    let database_key = database as *const Database as usize;
    if UNCERTAIN_INVOCATION_REGISTRIES
        .get()
        .and_then(|registries| registries.lock().ok())
        .is_some_and(|registries| registries.contains_key(&database_key))
    {
        return true;
    }
    let Some(registries) = INVOCATION_REGISTRIES.get() else {
        return false;
    };
    let Ok(mut registries) = registries.lock() else {
        return true;
    };
    registries.retain(|_, registry| registry.strong_count() != 0);
    registries
        .get(&database_key)
        .is_some_and(|registry| registry.strong_count() != 0)
}

fn interactive_recovery_key(transaction_id: [u8; 16]) -> Vec<u8> {
    let mut key = Vec::with_capacity(INTERACTIVE_TXN_RECOVERY_PREFIX.len() + transaction_id.len());
    key.extend_from_slice(INTERACTIVE_TXN_RECOVERY_PREFIX);
    key.extend_from_slice(&transaction_id);
    key
}

fn persist_interactive_transaction_marker(
    database: &Database,
    transaction_id: [u8; 16],
    controller_id: [u8; 16],
    retention_deadline_us: i64,
) -> Result<(), String> {
    if transaction_id == [0; 16] || controller_id == [0; 16] || retention_deadline_us <= 0 {
        return Err("invalid interactive transaction recovery marker".to_owned());
    }
    let key = interactive_recovery_key(transaction_id);
    let mut value = [0; 28];
    value[..4].copy_from_slice(INTERACTIVE_TXN_RECOVERY_MAGIC);
    value[4..20].copy_from_slice(&controller_id);
    value[20..].copy_from_slice(&retention_deadline_us.to_le_bytes());
    let mut write = database.begin_write().map_err(|error| error.to_string())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    write.set_two_phase_commit(true);
    {
        let mut metadata = write
            .open_table(ENGINE_META)
            .map_err(|error| error.to_string())?;
        if metadata
            .get(key.as_slice())
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("duplicate interactive transaction recovery marker".to_owned());
        }
        metadata
            .insert(key.as_slice(), value.as_slice())
            .map_err(|error| error.to_string())?;
    }
    write.commit().map_err(|error| error.to_string())
}

fn remove_interactive_transaction_marker(
    database: &Database,
    transaction_id: [u8; 16],
) -> Result<(), String> {
    let key = interactive_recovery_key(transaction_id);
    let mut write = database.begin_write().map_err(|error| error.to_string())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    write.set_two_phase_commit(true);
    write
        .open_table(ENGINE_META)
        .map_err(|error| error.to_string())?
        .remove(key.as_slice())
        .map_err(|error| error.to_string())?;
    write.commit().map_err(|error| error.to_string())
}

fn recover_interactive_transaction_markers(database: &Database) -> Result<(), String> {
    let markers = {
        let read = database.begin_read().map_err(|error| error.to_string())?;
        if !read
            .list_tables()
            .map_err(|error| error.to_string())?
            .any(|table| table.name() == ENGINE_META.name())
        {
            return Ok(());
        }
        let metadata = read
            .open_table(ENGINE_META)
            .map_err(|error| error.to_string())?;
        let mut markers = Vec::new();
        for entry in metadata.iter().map_err(|error| error.to_string())? {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            let key = key.value();
            if !key.starts_with(INTERACTIVE_TXN_RECOVERY_PREFIX) {
                continue;
            }
            if key.len() != INTERACTIVE_TXN_RECOVERY_PREFIX.len() + 16
                || value.value().len() != 28
                || value.value()[..4] != INTERACTIVE_TXN_RECOVERY_MAGIC[..]
            {
                return Err("invalid interactive transaction recovery marker".to_owned());
            }
            let transaction_id = key[INTERACTIVE_TXN_RECOVERY_PREFIX.len()..]
                .try_into()
                .map_err(|_| "invalid interactive transaction ID".to_owned())?;
            let controller_id = value.value()[4..20]
                .try_into()
                .map_err(|_| "invalid interactive transaction controller".to_owned())?;
            let retention_deadline_us = i64::from_le_bytes(
                value.value()[20..28]
                    .try_into()
                    .map_err(|_| "invalid interactive transaction deadline".to_owned())?,
            );
            if transaction_id == [0; 16] || controller_id == [0; 16] || retention_deadline_us <= 0 {
                return Err("invalid interactive transaction recovery marker".to_owned());
            }
            markers.push((transaction_id, controller_id, retention_deadline_us));
        }
        markers
    };
    if markers.is_empty() {
        return Ok(());
    }
    let now_us = now_unix_micros().ok_or_else(|| "system clock is unavailable".to_owned())?;
    for (transaction_id, controller_id, retention_deadline_us) in markers {
        if retention_deadline_us > now_us
            && lookup_outcome(database, transaction_id, controller_id, now_us)
                .map_err(|error| error.to_string())?
                .is_none()
        {
            record_aborted_outcome(
                database,
                transaction_id,
                controller_id,
                retention_deadline_us,
            )
            .map_err(|error| format!("could not persist recovered ABORTED outcome: {error:?}"))?;
        }
        remove_interactive_transaction_marker(database, transaction_id)?;
    }
    Ok(())
}

/// Quarantine this database handle after redb reports a commit error whose
/// publication state requires close/reopen recovery.
pub(crate) fn mark_database_uncertain(database: &Database) {
    let invocations = invocation_registry(
        database,
        StorageManagerConfig::default().max_active_invocations,
        StorageManagerConfig::default().max_active_analytical_invocations,
        StorageManagerConfig::default().max_statistics_collection_rows,
    );
    let first_uncertain = !invocations.database_uncertain.swap(true, Ordering::AcqRel);
    if first_uncertain {
        let mut live = invocations
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.generation = live.generation.wrapping_add(1).max(1);
    }
    retain_uncertain_registry(invocations.database_key, invocations);
}

/// Forget the closed handle's quarantined process-local state after a fresh
/// redb handle has been opened and validated.
pub(crate) fn clear_uncertain_database_key(database_key: usize) {
    UNCERTAIN_INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&database_key);
    INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&database_key);
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

    /// Select the next table catalog key for the background statistics cursor.
    /// This reads only one table definition per tick and wraps at the end, so
    /// a large catalog does not require materializing its full table list.
    pub(crate) fn next_committed_population_table_id(
        &self,
        after: Option<[u8; 16]>,
    ) -> Result<Option<[u8; 16]>, StatisticsMaintenanceError> {
        if self.admissions.database_uncertain.load(Ordering::Acquire) {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let family_prefix = 5u16.to_be_bytes();
        let after_key = after.map(|table_id| id_catalog_key(5, table_id));

        for pass in 0..if after_key.is_some() { 2 } else { 1 } {
            let start = if pass == 0 {
                after_key.as_deref().unwrap_or(family_prefix.as_slice())
            } else {
                family_prefix.as_slice()
            };
            for entry in catalog
                .range(start..)
                .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
            {
                let (key, value) = entry
                    .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
                let key = key.value().to_vec();
                if !key.starts_with(&family_prefix) {
                    break;
                }
                if pass == 0 && after_key.as_ref().is_some_and(|cursor| key <= *cursor) {
                    continue;
                }
                if key.len() != 19 || key[2] != 1 {
                    return Err(StatisticsMaintenanceError::Storage(
                        "invalid table catalog key".into(),
                    ));
                }
                let bytes = value.value().to_vec();
                let record = decode_registered_envelope(&bytes)
                    .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
                if record.family_code != 5 || record.logical_key != key {
                    return Err(StatisticsMaintenanceError::Storage(
                        "invalid table catalog record".into(),
                    ));
                }
                return Ok(Some(key[3..19].try_into().map_err(|_| {
                    StatisticsMaintenanceError::Storage("invalid table catalog ID".into())
                })?));
            }
        }
        Ok(None)
    }

    /// Read a committed-population record only when it matches engine metadata
    /// from the same fresh redb snapshot. The count stays within trusted
    /// storage and is never included in the maintenance IPC result.
    pub(crate) fn current_committed_population_record(
        &self,
        table_id: [u8; 16],
    ) -> Result<Option<CommittedPopulationRecord>, StatisticsMaintenanceError> {
        if table_id == [0; 16] || self.admissions.database_uncertain.load(Ordering::Acquire) {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let snapshot = RowSnapshot::new(read);
        let metadata_bytes = snapshot
            .get_engine_metadata_value()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
            .ok_or_else(|| StatisticsMaintenanceError::Storage("missing engine metadata".into()))?;
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        if metadata.lifecycle != Lifecycle::Ready {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }

        let mut key = Vec::with_capacity(COMMITTED_POPULATION_KEY_PREFIX.len() + table_id.len());
        key.extend_from_slice(COMMITTED_POPULATION_KEY_PREFIX);
        key.extend_from_slice(&table_id);
        let stats = snapshot
            .read_transaction()
            .open_table(STATS)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let Some(value) = stats
            .get(key.as_slice())
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let record = CommittedPopulationRecord::decode_record(table_id, value.value())
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        Ok((record.snapshot_sequence() == metadata.commit_sequence).then_some(record))
    }

    /// Collect a bounded committed row population from one redb snapshot.
    /// Exact global counts are stored only in the protected stats table; this
    /// public maintenance outcome omits the count. A trusted internal consumer
    /// must validate the stored source commit sequence before reuse. The method
    /// is not exposed over worker or planner IPC.
    pub fn collect_committed_table_population(
        &self,
        table_id: [u8; 16],
    ) -> Result<StatisticsCollectionOutcome, StatisticsMaintenanceError> {
        if table_id == [0; 16] || self.admissions.database_uncertain.load(Ordering::Acquire) {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }
        self.admissions
            .statistics_collection_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| StatisticsMaintenanceError::Busy)?;
        let _permit = StatisticsMaintenancePermit(&self.admissions.statistics_collection_active);

        let maximum = self
            .admissions
            .statistics_collection_limit
            .load(Ordering::Acquire)
            .min(MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS as usize) as u64;
        if maximum == 0 {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let snapshot = RowSnapshot::new(read);
        let metadata_bytes = snapshot
            .get_engine_metadata_value()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
            .ok_or_else(|| StatisticsMaintenanceError::Storage("missing engine metadata".into()))?;
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        if metadata.lifecycle != Lifecycle::Ready {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }
        let mut catalog_key = Vec::with_capacity(19);
        catalog_key.extend_from_slice(&5u16.to_be_bytes());
        catalog_key.push(1);
        catalog_key.extend_from_slice(&table_id);
        if snapshot
            .get_catalog_record(&catalog_key)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
            .is_none()
        {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }

        let mut row_count = 0u64;
        let mut after_row_id = None;
        let result = loop {
            let remaining = maximum.saturating_add(1).saturating_sub(row_count);
            if remaining == 0 {
                break CommittedPopulationRecord::Unknown {
                    table_id,
                    snapshot_sequence: metadata.commit_sequence,
                };
            }
            let page_limit = remaining.min(crate::snapshot::MAX_ROW_SCAN_PAGE as u64) as usize;
            let (row_ids, complete) = snapshot
                .scan_row_ids_after(table_id, after_row_id, page_limit)
                .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
            row_count = row_count.saturating_add(row_ids.len() as u64);
            if row_count > maximum {
                break CommittedPopulationRecord::Unknown {
                    table_id,
                    snapshot_sequence: metadata.commit_sequence,
                };
            }
            if complete {
                break CommittedPopulationRecord::Exact {
                    table_id,
                    snapshot_sequence: metadata.commit_sequence,
                    rows: row_count,
                };
            }
            after_row_id = row_ids.last().copied();
        };
        drop(snapshot);

        let record = result.encode_record();
        CommittedPopulationRecord::decode_record(result.table_id(), &record).map_err(|error| {
            let reason = match error {
                StatisticsRecordError::InvalidLength => "invalid record length",
                StatisticsRecordError::InvalidMagic => "invalid record magic",
                StatisticsRecordError::InvalidStatus => "invalid record status",
                StatisticsRecordError::InvalidValue => "invalid record value",
            };
            StatisticsMaintenanceError::Storage(reason.to_owned())
        })?;
        let mut key = Vec::with_capacity(COMMITTED_POPULATION_KEY_PREFIX.len() + table_id.len());
        key.extend_from_slice(COMMITTED_POPULATION_KEY_PREFIX);
        key.extend_from_slice(&table_id);
        let mut write = self
            .database
            .begin_write()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        write
            .set_durability(redb::Durability::Immediate)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        write.set_two_phase_commit(true);
        {
            let mut stats = write
                .open_table(STATS)
                .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
            stats
                .insert(key.as_slice(), record.as_slice())
                .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        }
        if let Err(error) = write.commit() {
            mark_database_uncertain(self.database);
            return Err(StatisticsMaintenanceError::Storage(error.to_string()));
        }
        Ok(match result {
            CommittedPopulationRecord::Exact { .. } => StatisticsCollectionOutcome::Exact,
            CommittedPopulationRecord::Unknown { .. } => StatisticsCollectionOutcome::Unknown,
        })
    }

    /// Prune one bounded batch of expired durable outcomes. Storage failures
    /// quarantine this database handle; the owning service must reopen it.
    pub(crate) fn prune_expired_outcomes(
        &self,
        now_us: i64,
        max_scanned: usize,
    ) -> Result<crate::outcome::OutcomePruneResult, crate::outcome::OutcomeMaintenanceError> {
        let result = crate::outcome::prune_expired_outcomes(self.database, now_us, max_scanned);
        self.admissions
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_outcome_maintenance(&result);
        result
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

    fn build(
        database: &'db Database,
        config: StorageManagerConfig,
        validate_startup: bool,
    ) -> Result<Self, StorageManagerInitError> {
        if config.max_active_invocations == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroActiveInvocationLimit,
            ));
        }
        if config.max_active_analytical_invocations == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroAnalyticalInvocationLimit,
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
        if config.max_planner_estimate_requests == 0
            || config.max_analytical_planner_estimate_requests == Some(0)
        {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroPlannerEstimateRequestLimit,
            ));
        }
        if config.max_planner_estimate_requests > MAX_PLANNER_ESTIMATE_REQUESTS
            || config
                .max_analytical_planner_estimate_requests
                .is_some_and(|value| value > MAX_PLANNER_ESTIMATE_REQUESTS)
        {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::PlannerEstimateRequestLimitTooLarge,
            ));
        }
        if config.max_planner_estimate_candidates == 0
            || config.max_analytical_planner_estimate_candidates == Some(0)
        {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroPlannerEstimateCandidateLimit,
            ));
        }
        if config.max_planner_estimate_candidates > MAX_CONFIGURABLE_ESTIMATE_CANDIDATES
            || config
                .max_analytical_planner_estimate_candidates
                .is_some_and(|value| value > MAX_CONFIGURABLE_ESTIMATE_CANDIDATES)
        {
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
            config.max_active_analytical_invocations,
            config.max_statistics_collection_rows,
        );
        Ok(Self {
            database,
            ddl_fences: DdlFenceCoordinator::new(DdlFenceManager::new(), config.ddl_fence_timeout),
            max_active_invocations: config.max_active_invocations,
            max_active_analytical_invocations: config.max_active_analytical_invocations,
            admissions,
            token_requests,
            admin_cancellation_requests,
            authentication_failure_audits,
            max_invocation_age: config.max_invocation_age,
            max_invocation_scan_work: config.max_invocation_scan_work,
            max_planner_estimate_requests: config.max_planner_estimate_requests,
            max_analytical_planner_estimate_requests: config
                .max_analytical_planner_estimate_requests,
            max_planner_estimate_candidates: config.max_planner_estimate_candidates,
            max_analytical_planner_estimate_candidates: config
                .max_analytical_planner_estimate_candidates,
            interactive_transactions: Arc::new(Mutex::new(HashMap::new())),
            interactive_begin_requests: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// The effective hard cap shared by live manager handles for this database.
    pub fn max_active_invocations(&self) -> usize {
        self.max_active_invocations
            .min(self.admissions.limit.load(Ordering::Acquire))
    }

    /// The effective analytical snapshot cap shared by live manager handles.
    pub fn max_active_analytical_invocations(&self) -> usize {
        self.max_active_analytical_invocations
            .min(self.admissions.analytical_limit.load(Ordering::Acquire))
    }

    pub fn max_invocation_age(&self) -> Duration {
        self.max_invocation_age
    }

    pub fn max_invocation_scan_work(&self) -> usize {
        self.max_invocation_scan_work
    }

    pub fn max_planner_estimate_requests_for(
        &self,
        workload_class: crate::supervisor::WorkloadClass,
    ) -> usize {
        match workload_class {
            crate::supervisor::WorkloadClass::Operational => self.max_planner_estimate_requests,
            crate::supervisor::WorkloadClass::Analytical => self
                .max_analytical_planner_estimate_requests
                .unwrap_or(self.max_planner_estimate_requests),
        }
    }

    pub fn max_planner_estimate_candidates_for(
        &self,
        workload_class: crate::supervisor::WorkloadClass,
    ) -> usize {
        match workload_class {
            crate::supervisor::WorkloadClass::Operational => self.max_planner_estimate_candidates,
            crate::supervisor::WorkloadClass::Analytical => self
                .max_analytical_planner_estimate_candidates
                .unwrap_or(self.max_planner_estimate_candidates),
        }
    }

    /// Whether a managed invocation hit an ambiguous redb commit error and
    /// this database handle must be closed and reopened before more work.
    pub fn requires_reopen_after_commit_error(&self) -> bool {
        self.admissions.database_uncertain.load(Ordering::Acquire)
    }

    /// Begin a managed invocation after a trusted caller has established its
    /// identity and performed the required admission checks. Service callers
    /// should use [`Self::begin_authenticated_invocation`] to validate the
    /// principal against the pinned catalog. The slot remains held until
    /// commit, abort, expiry, or transaction drop.
    pub fn begin_invocation(
        &self,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<InvocationTransaction<'db>, TransactionError> {
        self.begin_invocation_in_class(
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
            crate::supervisor::WorkloadClass::Operational,
        )
    }

    fn begin_invocation_in_class(
        &self,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
        workload_class: crate::supervisor::WorkloadClass,
    ) -> Result<InvocationTransaction<'db>, TransactionError> {
        let started_at_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        let admission_permit = self
            .admissions
            .acquire(
                invocation_id,
                principal_id,
                workload_class,
                started_at_us,
                self.max_invocation_age,
            )
            .map_err(|error| match error {
                AdmissionError::Limit => TransactionError::AdmissionLimitExceeded,
                AdmissionError::Duplicate => TransactionError::DuplicateInvocation,
                AdmissionError::DatabaseUncertain => TransactionError::Storage(
                    "database requires reopen after an ambiguous commit error".into(),
                ),
                AdmissionError::RandomnessUnavailable => TransactionError::RandomnessUnavailable,
            })?;
        InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::managed(
                self.ddl_fences.clone(),
                admission_permit,
                self.max_invocation_age,
                self.max_invocation_scan_work,
                self.max_planner_estimate_requests_for(workload_class),
                self.max_planner_estimate_candidates_for(workload_class),
            ),
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )
    }

    /// Admit an invocation for a principal already authenticated by a trusted
    /// caller, then load its role closure from the invocation's pinned catalog
    /// snapshot. This validates that the principal is a live LOGIN user and
    /// guards the identity and membership records through commit. It does not
    /// verify credentials, token expiry, or revocation; callers must complete
    /// those checks before invoking this method.
    pub fn begin_authenticated_invocation(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(InvocationTransaction<'db>, HashSet<[u8; 16]>), TransactionError> {
        let mut transaction = self.begin_invocation(
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )?;
        let roles = transaction.load_catalog_role_closure(authenticated_principal_id)?;
        Ok((transaction, roles))
    }

    /// Authenticate a database-issued bearer token, then admit the invocation
    /// against one pinned catalog snapshot. Token, user, and membership bytes
    /// are guarded through commit; a concurrent revocation makes the pending
    /// invocation conflict.
    pub fn begin_token_authenticated_invocation(
        &self,
        invocation_id: [u8; 16],
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(InvocationTransaction<'db>, HashSet<[u8; 16]>), TransactionError> {
        self.begin_token_authenticated_invocation_in_class(
            SupervisorAdmissionRequest {
                invocation_id,
                token_id,
                retention_deadline_us,
                max_rows,
                max_bytes,
                deadline: None,
                workload_class: crate::supervisor::WorkloadClass::Operational,
            },
            token_secret,
        )
    }

    fn begin_token_authenticated_invocation_in_class(
        &self,
        request: SupervisorAdmissionRequest,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<(InvocationTransaction<'db>, HashSet<[u8; 16]>), TransactionError> {
        let principal_id = lookup_credential_principal(self.database, request.token_id)?;
        let mut transaction = self.begin_invocation_in_class(
            request.invocation_id,
            principal_id,
            request.retention_deadline_us,
            request.max_rows,
            request.max_bytes,
            request.workload_class,
        )?;
        let Some(now_us) = now_unix_micros() else {
            transaction.discard_unstarted_admission();
            return Err(TransactionError::ClockUnavailable);
        };
        let roles =
            transaction.authenticate_credential_token(request.token_id, token_secret, now_us)?;
        Ok((transaction, roles))
    }

    /// Authenticate a database token for a query/control gateway session using
    /// one read snapshot. This does not allocate an invocation or durable
    /// outcome; START must revalidate through token-bound admission.
    pub fn authenticate_query_session(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<AuthenticatedQuerySession, TransactionError> {
        let identity = match self.resolve_query_identity(token_id, token_secret) {
            Ok(identity) => identity,
            Err(TransactionError::AuthenticationFailed) => {
                if self.authentication_failure_audits.consume(token_id).is_ok() {
                    persist_authentication_failure_audit(self.database, token_id)?;
                }
                return Err(TransactionError::AuthenticationFailed);
            }
            Err(error) => return Err(error),
        };
        let connection_id = generate_query_connection_id()?;
        Ok(AuthenticatedQuerySession {
            connection_id,
            token_id,
            principal_id: identity.principal_id,
            principal_name: identity.principal_name,
            authority_epoch: identity.authority_epoch,
            role_ids: identity.role_ids,
        })
    }

    /// Open a server-owned transaction whose MVCC snapshot and write overlay
    /// stay alive across package requests. The transaction ID is generated by
    /// storage and is never treated as authentication authority.
    pub fn begin_interactive_transaction(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        outcome_retention_deadline_us: i64,
    ) -> Result<InteractiveTransactionBegin, InteractiveTransactionError> {
        let mut begin_request_id = [0; 16];
        getrandom::fill(&mut begin_request_id)
            .map_err(|_| TransactionError::RandomnessUnavailable)?;
        if begin_request_id == [0; 16] {
            return Err(TransactionError::RandomnessUnavailable.into());
        }
        self.begin_interactive_transaction_with_request_id(
            token_id,
            token_secret,
            outcome_retention_deadline_us,
            begin_request_id,
        )
    }

    /// Idempotently open or recover the transaction created for one client
    /// request. The key is scoped to the authenticated controller and retained
    /// through the corresponding outcome-retention deadline.
    pub fn begin_interactive_transaction_with_request_id(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        outcome_retention_deadline_us: i64,
        begin_request_id: [u8; 16],
    ) -> Result<InteractiveTransactionBegin, InteractiveTransactionError> {
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        if outcome_retention_deadline_us <= now_us || begin_request_id == [0; 16] {
            return Err(InteractiveTransactionError::InvalidRequest);
        }
        let _ = self.reap_interactive_transactions(now_us, 64)?;
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let mut begin_requests = self
            .interactive_begin_requests
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
        begin_requests.retain(|_, record| record.retain_until_us > now_us);
        let request_key = (session.principal_id(), begin_request_id);
        if let Some(record) = begin_requests.get(&request_key) {
            let entry = self.interactive_transaction_entry(record.transaction_id)?;
            let mut entry = self.try_lock_interactive_transaction(&entry)?;
            if entry.controller_id != session.principal_id() {
                return Err(InteractiveTransactionError::NotOwner);
            }
            self.expire_interactive_transaction_if_needed(&mut entry)?;
            entry.last_activity = Instant::now();
            return Ok(InteractiveTransactionBegin {
                transaction_id: record.transaction_id,
                state: entry.state,
                idle_timeout_ms: INTERACTIVE_TRANSACTION_IDLE_TIMEOUT
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                lifetime_ms: self
                    .max_invocation_age
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                max_package_bytes: crate::supervisor::MAX_ARTIFACT_BYTES,
                max_packages: u64::try_from(INTERACTIVE_TRANSACTION_MAX_PACKAGES)
                    .unwrap_or(u64::MAX),
            });
        }
        if self
            .interactive_transactions
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
            .len()
            >= MAX_RETAINED_INTERACTIVE_TRANSACTION_RECORDS
            || begin_requests.len() >= MAX_RETAINED_INTERACTIVE_TRANSACTION_RECORDS
        {
            return Err(InteractiveTransactionError::RegistryLimit);
        }
        let mut transaction_id = [0; 16];
        getrandom::fill(&mut transaction_id)
            .map_err(|_| TransactionError::RandomnessUnavailable)?;
        if transaction_id == [0; 16] {
            return Err(TransactionError::RandomnessUnavailable.into());
        }
        let (mut transaction, _role_ids) = self.begin_token_authenticated_invocation(
            transaction_id,
            token_id,
            token_secret,
            outcome_retention_deadline_us,
            INTERACTIVE_TRANSACTION_MAX_ROWS,
            INTERACTIVE_TRANSACTION_MAX_WRITE_BYTES,
        )?;
        if let Err(error) = persist_interactive_transaction_marker(
            self.database,
            transaction_id,
            session.principal_id(),
            outcome_retention_deadline_us,
        ) {
            let _ = transaction.abort();
            return Err(TransactionError::Storage(error).into());
        }
        let now = Instant::now();
        let entry = Arc::new(Mutex::new(InteractiveTransactionEntry {
            transaction_id,
            controller_id: session.principal_id(),
            outcome_retention_deadline_us,
            transaction: Arc::new(Mutex::new(Some(transaction))),
            state: InteractiveTransactionState::Active,
            started_at: now,
            last_activity: now,
            packages: HashMap::new(),
        }));
        let mut registry = match self.interactive_transactions.lock() {
            Ok(registry) => registry,
            Err(_) => {
                if let Ok(entry) = entry.lock()
                    && let Ok(mut transaction) = entry.transaction.lock()
                    && let Some(mut transaction) = transaction.take()
                {
                    let _ = transaction.abort();
                }
                return Err(InteractiveTransactionError::RegistryPoisoned);
            }
        };
        if registry.contains_key(&transaction_id) {
            let duplicate = entry
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            let mut owner = duplicate
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            if let Some(mut transaction) = owner.take() {
                transaction.abort()?;
            }
            return Err(TransactionError::DuplicateInvocation.into());
        }
        registry.insert(transaction_id, entry);
        begin_requests.insert(
            request_key,
            InteractiveBeginRequestRecord {
                transaction_id,
                retain_until_us: outcome_retention_deadline_us,
            },
        );
        Ok(InteractiveTransactionBegin {
            transaction_id,
            state: InteractiveTransactionState::Active,
            idle_timeout_ms: INTERACTIVE_TRANSACTION_IDLE_TIMEOUT
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            lifetime_ms: self
                .max_invocation_age
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            max_package_bytes: crate::supervisor::MAX_ARTIFACT_BYTES,
            max_packages: u64::try_from(INTERACTIVE_TRANSACTION_MAX_PACKAGES).unwrap_or(u64::MAX),
        })
    }

    /// Authenticate the controller on every reattachment. A live transaction
    /// may move between TCP connections, but its ID is not a bearer token.
    pub fn attach_interactive_transaction(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
    ) -> Result<InteractiveTransactionState, InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = self.try_lock_interactive_transaction(&entry)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        self.expire_interactive_transaction_if_needed(&mut entry)?;
        match entry.state {
            InteractiveTransactionState::Active | InteractiveTransactionState::AbortOnly => {
                entry.last_activity = Instant::now();
                Ok(entry.state)
            }
            InteractiveTransactionState::Expired => Err(InteractiveTransactionError::Expired),
            _ => Err(InteractiveTransactionError::NotFound),
        }
    }

    /// Reserve and run one package under the retained transaction context.
    /// The transaction lock serializes package execution and the ID/digest
    /// record prevents a repeated package ID from executing again.
    pub fn execute_interactive_package<T>(
        &self,
        request: InteractivePackageRequest<'_>,
        execute: impl FnOnce(&mut InvocationTransaction<'db>) -> Result<T, TransactionError>,
    ) -> Result<T, InteractiveTransactionError> {
        let InteractivePackageRequest {
            token_id,
            token_secret,
            transaction_id,
            package_id,
            package_digest,
            requested_scope,
            actor,
        } = request;
        if package_id == [0; 16] || transaction_id == [0; 16] {
            return Err(InteractiveTransactionError::InvalidRequest);
        }
        let session = self.authenticate_query_session(token_id, token_secret)?;
        if actor.is_some() {
            return Err(InteractiveTransactionError::InvalidActor);
        }
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = self.try_lock_interactive_transaction(&entry)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        self.expire_interactive_transaction_if_needed(&mut entry)?;
        match entry.state {
            InteractiveTransactionState::Active => (),
            InteractiveTransactionState::AbortOnly => {
                return Err(InteractiveTransactionError::AbortOnly);
            }
            InteractiveTransactionState::Expired => {
                return Err(InteractiveTransactionError::Expired);
            }
            _ => return Err(InteractiveTransactionError::NotFound),
        }
        if entry.packages.contains_key(&package_id) {
            let same_package = entry
                .packages
                .get(&package_id)
                .is_some_and(|record| record.digest == package_digest);
            return Err(if same_package {
                InteractiveTransactionError::DuplicatePackage
            } else {
                InteractiveTransactionError::PackageConflict
            });
        }
        if entry.packages.len() >= INTERACTIVE_TRANSACTION_MAX_PACKAGES {
            return Err(InteractiveTransactionError::PackageLimit);
        }
        entry.packages.insert(
            package_id,
            InteractivePackageRecord {
                digest: package_digest,
                state: InteractivePackageState::Running,
                result_lost: false,
                actor: None,
            },
        );
        entry.last_activity = Instant::now();
        let execution = {
            let mut owner = entry
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            let transaction = owner
                .as_mut()
                .ok_or(InteractiveTransactionError::NotFound)?;
            transaction.set_package_requested_scope(Some(requested_scope));
            let result = execute(transaction);
            transaction.set_package_requested_scope(None);
            result
        };
        match execution {
            Ok(result) => {
                if let Some(record) = entry.packages.get_mut(&package_id) {
                    record.state = InteractivePackageState::Completed;
                }
                entry.last_activity = Instant::now();
                Ok(result)
            }
            Err(error) => {
                if let Some(record) = entry.packages.get_mut(&package_id) {
                    record.state = InteractivePackageState::Failed;
                }
                entry.state = InteractiveTransactionState::AbortOnly;
                let abort_result = entry
                    .transaction
                    .lock()
                    .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                    .as_mut()
                    .ok_or(InteractiveTransactionError::NotFound)?
                    .abort();
                entry.last_activity = Instant::now();
                abort_result?;
                Err(error.into())
            }
        }
    }

    /// Run a query package through the supervisor's normal disposable worker
    /// channels while sharing the transaction retained by the interactive
    /// registry. A successful callback leaves the transaction open; only the
    /// controller's later COMMIT can publish it. Worker/setup errors make the
    /// transaction abort-only and abort the shared storage context.
    pub fn execute_interactive_package_with_supervisor<T, E>(
        &self,
        request: InteractivePackageRequest<'_>,
        execute: impl FnOnce(&mut SupervisorInvocation<'db>) -> Result<T, E>,
    ) -> Result<T, InteractivePackageExecutionError<E>> {
        let InteractivePackageRequest {
            token_id,
            token_secret,
            transaction_id,
            package_id,
            package_digest,
            requested_scope,
            actor,
        } = request;
        if package_id == [0; 16] || transaction_id == [0; 16] {
            return Err(InteractivePackageExecutionError::Transaction(
                InteractiveTransactionError::InvalidRequest,
            ));
        }
        let session = self
            .authenticate_query_session(token_id, token_secret)
            .map_err(|error| InteractivePackageExecutionError::Transaction(error.into()))?;
        let actor_session = match actor {
            Some(proof) => {
                let actor_session = self
                    .authenticate_query_session(proof.token_id, proof.token_secret)
                    .map_err(|error| InteractivePackageExecutionError::Transaction(error.into()))?;
                if actor_session.principal_name() != proof.expected_name {
                    return Err(InteractivePackageExecutionError::Transaction(
                        InteractiveTransactionError::InvalidActor,
                    ));
                }
                actor_session
            }
            None => session.clone(),
        };
        let actor_record = Some(InteractivePackageActorRecord {
            token_id: actor.map_or(token_id, |proof| proof.token_id),
            principal_id: actor_session.principal_id(),
            principal_name: actor_session.principal_name().to_owned(),
            authority_epoch: actor_session.authority_epoch(),
            role_ids: actor_session.role_ids().clone(),
        });
        let actor_id = actor_session.principal_id();
        let actor_role_ids = actor_session.role_ids().clone();
        let entry = self
            .interactive_transaction_entry(transaction_id)
            .map_err(InteractivePackageExecutionError::Transaction)?;
        let mut entry = self
            .try_lock_interactive_transaction(&entry)
            .map_err(InteractivePackageExecutionError::Transaction)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractivePackageExecutionError::Transaction(
                InteractiveTransactionError::NotOwner,
            ));
        }
        self.expire_interactive_transaction_if_needed(&mut entry)
            .map_err(InteractivePackageExecutionError::Transaction)?;
        match entry.state {
            InteractiveTransactionState::Active => (),
            InteractiveTransactionState::AbortOnly => {
                return Err(InteractivePackageExecutionError::Transaction(
                    InteractiveTransactionError::AbortOnly,
                ));
            }
            InteractiveTransactionState::Expired => {
                return Err(InteractivePackageExecutionError::Transaction(
                    InteractiveTransactionError::Expired,
                ));
            }
            _ => {
                return Err(InteractivePackageExecutionError::Transaction(
                    InteractiveTransactionError::NotFound,
                ));
            }
        }
        if entry.packages.contains_key(&package_id) {
            let same_package = entry
                .packages
                .get(&package_id)
                .is_some_and(|record| record.digest == package_digest);
            return Err(InteractivePackageExecutionError::Transaction(
                if same_package {
                    InteractiveTransactionError::DuplicatePackage
                } else {
                    InteractiveTransactionError::PackageConflict
                },
            ));
        }
        if entry.packages.len() >= INTERACTIVE_TRANSACTION_MAX_PACKAGES {
            return Err(InteractivePackageExecutionError::Transaction(
                InteractiveTransactionError::PackageLimit,
            ));
        }
        entry.packages.insert(
            package_id,
            InteractivePackageRecord {
                digest: package_digest,
                state: InteractivePackageState::Running,
                result_lost: false,
                actor: actor_record.clone(),
            },
        );
        entry.last_activity = Instant::now();
        let transaction_handle = Arc::clone(&entry.transaction);
        {
            let mut owner = transaction_handle.lock().map_err(|_| {
                InteractivePackageExecutionError::Transaction(
                    InteractiveTransactionError::RegistryPoisoned,
                )
            })?;
            let transaction = owner.as_mut().ok_or_else(|| {
                InteractivePackageExecutionError::Transaction(InteractiveTransactionError::NotFound)
            })?;
            transaction.set_package_requested_scope(Some(requested_scope));
            transaction.set_package_principal(Some(actor_id));
        }
        let mut supervisor = SupervisorInvocation {
            invocation_id: transaction_id,
            principal_id: actor_id,
            role_ids: actor_role_ids,
            transaction: transaction_handle,
            channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
            deadline: entry.started_at.checked_add(self.max_invocation_age),
            abort_on_drop: false,
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(&mut supervisor)));
        if let Ok(mut owner) = supervisor.transaction.lock()
            && let Some(transaction) = owner.as_mut()
        {
            transaction.set_package_requested_scope(None);
            transaction.set_package_principal(None);
        }
        match result {
            Ok(Ok(value)) => {
                let transaction_live = supervisor
                    .transaction
                    .lock()
                    .map(|owner| owner.is_some())
                    .unwrap_or(false);
                if transaction_live {
                    if let Some(record) = entry.packages.get_mut(&package_id) {
                        record.state = InteractivePackageState::Completed;
                    }
                    entry.last_activity = Instant::now();
                    Ok(value)
                } else {
                    if let Some(record) = entry.packages.get_mut(&package_id) {
                        record.state = InteractivePackageState::Failed;
                    }
                    entry.state = InteractiveTransactionState::AbortOnly;
                    Err(InteractivePackageExecutionError::Transaction(
                        InteractiveTransactionError::NotFound,
                    ))
                }
            }
            Ok(Err(error)) => {
                if let Some(record) = entry.packages.get_mut(&package_id) {
                    record.state = InteractivePackageState::Failed;
                }
                entry.state = InteractiveTransactionState::AbortOnly;
                if let Ok(mut owner) = supervisor.transaction.lock()
                    && let Some(mut transaction) = owner.take()
                {
                    let _ = transaction.abort();
                }
                entry.last_activity = Instant::now();
                Err(InteractivePackageExecutionError::Execution(error))
            }
            Err(_) => {
                if let Some(record) = entry.packages.get_mut(&package_id) {
                    record.state = InteractivePackageState::Failed;
                }
                entry.state = InteractiveTransactionState::AbortOnly;
                if let Ok(mut owner) = supervisor.transaction.lock()
                    && let Some(mut transaction) = owner.take()
                {
                    let _ = transaction.abort();
                }
                entry.last_activity = Instant::now();
                Err(InteractivePackageExecutionError::Panicked)
            }
        }
    }

    /// Read package execution state without returning or replaying result
    /// bytes. The actor is currently the transaction controller; RUNAS actor
    /// binding is added by the later authorization slice.
    pub fn interactive_package_status(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<InteractivePackageStatus, InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = entry
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        self.expire_interactive_transaction_if_needed(&mut entry)?;
        entry.last_activity = Instant::now();
        Ok(match entry.packages.get(&package_id) {
            Some(record) => InteractivePackageStatus {
                state: record.state,
                result_lost: record.result_lost,
            },
            None => InteractivePackageStatus {
                state: InteractivePackageState::Unknown,
                result_lost: false,
            },
        })
    }

    /// Record that a completed package's result stream could not be delivered.
    /// Status can then reconcile execution without making the bytes replayable.
    pub fn mark_interactive_package_result_lost(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<(), InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = entry
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        let record = entry
            .packages
            .get_mut(&package_id)
            .ok_or(InteractiveTransactionError::NotFound)?;
        if record.state != InteractivePackageState::Completed {
            return Err(InteractiveTransactionError::InvalidRequest);
        }
        record.result_lost = true;
        entry.last_activity = Instant::now();
        Ok(())
    }

    /// Return live state or a retained durable decision after authenticating
    /// the current controller credential.
    pub fn interactive_transaction_status(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
    ) -> Result<InteractiveTransactionState, InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        match self.interactive_transaction_entry(transaction_id) {
            Ok(entry) => {
                let mut entry = entry
                    .lock()
                    .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
                if session.principal_id() != entry.controller_id {
                    return Err(InteractiveTransactionError::NotOwner);
                }
                self.expire_interactive_transaction_if_needed(&mut entry)?;
                entry.last_activity = Instant::now();
                return Ok(entry.state);
            }
            Err(InteractiveTransactionError::NotFound) => (),
            Err(error) => return Err(error),
        }
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        let status = self
            .get_invocation_status(transaction_id, session.principal_id(), now_us)
            .map_err(|_| InteractiveTransactionError::Transaction(TransactionError::NotFound))?
            .ok_or(InteractiveTransactionError::NotFound)?;
        match status.status {
            InvocationStatus::Terminal(outcome) => match outcome.status {
                crate::outcome::OutcomeStatus::Committed => {
                    Ok(InteractiveTransactionState::Committed)
                }
                crate::outcome::OutcomeStatus::Aborted => Ok(InteractiveTransactionState::Aborted),
            },
            InvocationStatus::Active { .. } | InvocationStatus::Committing { .. } => {
                Err(InteractiveTransactionError::NotFound)
            }
        }
    }

    /// Reap idle/lifetime-expired transactions and terminal package records in
    /// a bounded pass. The supervisor should call this from its periodic
    /// maintenance loop; a caller may also invoke it before admitting work.
    pub fn reap_interactive_transactions(
        &self,
        now_us: i64,
        max_entries: usize,
    ) -> Result<usize, InteractiveTransactionError> {
        if now_us <= 0 || max_entries == 0 {
            return Err(InteractiveTransactionError::InvalidRequest);
        }
        let candidates = {
            let registry = self
                .interactive_transactions
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            registry
                .iter()
                .take(max_entries.min(1024))
                .map(|(id, entry)| (*id, Arc::clone(entry)))
                .collect::<Vec<_>>()
        };
        let mut remove = Vec::new();
        let mut reaped = 0usize;
        for (transaction_id, entry) in candidates {
            let mut entry = match entry.try_lock() {
                Ok(entry) => entry,
                Err(std::sync::TryLockError::WouldBlock) => continue,
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(InteractiveTransactionError::RegistryPoisoned);
                }
            };
            let was_active = entry.state == InteractiveTransactionState::Active;
            self.expire_interactive_transaction_if_needed(&mut entry)?;
            if was_active && entry.state == InteractiveTransactionState::Expired {
                reaped = reaped.saturating_add(1);
            }
            if matches!(
                entry.state,
                InteractiveTransactionState::Committed
                    | InteractiveTransactionState::Aborted
                    | InteractiveTransactionState::Expired
            ) && entry.outcome_retention_deadline_us <= now_us
            {
                remove.push(transaction_id);
            }
        }
        if !remove.is_empty() {
            let mut registry = self
                .interactive_transactions
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            for transaction_id in remove {
                if registry.remove(&transaction_id).is_some() {
                    reaped = reaped.saturating_add(1);
                }
            }
        }
        Ok(reaped)
    }

    /// Atomically publish every package's writes and release the retained
    /// snapshot. Only the authenticated controller can commit.
    pub fn commit_interactive_transaction(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
    ) -> Result<u64, InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let transaction = {
            let mut entry = self.try_lock_interactive_transaction(&entry)?;
            if session.principal_id() != entry.controller_id {
                return Err(InteractiveTransactionError::NotOwner);
            }
            self.expire_interactive_transaction_if_needed(&mut entry)?;
            if entry.state == InteractiveTransactionState::AbortOnly {
                return Err(InteractiveTransactionError::AbortOnly);
            }
            if entry.state != InteractiveTransactionState::Active {
                return Err(InteractiveTransactionError::NotFound);
            }
            let staged_actor_ids = entry
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                .as_ref()
                .ok_or(InteractiveTransactionError::NotFound)?
                .staged_write_actor_ids()?;
            let actor_authority_current = staged_actor_ids.into_iter().all(|writer_id| {
                entry.packages.values().any(|package| {
                    let Some(actor) = &package.actor else {
                        return false;
                    };
                    actor.principal_id == writer_id
                        && self
                            .resolve_query_identity_by_token_id(actor.token_id)
                            .is_ok_and(|identity| {
                                identity.principal_id == actor.principal_id
                                    && identity.principal_name == actor.principal_name
                                    && identity.authority_epoch == actor.authority_epoch
                                    && identity.role_ids == actor.role_ids
                            })
                })
            });
            if !actor_authority_current {
                entry.state = InteractiveTransactionState::AbortOnly;
                if let Some(mut transaction) = entry
                    .transaction
                    .lock()
                    .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                    .take()
                {
                    transaction.abort()?;
                }
                entry.last_activity = Instant::now();
                return Err(InteractiveTransactionError::InvalidActor);
            }
            entry.state = InteractiveTransactionState::Committing;
            entry
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                .take()
                .ok_or(InteractiveTransactionError::NotFound)?
        };
        let outcome = transaction.commit();
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = entry
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
        match &outcome {
            Ok(_) => entry.state = InteractiveTransactionState::Committed,
            Err(_) => {
                let status = now_unix_micros().and_then(|now_us| {
                    self.get_invocation_status(transaction_id, session.principal_id(), now_us)
                        .ok()
                        .flatten()
                });
                entry.state = match status.map(|status| status.status) {
                    Some(InvocationStatus::Terminal(outcome))
                        if outcome.status == crate::outcome::OutcomeStatus::Aborted =>
                    {
                        InteractiveTransactionState::Aborted
                    }
                    _ => InteractiveTransactionState::Committing,
                };
            }
        }
        if outcome.is_ok() {
            remove_interactive_transaction_marker(self.database, transaction_id).map_err(
                |error| InteractiveTransactionError::Transaction(TransactionError::Storage(error)),
            )?;
        }
        outcome.map_err(InteractiveTransactionError::Transaction)
    }

    /// Abort the complete transaction and release its retained snapshot.
    pub fn rollback_interactive_transaction(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
    ) -> Result<(), InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry_guard = self.try_lock_interactive_transaction(&entry)?;
        if session.principal_id() != entry_guard.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        self.expire_interactive_transaction_if_needed(&mut entry_guard)?;
        if !matches!(
            entry_guard.state,
            InteractiveTransactionState::Active | InteractiveTransactionState::AbortOnly
        ) {
            return Err(InteractiveTransactionError::NotFound);
        }
        if let Some(mut transaction) = entry_guard
            .transaction
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
            .take()
        {
            transaction.abort()?;
        }
        remove_interactive_transaction_marker(self.database, transaction_id)
            .map_err(TransactionError::Storage)?;
        entry_guard.state = InteractiveTransactionState::Aborted;
        entry_guard.last_activity = Instant::now();
        Ok(())
    }

    fn interactive_transaction_entry(
        &self,
        transaction_id: [u8; 16],
    ) -> Result<Arc<Mutex<InteractiveTransactionEntry<'db>>>, InteractiveTransactionError> {
        self.interactive_transactions
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
            .get(&transaction_id)
            .cloned()
            .ok_or(InteractiveTransactionError::NotFound)
    }

    fn try_lock_interactive_transaction<'entry>(
        &self,
        entry: &'entry Arc<Mutex<InteractiveTransactionEntry<'db>>>,
    ) -> Result<
        std::sync::MutexGuard<'entry, InteractiveTransactionEntry<'db>>,
        InteractiveTransactionError,
    > {
        match entry.try_lock() {
            Ok(entry) => Ok(entry),
            Err(std::sync::TryLockError::WouldBlock) => {
                Err(InteractiveTransactionError::ConcurrentOperation)
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(InteractiveTransactionError::RegistryPoisoned)
            }
        }
    }

    fn expire_interactive_transaction_if_needed(
        &self,
        entry: &mut InteractiveTransactionEntry<'db>,
    ) -> Result<(), InteractiveTransactionError> {
        if entry.state != InteractiveTransactionState::Active {
            return Ok(());
        }
        if entry.started_at.elapsed() >= self.max_invocation_age
            || entry.last_activity.elapsed() >= INTERACTIVE_TRANSACTION_IDLE_TIMEOUT
        {
            if let Some(mut transaction) = entry
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                .take()
            {
                transaction.abort()?;
            }
            remove_interactive_transaction_marker(self.database, entry.transaction_id)
                .map_err(TransactionError::Storage)?;
            entry.state = InteractiveTransactionState::Expired;
        }
        Ok(())
    }

    /// Discover every schema visible to a token-authenticated principal from
    /// one request-scoped catalog snapshot. This read does not register an
    /// invocation or write a durable outcome. The bounded result is suitable
    /// for client-side requested-scope construction.
    pub fn discover_token_authorized_schemas(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<Vec<crate::metadata_discovery::VisibleSchema>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;

        let mut schemas = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let page = transaction.list_catalog_authorized_schemas(
                crate::metadata_discovery::MAX_SCHEMAS_PER_PAGE,
                continuation,
            )?;
            if schemas.len().saturating_add(page.schemas.len()) > MAX_DISCOVERED_SCHEMAS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for schema in &page.schemas {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(schema.name.len());
                if encoded_bytes > MAX_DISCOVERED_SCHEMA_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            schemas.extend(page.schemas);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        for schema in transaction.list_catalog_administrative_schemas()? {
            if !schemas
                .iter()
                .any(|existing| existing.schema_id == schema.schema_id)
            {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(schema.name.len());
                if schemas.len() >= MAX_DISCOVERED_SCHEMAS
                    || encoded_bytes > MAX_DISCOVERED_SCHEMA_BYTES
                {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                schemas.push(schema);
            }
        }
        schemas.sort_unstable_by_key(|schema| schema.schema_id);
        Ok(schemas)
    }

    /// Discover relations for one schema visible to a token-authenticated
    /// principal. Hidden and absent schemas return `None`; an authorized
    /// empty schema returns an empty vector. The read uses one request-scoped
    /// catalog snapshot and does not admit an invocation or write an outcome.
    pub fn discover_token_authorized_relations(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::VisibleRelation>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        if schema_id == crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID {
            return Ok(Some(
                crate::metadata_discovery::information_schema_relations().into(),
            ));
        }
        if let Some(relations) = transaction.list_catalog_administrative_relations(schema_id)? {
            return Ok(Some(relations));
        }

        let mut relations = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let Some(page) = transaction.list_catalog_authorized_relations(
                schema_id,
                crate::metadata_discovery::MAX_RELATIONS_PER_PAGE,
                continuation,
            )?
            else {
                return Ok(None);
            };
            if relations.len().saturating_add(page.relations.len()) > MAX_DISCOVERED_RELATIONS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for relation in &page.relations {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(relation.name.len());
                if encoded_bytes > MAX_DISCOVERED_RELATION_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            relations.extend(page.relations);
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(Some(relations));
            }
        }
    }

    /// Describe every declared column for a relation visible to a
    /// token-authenticated principal. Hidden and absent schema/relation pairs
    /// return `None`; the public information_schema shapes are always visible.
    /// The bounded read uses a single catalog snapshot and creates no
    /// invocation admission or durable outcome.
    pub fn discover_token_authorized_columns(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::VisibleColumn>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        if schema_id != crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID
            && let Some(columns) =
                transaction.describe_catalog_administrative_columns(schema_id, relation_id)?
        {
            return Ok(Some(columns));
        }

        let mut columns = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let Some(page) = transaction.describe_catalog_authorized_columns_page(
                schema_id,
                relation_id,
                crate::metadata_discovery::MAX_COLUMNS_PER_PAGE,
                continuation,
            )?
            else {
                return Ok(None);
            };
            if columns.len().saturating_add(page.columns.len()) > MAX_DISCOVERED_COLUMNS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for column in &page.columns {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(column.name.len())
                    .saturating_add(
                        crate::metadata_discovery::scalar_type_name(column.scalar_type).len(),
                    )
                    .saturating_add(5);
                if encoded_bytes > MAX_DISCOVERED_COLUMN_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            columns.extend(page.columns);
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(Some(columns));
            }
        }
    }

    /// List logical index definitions for a relation only when the
    /// authenticated control principal has administrative schema authority.
    /// Hidden and absent relations return `None`.
    pub fn discover_token_administrative_indexes(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeIndex>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(indexes) =
            transaction.list_catalog_administrative_indexes(schema_id, relation_id)?
        else {
            return Ok(None);
        };
        let mut encoded_bytes = 0usize;
        for index in &indexes {
            encoded_bytes = encoded_bytes
                .saturating_add(17)
                .saturating_add(index.column_ids.len().saturating_mul(16));
            if encoded_bytes > MAX_DISCOVERED_INDEX_BYTES {
                return Err(TransactionError::InvalidMetadataRequest);
            }
        }
        if indexes.len() > MAX_DISCOVERED_INDEXES {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(indexes))
    }

    /// Resolve one user or role name for an authenticated identity manager.
    /// Unauthorized and absent names share the same `None` result.
    pub fn resolve_token_administrative_identity_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_administrative_identity_name(name, kind)
    }

    /// Resolve a policy identity name only for an authenticated owner or
    /// MANAGE_POLICIES holder on the policy's schema/table, or a database
    /// administrator. Unauthorized and absent identities share `None`.
    pub fn resolve_token_policy_identity_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_policy_identity_name(schema_id, table_id, name, kind)
    }

    /// Resolve a USER or ROLE name only for an authenticated schema/table
    /// ALTER grantee, owner, or database administrator. Owner transfer
    /// rechecks current authority at mutation time.
    pub fn resolve_token_alter_identity_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_alter_identity_name(object_id, name, kind)
    }

    /// Resolve a grant recipient within the authenticated grant authority of
    /// one schema or table. Unauthorized and absent identities both return None.
    pub fn resolve_token_grant_target_identity_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_name: &str,
        table_name: Option<&str>,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<
        Option<crate::metadata_discovery::AdministrativeGrantIdentityResolution>,
        TransactionError,
    > {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_grant_target_identity_name(schema_name, table_name, name, kind)
    }

    /// Resolve a policy name inside an authenticated schema/table scope.
    /// Absent names and callers without policy DDL authority share `None`.
    pub fn resolve_token_policy_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        is_cell: Option<bool>,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_policy_name(schema_id, table_id, name, is_cell)
    }

    /// Describe logical constraints for a relation only when the
    /// authenticated control principal has administrative schema authority.
    /// Hidden foreign-key targets are redacted from the logical response.
    pub fn discover_token_administrative_constraints(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeConstraint>>, TransactionError>
    {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(constraints) =
            transaction.list_catalog_administrative_constraints(schema_id, relation_id)?
        else {
            return Ok(None);
        };
        let mut encoded_bytes = 0usize;
        for constraint in &constraints {
            encoded_bytes = encoded_bytes
                .saturating_add(19)
                .saturating_add(constraint.kind.as_wire_name().len())
                .saturating_add(constraint.column_ids.len().saturating_mul(16))
                .saturating_add(usize::from(constraint.referenced_table_id.is_some()) * 16)
                .saturating_add(constraint.referenced_column_ids.len().saturating_mul(16));
            if encoded_bytes > MAX_DISCOVERED_CONSTRAINT_BYTES {
                return Err(TransactionError::InvalidMetadataRequest);
            }
        }
        if constraints.len() > MAX_DISCOVERED_CONSTRAINTS {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(constraints))
    }

    /// List direct table/column grants when the authenticated principal has
    /// current administrative schema authority.
    pub fn discover_token_administrative_grants(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeGrant>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(grants) =
            transaction.list_catalog_administrative_grants(schema_id, relation_id)?
        else {
            return Ok(None);
        };
        let encoded_bytes = grants.iter().fold(0usize, |total, grant| {
            total
                .saturating_add(81)
                .saturating_add(grant.privilege.as_wire_name().len())
                .saturating_add(grant.target_scope.as_wire_name().len())
        });
        if grants.len() > MAX_DISCOVERED_GRANTS || encoded_bytes > MAX_DISCOVERED_GRANT_BYTES {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(grants))
    }

    /// List secret-free policy summaries when the authenticated principal has
    /// current schema-owner/database-administrator authority.
    pub fn discover_token_administrative_policies(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativePolicy>>, TransactionError>
    {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(policies) =
            transaction.list_catalog_administrative_policies(schema_id, relation_id)?
        else {
            return Ok(None);
        };
        let encoded_bytes = policies.iter().fold(0usize, |total, policy| {
            total
                .saturating_add(33)
                .saturating_add(policy.column_ids.len().saturating_mul(16))
                .saturating_add(
                    policy
                        .recipients
                        .as_ref()
                        .map_or(1, |recipients| recipients.len().saturating_mul(21)),
                )
        });
        if policies.len() > MAX_DISCOVERED_POLICIES || encoded_bytes > MAX_DISCOVERED_POLICY_BYTES {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(policies))
    }

    /// List direct dependency edges touching an administratively visible
    /// object, omitting any edge with a hidden endpoint.
    pub fn discover_token_administrative_dependencies(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        object_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeDependency>>, TransactionError>
    {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(dependencies) = transaction.list_catalog_administrative_dependencies(object_id)?
        else {
            return Ok(None);
        };
        let encoded_bytes = dependencies.iter().fold(0usize, |total, edge| {
            total.saturating_add(35 + edge.kind.as_wire_name().len())
        });
        if dependencies.len() > MAX_DISCOVERED_DEPENDENCIES
            || encoded_bytes > MAX_DISCOVERED_DEPENDENCY_BYTES
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(dependencies))
    }

    /// Refresh the current login and role closure before a new execution or
    /// control operation, preserving this network connection's session ID.
    pub fn revalidate_query_session(
        &self,
        session: &mut AuthenticatedQuerySession,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<(), TransactionError> {
        let identity = self.resolve_query_identity(session.token_id, token_secret)?;
        if identity.principal_id != session.principal_id {
            return Err(TransactionError::AuthenticationFailed);
        }
        session.principal_name = identity.principal_name;
        session.authority_epoch = identity.authority_epoch;
        session.role_ids = identity.role_ids;
        Ok(())
    }

    fn resolve_query_identity(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<ResolvedQueryIdentity, TransactionError> {
        self.resolve_query_identity_inner(token_id, Some(token_secret))
    }

    fn resolve_query_identity_by_token_id(
        &self,
        token_id: [u8; 16],
    ) -> Result<ResolvedQueryIdentity, TransactionError> {
        self.resolve_query_identity_inner(token_id, None)
    }

    fn resolve_query_identity_inner(
        &self,
        token_id: [u8; 16],
        token_secret: Option<&[u8; TOKEN_SECRET_BYTES]>,
    ) -> Result<ResolvedQueryIdentity, TransactionError> {
        if token_id == [0; 16] {
            return Err(TransactionError::AuthenticationFailed);
        }
        let Some(now_us) = now_unix_micros() else {
            return Err(TransactionError::ClockUnavailable);
        };
        let read = self
            .database
            .begin_read()
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let key = credential_catalog_key(token_id);
        let bytes = {
            let catalog = read
                .open_table(CATALOG)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            catalog
                .get(key.as_slice())
                .map_err(|error| TransactionError::Storage(error.to_string()))?
                .map(|value| value.value().to_vec())
                .ok_or(TransactionError::AuthenticationFailed)?
        };
        let record = decode_registered_envelope(&bytes)
            .map_err(|_| TransactionError::AuthenticationFailed)?;
        if record.logical_key != key {
            return Err(TransactionError::AuthenticationFailed);
        }
        let credential =
            decode_verifier_record(&record).map_err(|_| TransactionError::AuthenticationFailed)?;
        if !credential.is_valid_at(now_us)
            || token_secret.is_some_and(|secret| !credential.matches_secret(secret))
        {
            return Err(TransactionError::AuthenticationFailed);
        }
        let principal_id = credential.user_id;
        let snapshot = RowSnapshot::new(read);
        let role_ids = token_actor_roles(&snapshot, principal_id).map_err(|error| match error {
            TokenLifecycleError::TokenUnavailable => TransactionError::AuthenticationFailed,
            TokenLifecycleError::Storage(message) => TransactionError::Storage(message),
            _ => TransactionError::AuthenticationFailed,
        })?;
        let principal = snapshot
            .get_catalog_record(&principal_catalog_key(principal_id))
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .ok_or(TransactionError::AuthenticationFailed)?;
        let principal_name = principal
            .fields
            .iter()
            .find(|field| field.tag == 2)
            .and_then(|field| std::str::from_utf8(&field.payload).ok())
            .filter(|name| is_normalized_principal_name(name))
            .ok_or(TransactionError::AuthenticationFailed)?
            .to_owned();
        let authority_epoch = principal
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .and_then(|field| <[u8; 8]>::try_from(field.payload.as_slice()).ok())
            .map(u64::from_le_bytes)
            .filter(|epoch| *epoch != 0)
            .ok_or(TransactionError::AuthenticationFailed)?;
        Ok(ResolvedQueryIdentity {
            principal_id,
            principal_name,
            authority_epoch,
            role_ids,
        })
    }

    /// Admit a token-authenticated invocation into the supervisor-owned
    /// storage context used by planner, executor, and terminal IPC handlers.
    pub fn admit_supervisor_invocation(
        &self,
        invocation_id: [u8; 16],
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        self.admit_supervisor_invocation_inner(
            SupervisorAdmissionRequest {
                invocation_id,
                token_id,
                retention_deadline_us,
                max_rows,
                max_bytes,
                deadline: None,
                workload_class: crate::supervisor::WorkloadClass::Operational,
            },
            token_secret,
            None,
        )
    }

    /// Admit a token-authenticated supervisor invocation with an additional
    /// caller-requested execution deadline bounded by trusted scheduling.
    pub fn admit_supervisor_invocation_with_deadline(
        &self,
        request: SupervisorAdmissionRequest,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        if request.deadline.is_none() {
            return Err(TransactionError::InvalidContext);
        }
        self.admit_supervisor_invocation_inner(request, token_secret, None)
    }

    /// Admit an invocation and intersect its catalog-derived authority with
    /// the client's decoded requested capability set.
    pub fn admit_supervisor_invocation_with_deadline_and_scope(
        &self,
        request: SupervisorAdmissionRequest,
        requested_scope: crate::submission_scope::RequestedScope,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        if request.deadline.is_none() {
            return Err(TransactionError::InvalidContext);
        }
        self.admit_supervisor_invocation_inner(request, token_secret, Some(requested_scope))
    }

    fn admit_supervisor_invocation_inner(
        &self,
        request: SupervisorAdmissionRequest,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        requested_scope: Option<crate::submission_scope::RequestedScope>,
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, request.token_id)?;
        let (mut transaction, role_ids) =
            self.begin_token_authenticated_invocation_in_class(request, token_secret)?;
        if let Some(requested_scope) = requested_scope {
            transaction.restrict_to_requested_scope(requested_scope);
        }
        Ok(SupervisorInvocation {
            invocation_id: request.invocation_id,
            principal_id,
            role_ids,
            transaction: Arc::new(Mutex::new(Some(transaction))),
            channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
            deadline: request.deadline,
            abort_on_drop: true,
        })
    }

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

    /// Create a table and its ordered column catalog in one authorized DDL
    /// commit. Columns receive no implicit query grants.
    pub fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(request, None, None, &[], &[], &[], &[], &[], &[])
    }

    /// Create a table and its primary key catalog/index records atomically.
    pub fn create_table_with_primary_key(
        &self,
        request: CreateTableRequest,
        primary_key_columns: &[String],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            Some(primary_key_columns.to_vec()),
            None,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    /// Create a table and its table-level UNIQUE constraints and indexes atomically.
    pub fn create_table_with_unique_constraints(
        &self,
        request: CreateTableRequest,
        unique_constraints: &[Vec<String>],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            None,
            None,
            unique_constraints,
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    /// Create a table with a primary key and table-level UNIQUE constraints atomically.
    pub fn create_table_with_constraints(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            primary_key_columns.map(<[String]>::to_vec),
            None,
            unique_constraints,
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    pub fn create_table_with_checks_and_constraints(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
        check_expressions: &[crate::authorization::Expr],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            primary_key_columns.map(<[String]>::to_vec),
            None,
            unique_constraints,
            &[],
            check_expressions,
            &[],
            &[],
            &[],
        )
    }

    pub fn create_table_with_foreign_keys(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
        check_expressions: &[crate::authorization::Expr],
        foreign_keys: &[CreateTableForeignKeyDefinition],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            primary_key_columns.map(<[String]>::to_vec),
            None,
            unique_constraints,
            &[],
            check_expressions,
            foreign_keys,
            &[],
            &[],
        )
    }

    pub fn create_table_with_named_checks_and_foreign_keys(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
        check_expressions: &[crate::authorization::Expr],
        check_constraint_names: &[Option<String>],
        foreign_keys: &[CreateTableForeignKeyDefinition],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_with_named_constraints_and_foreign_keys(
            request,
            primary_key_columns,
            None,
            unique_constraints,
            &[],
            check_expressions,
            check_constraint_names,
            foreign_keys,
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_table_with_named_constraints_and_foreign_keys(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        primary_key_name: Option<String>,
        unique_constraints: &[Vec<String>],
        unique_constraint_names: &[Option<String>],
        check_expressions: &[crate::authorization::Expr],
        check_constraint_names: &[Option<String>],
        foreign_keys: &[CreateTableForeignKeyDefinition],
        foreign_key_names: &[Option<String>],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            primary_key_columns.map(<[String]>::to_vec),
            primary_key_name,
            unique_constraints,
            unique_constraint_names,
            check_expressions,
            foreign_keys,
            foreign_key_names,
            check_constraint_names,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_table_inner(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<Vec<String>>,
        primary_key_name: Option<String>,
        unique_constraints: &[Vec<String>],
        unique_constraint_names: &[Option<String>],
        check_expressions: &[crate::authorization::Expr],
        foreign_keys: &[CreateTableForeignKeyDefinition],
        foreign_key_names: &[Option<String>],
        check_constraint_names: &[Option<String>],
    ) -> Result<CreatedTable, TableLifecycleError> {
        let CreateTableRequest {
            invocation_id,
            authenticated_actor_id,
            schema_id,
            name,
            columns,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(&name)
            || columns.is_empty()
            || columns.len() > crate::tuple::MAX_COLUMNS
            || check_expressions.len() > crate::tuple::MAX_COLUMNS
            || (!check_constraint_names.is_empty()
                && check_constraint_names.len() != check_expressions.len())
            || (primary_key_name.is_some() && primary_key_columns.is_none())
            || (!unique_constraint_names.is_empty()
                && unique_constraint_names.len() != unique_constraints.len())
            || (!foreign_key_names.is_empty() && foreign_key_names.len() != foreign_keys.len())
            || primary_key_name
                .as_ref()
                .is_some_and(|name| !is_normalized_principal_name(name))
            || unique_constraint_names
                .iter()
                .flatten()
                .chain(foreign_key_names.iter().flatten())
                .any(|name| !is_normalized_principal_name(name))
            || check_constraint_names
                .iter()
                .flatten()
                .any(|name| !is_normalized_principal_name(name))
            || check_constraint_names
                .iter()
                .flatten()
                .collect::<HashSet<_>>()
                .len()
                != check_constraint_names.iter().flatten().count()
            || foreign_keys.len() > crate::tuple::MAX_COLUMNS
            || check_expressions.iter().any(|expression| {
                !crate::expression::is_row_local_expression(expression)
                    || crate::expression::encode_expression(expression).is_err()
            })
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
        let mut column_names = HashSet::with_capacity(columns.len());
        for column in &columns {
            if !is_normalized_principal_name(&column.name)
                || !column_names.insert(column.name.as_str())
                || (column.generated_uuid_identity
                    && (column.scalar_type != ScalarType::Uuid
                        || column.nullable
                        || column.default.is_some()))
                || matches!(
                    column.default.as_ref(),
                    Some(TableColumnDefault::CurrentPrincipalId)
                        if column.scalar_type != ScalarType::Uuid
                )
            {
                return Err(TableLifecycleError::InvalidRequest);
            }
            let (nullable, value) = match &column.default {
                Some(TableColumnDefault::Literal(value)) => (column.nullable, value.clone()),
                Some(TableColumnDefault::CurrentPrincipalId) => (
                    column.nullable,
                    Some(crate::tuple::ScalarValue::Uuid([1; 16])),
                ),
                None => (true, None),
            };
            encode_tuple(
                &[ColumnSchema {
                    scalar_type: column.scalar_type,
                    nullable,
                }],
                1,
                &[value],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        }
        if let Some(key_columns) = &primary_key_columns
            && (key_columns.is_empty()
                || key_columns.len() > columns.len()
                || key_columns.iter().collect::<HashSet<_>>().len() != key_columns.len()
                || key_columns.iter().any(|name| {
                    !columns
                        .iter()
                        .any(|column| &column.name == name && !column.nullable)
                }))
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if unique_constraints.len() > crate::tuple::MAX_COLUMNS
            || unique_constraints.iter().any(|key_columns| {
                key_columns.is_empty()
                    || key_columns.len() > columns.len()
                    || key_columns.iter().collect::<HashSet<_>>().len() != key_columns.len()
                    || key_columns
                        .iter()
                        .any(|name| !columns.iter().any(|column| &column.name == name))
            })
            || unique_constraints
                .iter()
                .enumerate()
                .any(|(index, columns)| unique_constraints[..index].contains(columns))
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let mut supplied_constraint_names = HashSet::new();
        if primary_key_name
            .iter()
            .chain(unique_constraint_names.iter().flatten())
            .chain(check_constraint_names.iter().flatten())
            .chain(foreign_key_names.iter().flatten())
            .any(|name| !supplied_constraint_names.insert(name.as_str()))
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if foreign_keys.iter().any(|foreign_key| {
            foreign_key.source_columns.is_empty()
                || foreign_key.source_columns.len() > columns.len()
                || (foreign_key.target_constraint_id == [0; 16])
                    != foreign_key.target_columns.is_some()
                || foreign_key
                    .target_columns
                    .as_ref()
                    .is_some_and(|target_columns| {
                        target_columns.is_empty()
                            || target_columns.len() > columns.len()
                            || target_columns.iter().collect::<HashSet<_>>().len()
                                != target_columns.len()
                            || target_columns
                                .iter()
                                .any(|name| !columns.iter().any(|column| column.name == *name))
                    })
                || foreign_key
                    .source_columns
                    .iter()
                    .collect::<HashSet<_>>()
                    .len()
                    != foreign_key.source_columns.len()
                || foreign_key
                    .source_columns
                    .iter()
                    .any(|name| !columns.iter().any(|column| column.name == *name))
        }) {
            return Err(TableLifecycleError::InvalidRequest);
        }

        let type_definitions = load_builtin_type_definitions(self.database)?;
        let table_id = self.generate_table_object_id()?;
        let mut used_ids = HashSet::with_capacity(columns.len() + 1);
        used_ids.insert(table_id);
        let mut column_ids = Vec::with_capacity(columns.len());
        for _ in &columns {
            let mut column_id = self.generate_table_object_id()?;
            if !used_ids.insert(column_id) {
                column_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            column_ids.push(column_id);
        }
        let mut key_constraints = Vec::with_capacity(
            unique_constraints.len() + usize::from(primary_key_columns.is_some()),
        );
        if let Some(key_columns) = &primary_key_columns {
            let mut key_column_ids = Vec::with_capacity(key_columns.len());
            for name in key_columns {
                let ordinal = columns
                    .iter()
                    .position(|column| &column.name == name)
                    .ok_or(TableLifecycleError::InvalidRequest)?;
                key_column_ids.push(column_ids[ordinal]);
            }
            let mut constraint_id = self.generate_table_object_id()?;
            if !used_ids.insert(constraint_id) {
                constraint_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            let mut index_id = self.generate_table_object_id()?;
            if !used_ids.insert(index_id) {
                index_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            key_constraints.push((
                2u16,
                key_column_ids,
                primary_key_name.clone(),
                constraint_id,
                index_id,
            ));
        }
        for (index, columns_for_key) in unique_constraints.iter().enumerate() {
            let key_column_ids = columns_for_key
                .iter()
                .map(|name| {
                    let ordinal = columns
                        .iter()
                        .position(|column| &column.name == name)
                        .ok_or(TableLifecycleError::InvalidRequest)?;
                    Ok(column_ids[ordinal])
                })
                .collect::<Result<Vec<_>, TableLifecycleError>>()?;
            let mut constraint_id = self.generate_table_object_id()?;
            if !used_ids.insert(constraint_id) {
                constraint_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            let mut index_id = self.generate_table_object_id()?;
            if !used_ids.insert(index_id) {
                index_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            key_constraints.push((
                3u16,
                key_column_ids,
                unique_constraint_names.get(index).cloned().flatten(),
                constraint_id,
                index_id,
            ));
        }
        let mut check_constraints = Vec::with_capacity(check_expressions.len());
        for (index, expression) in check_expressions.iter().enumerate() {
            let mut constraint_id = self.generate_table_object_id()?;
            if !used_ids.insert(constraint_id) {
                constraint_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            let mut expression_id = self.generate_table_object_id()?;
            if !used_ids.insert(expression_id) {
                expression_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            check_constraints.push((
                expression.clone(),
                check_constraint_names.get(index).cloned().flatten(),
                constraint_id,
                expression_id,
            ));
        }
        let mut default_expression_ids = Vec::with_capacity(columns.len());
        for column in &columns {
            if column.default.is_some() {
                let mut expression_id = self.generate_table_object_id()?;
                if !used_ids.insert(expression_id) {
                    expression_id.zeroize();
                    return Err(TableLifecycleError::ObjectIdGenerationFailed);
                }
                default_expression_ids.push(Some(expression_id));
            } else {
                default_expression_ids.push(None);
            }
        }

        let default_count = default_expression_ids.iter().flatten().count();
        let mut writes = Vec::with_capacity(3 + columns.len() * 3 + default_count * 3);
        let table_object_key = id_catalog_key(2, table_id);
        writes.push(CatalogWrite {
            key: table_object_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &table_object_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: 3u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::Id,
                            payload: schema_id.to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: name.as_bytes().to_vec(),
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
        });

        let table_key = id_catalog_key(5, table_id);
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(5).ok_or(TableLifecycleError::Storage)?,
                    &table_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: schema_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                    ],
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });
        writes.push(new_dependency_write(table_id, schema_id, 1)?);

        let table_columns = columns
            .iter()
            .map(|column| ColumnSchema {
                scalar_type: column.scalar_type,
                nullable: column.nullable,
            })
            .collect::<Vec<_>>();
        for (ordinal, ((column_id, column), default_expression_id)) in column_ids
            .iter()
            .zip(&columns)
            .zip(&default_expression_ids)
            .enumerate()
        {
            let object_key = id_catalog_key(2, *column_id);
            writes.push(CatalogWrite {
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
            });
            let type_kind = scalar_type_kind(column.scalar_type);
            let (type_id, type_record) = type_definitions
                .get(&type_kind)
                .ok_or(TableLifecycleError::Storage)?;
            let type_parameters = scalar_type_parameters(column.scalar_type);
            decode_builtin_scalar_type(type_record, &type_parameters)
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
            if let (Some(expression_id), Some(default)) = (default_expression_id, &column.default) {
                let expression = match default {
                    TableColumnDefault::Literal(value) => {
                        crate::authorization::Expr::Literal(value.clone())
                    }
                    TableColumnDefault::CurrentPrincipalId => {
                        crate::authorization::Expr::CurrentPrincipalId
                    }
                };
                let catalog_expression = crate::expression::CatalogExpression {
                    expression_id: *expression_id,
                    purpose: crate::expression::ExpressionPurpose::Default,
                    result_type_id: *type_id,
                    result_parameters: type_parameters,
                    dependencies: Vec::new(),
                    expression: expression.clone(),
                };
                crate::expression::validate_expression_binding(
                    &catalog_expression,
                    &table_columns,
                    &column_ids,
                    *type_id,
                    column.scalar_type,
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
                writes.push(new_default_expression_object_write(
                    *expression_id,
                    ordinal,
                    authenticated_actor_id,
                )?);
                writes.push(new_default_expression_write(
                    *expression_id,
                    *type_id,
                    type_parameters,
                    expression,
                )?);
                writes.push(new_dependency_write(*column_id, *expression_id, 2)?);
            }
            let column_key = id_catalog_key(6, *column_id);
            let mut column_fields = vec![
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
            if let Some(expression_id) = default_expression_id {
                column_fields.push(CatalogField {
                    tag: 7,
                    wire_type: WireType::Id,
                    payload: expression_id.to_vec(),
                });
            }
            column_fields.extend([
                CatalogField {
                    tag: 8,
                    wire_type: WireType::U16,
                    payload: u16::from(column.generated_uuid_identity)
                        .to_le_bytes()
                        .to_vec(),
                },
                CatalogField {
                    tag: 9,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ]);
            writes.push(CatalogWrite {
                key: column_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(6).ok_or(TableLifecycleError::Storage)?,
                        &column_key,
                        &column_fields,
                    )
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            writes.push(new_dependency_write(*column_id, table_id, 1)?);
        }

        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        for (constraint_kind, key_column_ids, constraint_name, constraint_id, index_id) in
            &key_constraints
        {
            let constraint_key = id_catalog_key(8, *constraint_id);
            let constraint_fields = [
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U16,
                    payload: constraint_kind.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Ids,
                    payload: encode_ids(key_column_ids),
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
                CatalogField {
                    tag: 11,
                    wire_type: WireType::Id,
                    payload: index_id.to_vec(),
                },
            ];
            writes.push(CatalogWrite {
                key: constraint_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(8).ok_or(TableLifecycleError::Storage)?,
                        &constraint_key,
                        &constraint_fields,
                    )
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            let (constraint_kind_object, index_kind_object, constraint_prefix, index_prefix) =
                if *constraint_kind == 2 {
                    (6u16, 7u16, "primary_key_constraint", "primary_key_index")
                } else {
                    (6u16, 7u16, "unique_constraint", "unique_index")
                };
            for (object_id, kind, prefix, supplied_name) in [
                (
                    *constraint_id,
                    constraint_kind_object,
                    constraint_prefix,
                    constraint_name.as_deref(),
                ),
                (*index_id, index_kind_object, index_prefix, None),
            ] {
                let key = id_catalog_key(2, object_id);
                let name = supplied_name.map_or_else(
                    || {
                        format!(
                            "{prefix}_{}",
                            object_id
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )
                    },
                    str::to_owned,
                );
                let payload = encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: kind.to_le_bytes().to_vec(),
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
                    key,
                    expected_value: None,
                    payload: Some(payload),
                });
            }
            let index_key = id_catalog_key(9, *index_id);
            let index_fields = [
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::Ids,
                    payload: encode_ids(key_column_ids),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Bool,
                    payload: vec![1],
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Id,
                    payload: constraint_id.to_vec(),
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
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ];
            writes.push(CatalogWrite {
                key: index_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(9).ok_or(TableLifecycleError::Storage)?,
                        &index_key,
                        &index_fields,
                    )
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            writes.push(new_dependency_write(*constraint_id, *index_id, 4)?);
        }
        let boolean_type_id = type_definitions
            .get(&1)
            .map(|(type_id, _)| *type_id)
            .ok_or(TableLifecycleError::Storage)?;
        fn collect_check_dependencies(
            expression: &crate::authorization::Expr,
            column_ids: &[[u8; 16]],
            dependencies: &mut std::collections::BTreeSet<[u8; 16]>,
        ) -> Result<(), TableLifecycleError> {
            use crate::authorization::Expr;
            match expression {
                Expr::Field(ordinal) => {
                    let id = column_ids
                        .get(*ordinal)
                        .copied()
                        .ok_or(TableLifecycleError::InvalidRequest)?;
                    dependencies.insert(id);
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
                    collect_check_dependencies(left, column_ids, dependencies)?;
                    collect_check_dependencies(right, column_ids, dependencies)?;
                }
                Expr::And(items) | Expr::Or(items) => {
                    for item in items {
                        collect_check_dependencies(item, column_ids, dependencies)?;
                    }
                }
                Expr::Not(inner) | Expr::IsNull(inner) | Expr::Negate(inner) => {
                    collect_check_dependencies(inner, column_ids, dependencies)?;
                }
                Expr::Literal(_) => {}
                Expr::CurrentPrincipalId | Expr::HasRole(_) => {
                    return Err(TableLifecycleError::InvalidRequest);
                }
            }
            Ok(())
        }
        for (expression, constraint_name, constraint_id, expression_id) in &check_constraints {
            let mut dependency_ids = std::collections::BTreeSet::new();
            collect_check_dependencies(expression, &column_ids, &mut dependency_ids)?;
            let dependency_ids = dependency_ids.into_iter().collect::<Vec<_>>();
            let catalog_expression = crate::expression::CatalogExpression {
                expression_id: *expression_id,
                purpose: crate::expression::ExpressionPurpose::Check,
                result_type_id: boolean_type_id,
                result_parameters: [0; 8],
                dependencies: dependency_ids.clone(),
                expression: expression.clone(),
            };
            crate::expression::validate_expression_binding(
                &catalog_expression,
                &table_columns,
                &column_ids,
                boolean_type_id,
                ScalarType::Boolean,
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let expression_bytes = crate::expression::encode_expression(expression)
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let expression_key = id_catalog_key(7, *expression_id);
            let expression_payload = encode_envelope(
                &family_schema(7).ok_or(TableLifecycleError::Storage)?,
                &expression_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: 2u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U32,
                        payload: 1u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Id,
                        payload: boolean_type_id.to_vec(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Bytes,
                        payload: vec![0; 8],
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::Bytes,
                        payload: expression_bytes,
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: encode_ids(&dependency_ids),
                    },
                ],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let constraint_key = id_catalog_key(8, *constraint_id);
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
                        payload: 4u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Ids,
                        payload: 0u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Id,
                        payload: expression_id.to_vec(),
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
            for (object_id, kind, prefix, supplied_name) in [
                (
                    *constraint_id,
                    6u16,
                    "check_constraint",
                    constraint_name.as_deref(),
                ),
                (*expression_id, 5u16, "check_expression", None),
            ] {
                let object_key = id_catalog_key(2, object_id);
                let object_name = supplied_name.map_or_else(
                    || {
                        format!(
                            "{prefix}_{}",
                            object_id
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )
                    },
                    str::to_owned,
                );
                let object_payload = encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &object_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: kind.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: object_name.into_bytes(),
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
                    key: object_key,
                    expected_value: None,
                    payload: Some(object_payload),
                });
            }
            writes.extend([
                CatalogWrite {
                    key: constraint_key,
                    expected_value: None,
                    payload: Some(constraint_payload),
                },
                CatalogWrite {
                    key: expression_key,
                    expected_value: None,
                    payload: Some(expression_payload),
                },
                new_dependency_write(*constraint_id, *expression_id, 2)?,
            ]);
            for dependency_id in dependency_ids {
                writes.push(new_dependency_write(*expression_id, dependency_id, 2)?);
            }
        }

        if !foreign_keys.is_empty() {
            let read = self
                .database
                .begin_read()
                .map_err(|_| TableLifecycleError::Storage)?;
            let catalog = read
                .open_table(CATALOG)
                .map_err(|_| TableLifecycleError::Storage)?;
            let mut referenced_tables = HashMap::<[u8; 16], Vec<u8>>::new();
            let mut foreign_key_shapes = HashSet::new();
            for (index, definition) in foreign_keys.iter().enumerate() {
                let source_column_ids = definition
                    .source_columns
                    .iter()
                    .map(|name| {
                        let ordinal = columns
                            .iter()
                            .position(|column| column.name == *name)
                            .ok_or(TableLifecycleError::InvalidRequest)?;
                        Ok(column_ids[ordinal])
                    })
                    .collect::<Result<Vec<_>, TableLifecycleError>>()?;
                let local_target = definition
                    .target_columns
                    .as_ref()
                    .map(|names| {
                        names
                            .iter()
                            .map(|name| {
                                columns
                                    .iter()
                                    .position(|column| &column.name == name)
                                    .map(|ordinal| column_ids[ordinal])
                                    .ok_or(TableLifecycleError::InvalidRequest)
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?;
                let target_constraint_id = if let Some(target_column_ids) = &local_target {
                    key_constraints
                        .iter()
                        .find(|(kind, key_columns, _, _, _)| {
                            matches!(kind, 2 | 3) && key_columns == target_column_ids
                        })
                        .map(|(_, _, _, constraint_id, _)| *constraint_id)
                        .ok_or(TableLifecycleError::InvalidRequest)?
                } else {
                    definition.target_constraint_id
                };
                if !foreign_key_shapes.insert((source_column_ids.clone(), target_constraint_id)) {
                    return Err(TableLifecycleError::InvalidRequest);
                }
                let local_target = local_target.is_some();
                let (target_table_id, target_column_ids) = if let Some(target_column_ids) =
                    definition.target_columns.as_ref()
                {
                    let target_column_ids = target_column_ids
                        .iter()
                        .map(|name| {
                            columns
                                .iter()
                                .position(|column| &column.name == name)
                                .map(|ordinal| column_ids[ordinal])
                                .ok_or(TableLifecycleError::InvalidRequest)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    (table_id, target_column_ids)
                } else {
                    let target_constraint_key = id_catalog_key(8, target_constraint_id);
                    let target_constraint_bytes = catalog
                        .get(target_constraint_key.as_slice())
                        .map_err(|_| TableLifecycleError::Storage)?
                        .map(|value| value.value().to_vec())
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                    let target_constraint =
                        read_catalog_record(&target_constraint_key, &target_constraint_bytes, 8)?;
                    if !matches!(
                        read_catalog_u16(&target_constraint_key, &target_constraint_bytes, 8, 2,)?,
                        2 | 3
                    ) {
                        return Err(TableLifecycleError::InvalidRequest);
                    }
                    let target_table_id =
                        read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)?;
                    let target_column_ids = target_constraint
                        .fields
                        .iter()
                        .find(|field| field.tag == 3 && field.wire_type == WireType::Ids)
                        .map(|field| decode_catalog_id_list(&field.payload))
                        .transpose()?
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                    (target_table_id, target_column_ids)
                };
                if target_column_ids.is_empty()
                    || target_column_ids.len() != source_column_ids.len()
                    || target_column_ids.contains(&[0; 16])
                    || target_column_ids.iter().collect::<HashSet<_>>().len()
                        != target_column_ids.len()
                {
                    return Err(TableLifecycleError::InvalidRequest);
                }
                for (source_name, target_column_id) in
                    definition.source_columns.iter().zip(&target_column_ids)
                {
                    let source_ordinal = columns
                        .iter()
                        .position(|column| column.name == *source_name)
                        .ok_or(TableLifecycleError::InvalidRequest)?;
                    let target_scalar_type = if local_target {
                        let target_ordinal = column_ids
                            .iter()
                            .position(|column_id| column_id == target_column_id);
                        let target_ordinal = target_ordinal
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                        columns[target_ordinal].scalar_type
                    } else {
                        let target_column_key = id_catalog_key(6, *target_column_id);
                        let target_column_bytes = catalog
                            .get(target_column_key.as_slice())
                            .map_err(|_| TableLifecycleError::Storage)?
                            .map(|value| value.value().to_vec())
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                        if read_catalog_id(&target_column_key, &target_column_bytes, 6, 1)?
                            != target_table_id
                        {
                            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                        }
                        let target_type_id =
                            read_catalog_id(&target_column_key, &target_column_bytes, 6, 4)?;
                        let target_type_key = id_catalog_key(4, target_type_id);
                        let target_type_bytes = catalog
                            .get(target_type_key.as_slice())
                            .map_err(|_| TableLifecycleError::Storage)?
                            .map(|value| value.value().to_vec())
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                        let target_parameters =
                            read_catalog_field(&target_column_key, &target_column_bytes, 6, 5)?;
                        let target_type_record =
                            read_catalog_record(&target_type_key, &target_type_bytes, 4)?;
                        decode_builtin_scalar_type(&target_type_record, &target_parameters)
                            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                    };
                    if columns[source_ordinal].scalar_type != target_scalar_type {
                        return Err(TableLifecycleError::InvalidRequest);
                    }
                }

                let mut foreign_key_id = self.generate_table_object_id()?;
                if !used_ids.insert(foreign_key_id) {
                    foreign_key_id.zeroize();
                    return Err(TableLifecycleError::ObjectIdGenerationFailed);
                }
                let encode_id_list = |ids: &[[u8; 16]]| {
                    let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
                    payload.extend(ids.iter().flatten());
                    payload
                };
                let constraint_key = id_catalog_key(8, foreign_key_id);
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
                            payload: 5u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Ids,
                            payload: encode_id_list(&source_column_ids),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::Id,
                            payload: target_constraint_id.to_vec(),
                        },
                        CatalogField {
                            tag: 6,
                            wire_type: WireType::Ids,
                            payload: encode_id_list(&target_column_ids),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 8,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 9,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 10,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                    ],
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
                let object_key = id_catalog_key(2, foreign_key_id);
                let object_name = foreign_key_names
                    .get(index)
                    .cloned()
                    .flatten()
                    .unwrap_or_else(|| {
                        format!(
                            "foreign_key_{}",
                            foreign_key_id
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )
                    });
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
                            payload: object_name.into_bytes(),
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
                writes.extend([
                    CatalogWrite {
                        key: constraint_key,
                        expected_value: None,
                        payload: Some(constraint_payload),
                    },
                    CatalogWrite {
                        key: object_key,
                        expected_value: None,
                        payload: Some(object_payload),
                    },
                    new_dependency_write(foreign_key_id, target_constraint_id, 3)?,
                ]);
                if !local_target
                    && let std::collections::hash_map::Entry::Vacant(entry) =
                        referenced_tables.entry(target_table_id)
                {
                    let table_key = id_catalog_key(5, target_table_id);
                    let table_bytes = catalog
                        .get(table_key.as_slice())
                        .map_err(|_| TableLifecycleError::Storage)?
                        .map(|value| value.value().to_vec())
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                    entry.insert(table_bytes);
                }
            }
            for (target_table_id, table_bytes) in referenced_tables {
                let table_key = id_catalog_key(5, target_table_id);
                let epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
                let next_epoch = epoch
                    .checked_add(1)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key: table_key.clone(),
                    expected_value: Some(table_bytes.clone()),
                    payload: Some(rewrite_catalog_u64_field(
                        &table_key,
                        &table_bytes,
                        5,
                        3,
                        next_epoch,
                    )?),
                });
            }
        }
        audit_event.target_ids = vec![table_id];
        let commit_sequence = commit_authorized_table_create_ddl_with_fences(
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
        Ok(CreatedTable {
            table_id,
            column_ids,
            commit_sequence,
        })
    }

    fn generate_table_object_id(&self) -> Result<[u8; 16], TableLifecycleError> {
        for _ in 0..8 {
            let mut candidate = [0; 16];
            if getrandom::fill(&mut candidate).is_err() {
                candidate.zeroize();
                return Err(TableLifecycleError::RandomnessUnavailable);
            }
            if candidate == [0; 16] {
                candidate.zeroize();
                continue;
            }
            let read = self
                .database
                .begin_read()
                .map_err(|_| TableLifecycleError::Storage)?;
            let catalog = read
                .open_table(CATALOG)
                .map_err(|_| TableLifecycleError::Storage)?;
            if catalog
                .get(id_catalog_key(2, candidate).as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .is_none()
                && catalog
                    .get(id_catalog_key(17, candidate).as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .is_none()
            {
                return Ok(candidate);
            }
            candidate.zeroize();
        }
        Err(TableLifecycleError::ObjectIdGenerationFailed)
    }

    /// Create a LOGIN user through the authorized identity DDL path.
    pub fn create_user(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<CreatedUser, IdentityLifecycleError> {
        let (user_id, commit_sequence) = self.create_identity(
            invocation_id,
            authenticated_actor_id,
            name,
            NewPrincipalKind::User,
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
        let (user_id, commit_sequence) = self.create_identity(
            invocation_id,
            authenticated_actor_id,
            name,
            NewPrincipalKind::UserNoLogin,
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

    fn create_identity(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        kind: NewPrincipalKind,
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
        let principal_payload = encode_envelope(
            &family_schema(10).ok_or(IdentityLifecycleError::Storage)?,
            &principal_key,
            &[
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
            ],
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
        let writes = [
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

    fn generate_identity_id(&self) -> Result<[u8; 16], IdentityLifecycleError> {
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

    /// Issue one random bearer token for a LOGIN user through the authorized
    /// identity DDL path. Unauthorized callers, unavailable targets, and a
    /// target that becomes unavailable before commit share one result. The
    /// secret is returned once and only its verifier is written to the catalog
    /// and audit event.
    pub fn issue_token(
        &self,
        request: TokenIssueRequest<'_>,
    ) -> Result<IssuedToken, TokenLifecycleError> {
        let TokenIssueRequest {
            invocation_id,
            authenticated_actor_id,
            user_id,
            label,
            expires_at_us,
            retention_deadline_us,
            mut audit_event,
        } = request;
        let Some(issued_at_us) = now_unix_micros() else {
            return Err(TokenLifecycleError::InvalidRequest);
        };
        if user_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || expires_at_us <= issued_at_us
            || label.len() > MAX_TOKEN_LABEL_BYTES
        {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.action != AuditAction::TokenLifecycle
            || audit_event.result != AuditResult::Succeeded
            || !audit_event.target_ids.contains(&user_id)
        {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        self.token_requests.consume(authenticated_actor_id)?;

        let read = self
            .database
            .begin_read()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let snapshot = RowSnapshot::new(read);
        let roles = token_actor_roles(&snapshot, authenticated_actor_id)?;
        let mut database_admin_role = [0x80; 16];
        database_admin_role[15] = 3;
        let metadata_bytes = snapshot
            .get_engine_metadata_value()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
            .ok_or_else(|| TokenLifecycleError::Storage("engine metadata is missing".into()))?;
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
            return Err(TokenLifecycleError::Storage(
                "database is not ready for token issuance".into(),
            ));
        }
        let mut authority_subjects = roles.clone();
        authority_subjects.insert(authenticated_actor_id);
        if !roles.contains(&database_admin_role)
            && !token_inventory_has_manage_users(
                &snapshot,
                metadata.database_id,
                &authority_subjects,
            )?
        {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let target = snapshot
            .get_catalog_record(&principal_catalog_key(user_id))
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
            .ok_or(TokenLifecycleError::TokenUnavailable)?;
        let target_login = token_catalog_field(&target, 3)?;
        if token_catalog_u16(&target, 1)? != 1
            || !(target_login == [0] || target_login == [1])
            || token_catalog_u64(&target, 4)? == 0
        {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        drop(snapshot);

        let mut generated = None;
        for _ in 0..8 {
            let mut material = [0u8; 16 + TOKEN_SECRET_BYTES];
            if getrandom::fill(&mut material).is_err() {
                material.zeroize();
                return Err(TokenLifecycleError::RandomnessUnavailable);
            }
            let token_id: [u8; 16] = material[..16].try_into().expect("fixed token ID length");
            let mut secret: [u8; TOKEN_SECRET_BYTES] = material[16..]
                .try_into()
                .expect("fixed token secret length");
            material.zeroize();
            if token_id == [0; 16] {
                secret.zeroize();
                continue;
            }
            match credential_token_exists(self.database, token_id) {
                Ok(false) => {
                    generated = Some((token_id, secret));
                    secret.zeroize();
                    break;
                }
                Ok(true) => secret.zeroize(),
                Err(error) => {
                    secret.zeroize();
                    return Err(error);
                }
            }
        }
        let Some((token_id, mut secret)) = generated else {
            return Err(TokenLifecycleError::TokenIdGenerationFailed);
        };
        let payload = match encode_new_verifier_record(
            token_id,
            user_id,
            &secret,
            issued_at_us,
            expires_at_us,
            label,
        ) {
            Ok(payload) => payload,
            Err(_) => {
                secret.zeroize();
                return Err(TokenLifecycleError::EncodingFailed);
            }
        };
        let mut target_ids = audit_event.target_ids;
        if target_ids.len() >= 256 {
            secret.zeroize();
            return Err(TokenLifecycleError::InvalidAudit);
        }
        target_ids.push(token_id);
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let write = CatalogWrite {
            key: credential_catalog_key(token_id),
            expected_value: None,
            payload: Some(payload),
        };
        let sequence = match self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[write],
            &[audit_event],
        ) {
            Ok(sequence) => sequence,
            Err(error) => {
                secret.zeroize();
                return Err(match error {
                    CommitError::DdlAuthorizationDenied
                    | CommitError::Conflict
                    | CommitError::InvalidCatalog => TokenLifecycleError::TokenUnavailable,
                    error => TokenLifecycleError::Commit(error),
                });
            }
        };
        let issued = IssuedToken {
            token_id,
            commit_sequence: sequence,
            secret: Some(secret),
        };
        secret.zeroize();
        Ok(issued)
    }

    /// Revoke one token through the authorized identity DDL path. Unknown,
    /// already revoked, and unauthorized token requests share
    /// `TokenUnavailable` so this method cannot be used to enumerate tokens.
    pub fn revoke_token(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        token_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TokenLifecycleError> {
        if authenticated_actor_id == [0; 16] || token_id == [0; 16] {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.action != AuditAction::TokenLifecycle
            || audit_event.result != AuditResult::Succeeded
            || !audit_event.target_ids.contains(&token_id)
        {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        self.token_requests.consume(authenticated_actor_id)?;
        let key = credential_catalog_key(token_id);
        let read = self
            .database
            .begin_read()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let table = read
            .open_table(CATALOG)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let current_bytes = table
            .get(key.as_slice())
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(TokenLifecycleError::TokenUnavailable)?;
        let current_record = decode_registered_envelope(&current_bytes)
            .map_err(|_| TokenLifecycleError::TokenUnavailable)?;
        if current_record.logical_key != key {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let current = decode_verifier_record(&current_record)
            .map_err(|_| TokenLifecycleError::TokenUnavailable)?;
        if current.revoked {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let Some(revoked_at_us) = now_unix_micros() else {
            return Err(TokenLifecycleError::InvalidRequest);
        };
        let payload = encode_revoked_verifier_record(&current_record, revoked_at_us)
            .map_err(|_| TokenLifecycleError::EncodingFailed)?;
        drop(table);
        drop(read);
        let mut target_ids = audit_event.target_ids;
        if target_ids.len() >= 256 {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        target_ids.push(current.user_id);
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let write = CatalogWrite {
            key,
            expected_value: Some(current_bytes),
            payload: Some(payload),
        };
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[write],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TokenLifecycleError::TokenUnavailable,
            error => TokenLifecycleError::Commit(error),
        })
    }

    /// Atomically revoke an active token and issue its replacement for the
    /// same user. The replacement secret is returned once; on any failed
    /// commit the old token remains active and no replacement is published.
    /// Unknown, unavailable, unauthorized, and conflicted rotations share the
    /// `TokenUnavailable` result to avoid token-ID probing.
    pub fn rotate_token(
        &self,
        request: TokenRotationRequest<'_>,
    ) -> Result<IssuedToken, TokenLifecycleError> {
        let TokenRotationRequest {
            invocation_id,
            authenticated_actor_id,
            old_token_id,
            label,
            expires_at_us,
            retention_deadline_us,
            mut audit_event,
        } = request;
        let Some(issued_at_us) = now_unix_micros() else {
            return Err(TokenLifecycleError::InvalidRequest);
        };
        if authenticated_actor_id == [0; 16]
            || old_token_id == [0; 16]
            || expires_at_us <= issued_at_us
            || label.is_empty()
            || label.len() > MAX_TOKEN_LABEL_BYTES
        {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.action != AuditAction::TokenLifecycle
            || audit_event.result != AuditResult::Succeeded
            || !audit_event.target_ids.contains(&old_token_id)
        {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        self.token_requests.consume(authenticated_actor_id)?;
        let old_key = credential_catalog_key(old_token_id);
        let read = self
            .database
            .begin_read()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let table = read
            .open_table(CATALOG)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let old_bytes = table
            .get(old_key.as_slice())
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(TokenLifecycleError::TokenUnavailable)?;
        let old_envelope = decode_registered_envelope(&old_bytes)
            .map_err(|_| TokenLifecycleError::TokenUnavailable)?;
        if old_envelope.logical_key != old_key {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let old_verifier = decode_verifier_record(&old_envelope)
            .map_err(|_| TokenLifecycleError::TokenUnavailable)?;
        if old_verifier.revoked || old_verifier.expires_at_us <= issued_at_us {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let user_id = old_verifier.user_id;
        let revoked_payload = encode_revoked_verifier_record(&old_envelope, issued_at_us)
            .map_err(|_| TokenLifecycleError::EncodingFailed)?;
        drop(table);
        drop(read);

        let mut generated = None;
        for _ in 0..8 {
            let mut material = [0u8; 16 + TOKEN_SECRET_BYTES];
            if getrandom::fill(&mut material).is_err() {
                material.zeroize();
                return Err(TokenLifecycleError::RandomnessUnavailable);
            }
            let token_id: [u8; 16] = material[..16].try_into().expect("fixed token ID length");
            let mut secret: [u8; TOKEN_SECRET_BYTES] = material[16..]
                .try_into()
                .expect("fixed token secret length");
            material.zeroize();
            if token_id == [0; 16] || token_id == old_token_id {
                secret.zeroize();
                continue;
            }
            match credential_token_exists(self.database, token_id) {
                Ok(false) => {
                    generated = Some((token_id, secret));
                    secret.zeroize();
                    break;
                }
                Ok(true) => secret.zeroize(),
                Err(error) => {
                    secret.zeroize();
                    return Err(error);
                }
            }
        }
        let Some((new_token_id, mut secret)) = generated else {
            return Err(TokenLifecycleError::TokenIdGenerationFailed);
        };
        let new_payload = match encode_new_verifier_record(
            new_token_id,
            user_id,
            &secret,
            issued_at_us,
            expires_at_us,
            label,
        ) {
            Ok(payload) => payload,
            Err(_) => {
                secret.zeroize();
                return Err(TokenLifecycleError::EncodingFailed);
            }
        };
        let mut target_ids = audit_event.target_ids;
        if target_ids.len() >= 256 {
            secret.zeroize();
            return Err(TokenLifecycleError::InvalidAudit);
        }
        target_ids.extend([user_id, new_token_id]);
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let writes = [
            CatalogWrite {
                key: old_key,
                expected_value: Some(old_bytes),
                payload: Some(revoked_payload),
            },
            CatalogWrite {
                key: credential_catalog_key(new_token_id),
                expected_value: None,
                payload: Some(new_payload),
            },
        ];
        let sequence = match self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        ) {
            Ok(sequence) => sequence,
            Err(error) => {
                secret.zeroize();
                return Err(match error {
                    CommitError::DdlAuthorizationDenied | CommitError::Conflict => {
                        TokenLifecycleError::TokenUnavailable
                    }
                    error => TokenLifecycleError::Commit(error),
                });
            }
        };
        let issued = IssuedToken {
            token_id: new_token_id,
            commit_sequence: sequence,
            secret: Some(secret),
        };
        secret.zeroize();
        Ok(issued)
    }

    /// List token metadata for the authenticated user, or for another user
    /// when the actor has database-admin membership or MANAGE_USERS. The read
    /// uses one catalog snapshot and never returns verifier material.
    pub fn list_tokens(
        &self,
        authenticated_actor_id: [u8; 16],
        user_id: [u8; 16],
    ) -> Result<Vec<TokenMetadata>, TokenLifecycleError> {
        if authenticated_actor_id == [0; 16] || user_id == [0; 16] {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        self.token_requests.consume(authenticated_actor_id)?;
        let read = self
            .database
            .begin_read()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let snapshot = RowSnapshot::new(read);
        let can_list = token_inventory_authority(&snapshot, authenticated_actor_id, user_id)?;
        if !can_list {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let records = snapshot
            .scan_catalog_family(16, MAX_CATALOG_FAMILY_SCAN)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let mut tokens = Vec::new();
        for record in records {
            let verifier = decode_verifier_record(&record)
                .map_err(|_| TokenLifecycleError::Storage("invalid token catalog record".into()))?;
            if verifier.user_id != user_id {
                continue;
            }
            let token_id = record
                .logical_key
                .get(3..19)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| TokenLifecycleError::Storage("invalid token catalog key".into()))?;
            let label = String::from_utf8(verifier.label)
                .map_err(|_| TokenLifecycleError::Storage("invalid token label".into()))?;
            tokens.push(TokenMetadata {
                token_id,
                label,
                issued_at_us: verifier.issued_at_us,
                expires_at_us: verifier.expires_at_us,
                revoked: verifier.revoked,
                revoked_at_us: verifier.revoked_at_us,
            });
        }
        Ok(tokens)
    }

    /// Return at most 64 token records after an exclusive token ID cursor.
    /// The scan and authorization semantics match `list_tokens`.
    pub fn list_tokens_page(
        &self,
        authenticated_actor_id: [u8; 16],
        user_id: [u8; 16],
        after_token_id: Option<[u8; 16]>,
    ) -> Result<TokenMetadataPage, TokenLifecycleError> {
        const PAGE_SIZE: usize = 64;
        let mut tokens = self.list_tokens(authenticated_actor_id, user_id)?;
        if let Some(cursor) = after_token_id {
            tokens.retain(|token| token.token_id > cursor);
        }
        let has_more = tokens.len() > PAGE_SIZE;
        tokens.truncate(PAGE_SIZE);
        let next_after_token_id = has_more
            .then(|| tokens.last().map(|token| token.token_id))
            .flatten();
        Ok(TokenMetadataPage {
            tokens,
            next_after_token_id,
        })
    }

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

    /// Authorize and commit row/cell policy DDL for an actor authenticated by
    /// the trusted administrative channel. The actor must own each affected
    /// table or hold inherited `MANAGE_POLICIES` on it.
    pub fn commit_authorized_policy_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_policy_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit foreign-key additions/removals on existing
    /// tables. Creation requires source-table `ALTER` and target-table or
    /// target-column `REFERENCES`; removal requires source-table `ALTER`.
    pub fn commit_authorized_foreign_key_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        let mut writes = catalog_writes.to_vec();
        let retired_ids = catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&2u16.to_be_bytes()) && write.payload.is_none())
            .filter_map(|write| write.key.get(3..19))
            .filter_map(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .collect::<HashSet<_>>();
        for retired_id in retired_ids {
            let key = retired_object_id_catalog_key(retired_id);
            if writes.iter().any(|write| write.key == key) {
                continue;
            }
            let payload = encode_envelope(
                &family_schema(17).ok_or(CommitError::InvalidCatalog)?,
                &key,
                &[],
            )
            .map_err(|_| CommitError::InvalidCatalog)?;
            writes.push(CatalogWrite {
                key,
                expected_value: None,
                payload: Some(payload),
            });
        }
        commit_authorized_foreign_key_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            &writes,
            audit_events,
        )
    }

    /// Create an FK on existing tables. The final serialized DDL commit
    /// validates source/target column compatibility, REFERENCES and ALTER
    /// authority, and existing rows while fencing both tables.
    pub fn create_foreign_key(
        &self,
        request: CreateForeignKeyRequest,
    ) -> Result<CreatedForeignKey, TableLifecycleError> {
        self.create_foreign_key_named(request, None)
    }

    pub fn create_named_foreign_key(
        &self,
        request: CreateForeignKeyRequest,
        name: String,
    ) -> Result<CreatedForeignKey, TableLifecycleError> {
        if !is_normalized_principal_name(&name) {
            return Err(TableLifecycleError::InvalidRequest);
        }
        self.create_foreign_key_named(request, Some(name))
    }

    fn create_foreign_key_named(
        &self,
        request: CreateForeignKeyRequest,
        name: Option<String>,
    ) -> Result<CreatedForeignKey, TableLifecycleError> {
        let CreateForeignKeyRequest {
            invocation_id,
            authenticated_actor_id: authenticated_principal_id,
            source_table_id,
            source_column_ids,
            target_constraint_id,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_principal_id == [0; 16]
            || source_table_id == [0; 16]
            || target_constraint_id == [0; 16]
            || source_column_ids.is_empty()
            || source_column_ids.len() > 4096
            || source_column_ids.contains(&[0; 16])
            || source_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != source_column_ids.len()
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_principal_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }

        let foreign_key_id = self.generate_table_object_id()?;
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let source_table_key = id_catalog_key(5, source_table_id);
        let source_table_bytes = catalog
            .get(source_table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&source_table_key, &source_table_bytes, 5)?;

        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = catalog
            .get(target_constraint_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let target_constraint =
            read_catalog_record(&target_constraint_key, &target_constraint_bytes, 8)?;
        if !matches!(
            read_catalog_u16(&target_constraint_key, &target_constraint_bytes, 8, 2)?,
            2 | 3
        ) {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let target_table_id =
            read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)?;
        let target_column_ids = target_constraint
            .fields
            .iter()
            .find(|field| field.tag == 3 && field.wire_type == WireType::Ids)
            .map(|field| decode_catalog_id_list(&field.payload))
            .transpose()?
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if target_column_ids.len() != source_column_ids.len() {
            return Err(TableLifecycleError::InvalidRequest);
        }

        let mut writes = Vec::with_capacity(7);
        let constraint_key = id_catalog_key(8, foreign_key_id);
        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let constraint_fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: source_table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 5u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: encode_ids(&source_column_ids),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: target_constraint_id.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: encode_ids(&target_column_ids),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 10,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ];
        let constraint_payload = encode_envelope(
            &family_schema(8).ok_or(TableLifecycleError::Storage)?,
            &constraint_key,
            &constraint_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        writes.push(CatalogWrite {
            key: constraint_key,
            expected_value: None,
            payload: Some(constraint_payload),
        });

        let source_object_key = id_catalog_key(2, source_table_id);
        let source_object_bytes = catalog
            .get(source_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&source_object_key, &source_object_bytes, 2, 4)?;
        let object_key = id_catalog_key(2, foreign_key_id);
        let name = name.unwrap_or_else(|| {
            format!(
                "foreign_key_{}",
                foreign_key_id
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )
        });
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
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        writes.push(CatalogWrite {
            key: object_key,
            expected_value: None,
            payload: Some(object_payload),
        });
        writes.push(new_dependency_write(
            foreign_key_id,
            target_constraint_id,
            3,
        )?);

        let mut table_ids = vec![source_table_id, target_table_id];
        table_ids.sort_unstable();
        table_ids.dedup();
        for table_id in table_ids {
            let table_key = id_catalog_key(5, table_id);
            let table_bytes = catalog
                .get(table_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
            let next_epoch = epoch
                .checked_add(1)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(
                    &table_key,
                    &table_bytes,
                    5,
                    3,
                    next_epoch,
                )?),
            });
        }
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![foreign_key_id];
        let commit_sequence = self
            .commit_authorized_foreign_key_ddl(
                invocation_id,
                authenticated_principal_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(|error| match error {
                CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
                error => TableLifecycleError::Commit(error),
            })?;
        Ok(CreatedForeignKey {
            foreign_key_id,
            commit_sequence,
        })
    }

    /// Add a deterministic row-local CHECK expression and validate it against
    /// stored rows while the table DDL fence is held.
    pub fn create_check_constraint(
        &self,
        request: CreateCheckConstraintRequest,
    ) -> Result<CreatedCheckConstraint, TableLifecycleError> {
        let CreateCheckConstraintRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            name,
            expression,
            dependency_column_ids,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
            || name
                .as_deref()
                .is_some_and(|value| !is_normalized_principal_name(value))
            || dependency_column_ids.len() > crate::tuple::MAX_COLUMNS
            || dependency_column_ids.contains(&[0; 16])
            || dependency_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != dependency_column_ids.len()
            || !crate::expression::is_row_local_expression(&expression)
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
        let expression_bytes = crate::expression::encode_expression(&expression)
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let boolean_type_id = load_builtin_type_definitions(self.database)?
            .get(&1)
            .map(|(type_id, _)| *type_id)
            .ok_or(TableLifecycleError::Storage)?;
        let constraint_id = self.generate_table_object_id()?;
        let mut expression_id = self.generate_table_object_id()?;
        for _ in 0..8 {
            if expression_id != constraint_id {
                break;
            }
            expression_id = self.generate_table_object_id()?;
        }
        if expression_id == constraint_id {
            return Err(TableLifecycleError::ObjectIdGenerationFailed);
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
        read_catalog_record(&table_key, &table_bytes, 5)?;
        let table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_table_epoch = table_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_object_key = id_catalog_key(2, table_id);
        let table_object = catalog
            .get(table_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&table_object_key, &table_object, 2, 4)?;
        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let expression_key = id_catalog_key(7, expression_id);
        let expression_payload = encode_envelope(
            &family_schema(7).ok_or(TableLifecycleError::Storage)?,
            &expression_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 2u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U32,
                    payload: 1u32.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Id,
                    payload: boolean_type_id.to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Bytes,
                    payload: vec![0; 8],
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Bytes,
                    payload: expression_bytes,
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Ids,
                    payload: encode_ids(&dependency_column_ids),
                },
            ],
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let constraint_key = id_catalog_key(8, constraint_id);
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
                    payload: 4u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Ids,
                    payload: 0u32.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: expression_id.to_vec(),
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
        let object_write = |object_id: [u8; 16], kind: u16, name: String| {
            let key = id_catalog_key(2, object_id);
            let payload = encode_envelope(
                &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                &key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: kind.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Text,
                        payload: name.into_bytes(),
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
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            Ok::<_, TableLifecycleError>(CatalogWrite {
                key,
                expected_value: None,
                payload: Some(payload),
            })
        };
        let mut writes = vec![
            CatalogWrite {
                key: constraint_key,
                expected_value: None,
                payload: Some(constraint_payload),
            },
            object_write(
                constraint_id,
                6,
                name.clone().unwrap_or_else(|| {
                    format!(
                        "check_constraint_{}",
                        constraint_id
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                    )
                }),
            )?,
            CatalogWrite {
                key: expression_key,
                expected_value: None,
                payload: Some(expression_payload),
            },
            object_write(
                expression_id,
                5,
                format!(
                    "check_expression_{}",
                    expression_id
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                ),
            )?,
            new_dependency_write(constraint_id, expression_id, 2)?,
        ];
        for column_id in &dependency_column_ids {
            writes.push(new_dependency_write(expression_id, *column_id, 2)?);
        }
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_bytes,
                5,
                3,
                next_table_epoch,
            )?),
        });
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![constraint_id, expression_id];
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
        Ok(CreatedCheckConstraint {
            constraint_id,
            expression_id,
            commit_sequence,
        })
    }

    fn collect_policy_drop_writes(
        catalog: &impl ReadableTable<&'static [u8], &'static [u8]>,
        policy_id: [u8; 16],
    ) -> Result<PolicyDropWriteSet, TableLifecycleError> {
        let policy_key = id_catalog_key(13, policy_id);
        let policy_bytes = catalog
            .get(policy_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let policy_record = read_catalog_record(&policy_key, &policy_bytes, 13)?;
        let table_id = read_catalog_id(&policy_key, &policy_bytes, 13, 1)?;
        let kind = read_catalog_u16(&policy_key, &policy_bytes, 13, 2)?;
        if !matches!(kind, 1 | 2) {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }

        let mut writes = Vec::<CatalogWrite>::new();
        let mut expression_ids = Vec::new();
        for (tag, expected_purpose) in [
            (5u16, crate::expression::ExpressionPurpose::PolicyUsing),
            (6u16, crate::expression::ExpressionPurpose::PolicyCheck),
        ] {
            let Some(field) = policy_record.fields.iter().find(|field| field.tag == tag) else {
                continue;
            };
            let expression_id: [u8; 16] = field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let expression_key = id_catalog_key(7, expression_id);
            let expression_bytes = catalog
                .get(expression_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let expression = crate::expression::decode_catalog_expression(&expression_bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if expression.purpose != expected_purpose {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            writes.push(CatalogWrite {
                key: expression_key,
                expected_value: Some(expression_bytes),
                payload: None,
            });
            let expression_object_key = id_catalog_key(2, expression_id);
            let expression_object = catalog
                .get(expression_object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: expression_object_key,
                expected_value: Some(expression_object),
                payload: None,
            });
            let policy_expression_edge = dependency_catalog_key(policy_id, expression_id, 2);
            let policy_expression_value = catalog
                .get(policy_expression_edge.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: policy_expression_edge,
                expected_value: Some(policy_expression_value),
                payload: None,
            });
            for dependency_id in expression.dependencies {
                let key = dependency_catalog_key(expression_id, dependency_id, 2);
                let value = catalog
                    .get(key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key,
                    expected_value: Some(value),
                    payload: None,
                });
            }
            expression_ids.push(expression_id);
        }

        let target_prefix = 14u16.to_be_bytes();
        let mut scanned_targets = 0usize;
        let mut has_recipient = false;
        for entry in catalog
            .range(target_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&target_prefix) {
                break;
            }
            scanned_targets += 1;
            if scanned_targets > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key.len() != 39 {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key[3..19] != policy_id {
                continue;
            }
            let target = decode_policy_target(value.value())
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if target.policy_id != policy_id {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if matches!(target.kind, 1 | 2) {
                has_recipient = true;
                let edge_key = dependency_catalog_key(policy_id, target.target_id, 5);
                let edge_value = catalog
                    .get(edge_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key: edge_key,
                    expected_value: Some(edge_value),
                    payload: None,
                });
            }
            writes.push(CatalogWrite {
                key,
                expected_value: Some(value.value().to_vec()),
                payload: None,
            });
        }
        if !has_recipient {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        for (key, value) in [
            (policy_key, policy_bytes),
            (
                id_catalog_key(2, policy_id),
                catalog
                    .get(id_catalog_key(2, policy_id).as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
            ),
        ] {
            writes.push(CatalogWrite {
                key,
                expected_value: Some(value),
                payload: None,
            });
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
        Ok(PolicyDropWriteSet {
            table_id,
            kind,
            writes,
            expression_ids,
        })
    }

    /// Create a row or cell policy and its expression/recipient records as one
    /// authorized table DDL commit.
    pub fn create_policy(
        &self,
        request: CreatePolicyRequest,
    ) -> Result<CreatedPolicy, TableLifecycleError> {
        self.create_policy_inner(request, None)
    }

    /// Replace an existing same-name policy in the same catalog commit that
    /// publishes its successor. The old policy is removed from the final
    /// catalog view before duplicate-name validation and the table epoch
    /// advances once for the complete replacement.
    pub fn replace_policy(
        &self,
        request: CreatePolicyRequest,
        replaced_policy_id: [u8; 16],
    ) -> Result<CreatedPolicy, TableLifecycleError> {
        if replaced_policy_id == [0; 16] {
            return Err(TableLifecycleError::InvalidRequest);
        }
        self.create_policy_inner(request, Some(replaced_policy_id))
    }

    fn create_policy_inner(
        &self,
        request: CreatePolicyRequest,
        replaced_policy_id: Option<[u8; 16]>,
    ) -> Result<CreatedPolicy, TableLifecycleError> {
        let CreatePolicyRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            name,
            kind,
            operation,
            effect,
            recipients,
            target_column_ids,
            using_expression,
            with_check_expression,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(&name)
            || recipients.is_empty()
            || recipients.len() > crate::authorization::MAX_POLICY_SUBJECTS
            || recipients.iter().copied().collect::<HashSet<_>>().len() != recipients.len()
            || recipients.iter().any(|subject| match subject {
                Subject::Principal(id) | Subject::Role(id) => *id == [0; 16],
            })
            || target_column_ids.len() > crate::tuple::MAX_COLUMNS
            || target_column_ids.contains(&[0; 16])
            || target_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != target_column_ids.len()
            || (kind == PolicyKind::Row && !target_column_ids.is_empty())
            || (kind == PolicyKind::Cell && target_column_ids.is_empty())
            || (kind == PolicyKind::Cell && operation == Operation::Delete)
            || !match operation {
                Operation::Select | Operation::Delete => {
                    using_expression.is_some() && with_check_expression.is_none()
                }
                Operation::Insert => using_expression.is_none() && with_check_expression.is_some(),
                Operation::Update => using_expression.is_some() && with_check_expression.is_some(),
                Operation::Usage => false,
            }
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
        read_catalog_record(&table_key, &table_bytes, 5)?;
        let table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_table_epoch = table_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_object_key = id_catalog_key(2, table_id);
        let table_object = catalog
            .get(table_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&table_object_key, &table_object, 2, 4)?;

        let type_definitions = load_builtin_type_definitions(self.database)?;
        let boolean_type_id = type_definitions
            .get(&1)
            .map(|(type_id, _)| *type_id)
            .ok_or(TableLifecycleError::Storage)?;
        let mut column_records = Vec::<(u32, [u8; 16], ColumnSchema)>::new();
        let column_prefix = 6u16.to_be_bytes();
        let mut scanned_columns = 0usize;
        for entry in catalog
            .range(column_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value();
            if !key.starts_with(&column_prefix) {
                break;
            }
            scanned_columns += 1;
            if scanned_columns > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(key, &bytes, 6, 1)? != table_id {
                continue;
            }
            let column_id: [u8; 16] = key
                .get(3..19)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let type_id = read_catalog_id(key, &bytes, 6, 4)?;
            let parameters: [u8; 8] = read_catalog_field(key, &bytes, 6, 5)?
                .as_slice()
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let type_record = type_definitions
                .values()
                .find(|(candidate_id, _)| *candidate_id == type_id)
                .map(|(_, record)| record)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let scalar_type = decode_builtin_scalar_type(type_record, &parameters)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let nullable = match read_catalog_field(key, &bytes, 6, 6)?.as_slice() {
                [0] => false,
                [1] => true,
                _ => return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog)),
            };
            column_records.push((
                read_catalog_u32(key, &bytes, 6, 2)?,
                column_id,
                ColumnSchema {
                    scalar_type,
                    nullable,
                },
            ));
        }
        column_records.sort_by_key(|(ordinal, _, _)| *ordinal);
        let column_ids = column_records
            .iter()
            .map(|(_, id, _)| *id)
            .collect::<Vec<_>>();
        let columns = column_records
            .iter()
            .map(|(_, _, column)| column.clone())
            .collect::<Vec<_>>();
        if target_column_ids
            .iter()
            .any(|column_id| !column_ids.contains(column_id))
        {
            return Err(TableLifecycleError::InvalidRequest);
        }

        for subject in &recipients {
            let (principal_id, expected_kind) = match subject {
                Subject::Principal(id) => (*id, 1),
                Subject::Role(id) => (*id, 2),
            };
            let principal_key = id_catalog_key(10, principal_id);
            let principal_bytes = catalog
                .get(principal_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::InvalidRequest)?;
            if read_catalog_u16(&principal_key, &principal_bytes, 10, 1)? != expected_kind {
                return Err(TableLifecycleError::InvalidRequest);
            }
        }

        let mut policy_scan = 0usize;
        let mut same_name_policy_id = None;
        let policy_prefix = 13u16.to_be_bytes();
        for entry in catalog
            .range(policy_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value();
            if !key.starts_with(&policy_prefix) {
                break;
            }
            policy_scan += 1;
            if policy_scan > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(key, &bytes, 13, 1)? != table_id {
                continue;
            }
            let policy_id: [u8; 16] = key
                .get(3..19)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let object_key = id_catalog_key(2, policy_id);
            let object_bytes = catalog
                .get(object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if read_catalog_field(&object_key, &object_bytes, 2, 3)? == name.as_bytes()
                && same_name_policy_id.replace(policy_id).is_some()
            {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
        }
        if same_name_policy_id != replaced_policy_id {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let mut replaced_expression_ids = Vec::new();
        let mut writes = if let Some(policy_id) = replaced_policy_id {
            let drop_write_set = Self::collect_policy_drop_writes(&catalog, policy_id)?;
            if drop_write_set.table_id != table_id || !matches!(drop_write_set.kind, 1 | 2) {
                return Err(TableLifecycleError::InvalidRequest);
            }
            replaced_expression_ids = drop_write_set.expression_ids;
            drop_write_set.writes
        } else {
            Vec::new()
        };

        let mut generated = HashSet::new();
        let policy_id = self.generate_table_object_id()?;
        if replaced_policy_id == Some(policy_id) {
            return Err(TableLifecycleError::ObjectIdGenerationFailed);
        }
        generated.insert(policy_id);
        let mut generated_expression_id = || -> Result<[u8; 16], TableLifecycleError> {
            for _ in 0..8 {
                let candidate = self.generate_table_object_id()?;
                if generated.insert(candidate) {
                    return Ok(candidate);
                }
            }
            Err(TableLifecycleError::ObjectIdGenerationFailed)
        };
        let using_expression_id = using_expression
            .as_ref()
            .map(|_| generated_expression_id())
            .transpose()?;
        let with_check_expression_id = with_check_expression
            .as_ref()
            .map(|_| generated_expression_id())
            .transpose()?;

        let kind_code = match kind {
            PolicyKind::Row => 1u16,
            PolicyKind::Cell => 2u16,
        };
        let operation_code = match operation {
            Operation::Select => 1u16,
            Operation::Insert => 2u16,
            Operation::Update => 3u16,
            Operation::Delete => 4u16,
            Operation::Usage => return Err(TableLifecycleError::InvalidRequest),
        };
        let effect_code = match effect {
            Effect::Allow => 1u16,
            Effect::Restrict => 2u16,
        };
        let policy_key = id_catalog_key(13, policy_id);
        let policy_id_hex = policy_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let mut policy_fields = vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: kind_code.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: operation_code.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: effect_code.to_le_bytes().to_vec(),
            },
        ];
        for (expression, expression_id, purpose, tag) in [
            (
                using_expression.as_ref(),
                using_expression_id,
                crate::expression::ExpressionPurpose::PolicyUsing,
                5,
            ),
            (
                with_check_expression.as_ref(),
                with_check_expression_id,
                crate::expression::ExpressionPurpose::PolicyCheck,
                6,
            ),
        ] {
            let (Some(expression), Some(expression_id)) = (expression, expression_id) else {
                continue;
            };
            let dependencies = policy_expression_dependencies(expression, &column_ids)?;
            for dependency_id in dependencies
                .iter()
                .filter(|dependency_id| !column_ids.contains(dependency_id))
            {
                let role_key = id_catalog_key(10, *dependency_id);
                let role_bytes = catalog
                    .get(role_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::InvalidRequest)?;
                if read_catalog_u16(&role_key, &role_bytes, 10, 1)? != 2 {
                    return Err(TableLifecycleError::InvalidRequest);
                }
            }
            let catalog_expression = crate::expression::CatalogExpression {
                expression_id,
                purpose,
                result_type_id: boolean_type_id,
                result_parameters: [0; 8],
                dependencies: dependencies.clone(),
                expression: expression.clone(),
            };
            crate::expression::validate_expression_binding(
                &catalog_expression,
                &columns,
                &column_ids,
                boolean_type_id,
                ScalarType::Boolean,
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let expression_bytes = crate::expression::encode_expression(expression)
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let expression_key = id_catalog_key(7, expression_id);
            let mut dependency_payload = (dependencies.len() as u32).to_le_bytes().to_vec();
            for dependency_id in &dependencies {
                dependency_payload.extend_from_slice(dependency_id);
            }
            let expression_payload = encode_envelope(
                &family_schema(7).ok_or(TableLifecycleError::Storage)?,
                &expression_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: match purpose {
                            crate::expression::ExpressionPurpose::PolicyUsing => 3u16,
                            crate::expression::ExpressionPurpose::PolicyCheck => 4u16,
                            _ => unreachable!(),
                        }
                        .to_le_bytes()
                        .to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U32,
                        payload: 1u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Id,
                        payload: boolean_type_id.to_vec(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Bytes,
                        payload: [0; 8].to_vec(),
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::Bytes,
                        payload: expression_bytes,
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: dependency_payload,
                    },
                ],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let object_key = id_catalog_key(2, expression_id);
            writes.push(CatalogWrite {
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
                                payload: 5u16.to_le_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 3,
                                wire_type: WireType::Text,
                                payload: format!("policy_{policy_id_hex}_{tag}").into_bytes(),
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
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            writes.push(CatalogWrite {
                key: expression_key,
                expected_value: None,
                payload: Some(expression_payload),
            });
            writes.push(new_dependency_write(policy_id, expression_id, 2)?);
            for dependency_id in dependencies {
                writes.push(new_dependency_write(expression_id, dependency_id, 2)?);
            }
            policy_fields.push(CatalogField {
                tag,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        policy_fields.push(CatalogField {
            tag: 7,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        });
        let policy_payload = encode_envelope(
            &family_schema(13).ok_or(TableLifecycleError::Storage)?,
            &policy_key,
            &policy_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        writes.push(CatalogWrite {
            key: policy_key,
            expected_value: None,
            payload: Some(policy_payload),
        });
        let policy_object_key = id_catalog_key(2, policy_id);
        writes.push(CatalogWrite {
            key: policy_object_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &policy_object_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: 10u16.to_le_bytes().to_vec(),
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
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });
        for recipient in recipients {
            let (target_kind, target_id) = match recipient {
                Subject::Principal(id) => (1, id),
                Subject::Role(id) => (2, id),
            };
            let payload = encode_policy_target(&PolicyTarget {
                policy_id,
                kind: target_kind,
                target_id,
            })
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            writes.push(CatalogWrite {
                key: payload[32..].to_vec(),
                expected_value: None,
                payload: Some(payload),
            });
            writes.push(new_dependency_write(policy_id, target_id, 5)?);
        }
        for column_id in target_column_ids {
            let payload = encode_policy_target(&PolicyTarget {
                policy_id,
                kind: 3,
                target_id: column_id,
            })
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            writes.push(CatalogWrite {
                key: payload[32..].to_vec(),
                expected_value: None,
                payload: Some(payload),
            });
        }
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_bytes,
                5,
                3,
                next_table_epoch,
            )?),
        });
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![policy_id];
        audit_event.target_ids.extend(replaced_policy_id);
        audit_event.target_ids.extend(replaced_expression_ids);
        audit_event.target_ids.extend(using_expression_id);
        audit_event.target_ids.extend(with_check_expression_id);
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_policy_ddl(
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
        Ok(CreatedPolicy {
            policy_id,
            using_expression_id,
            with_check_expression_id,
            commit_sequence,
        })
    }

    /// Remove one policy, its targets and private expressions under the
    /// authorized policy DDL path, advancing the table write epoch atomically.
    pub fn drop_policy(&self, request: DropPolicyRequest) -> Result<u64, TableLifecycleError> {
        let DropPolicyRequest {
            invocation_id,
            authenticated_actor_id,
            policy_id,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || policy_id == [0; 16]
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
        let policy_key = id_catalog_key(13, policy_id);
        let policy_bytes = catalog
            .get(policy_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let policy_record = read_catalog_record(&policy_key, &policy_bytes, 13)?;
        let table_id = read_catalog_id(&policy_key, &policy_bytes, 13, 1)?;
        let kind = read_catalog_u16(&policy_key, &policy_bytes, 13, 2)?;
        if !matches!(kind, 1 | 2) {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let mut writes = Vec::<CatalogWrite>::new();
        let mut expression_ids = Vec::new();
        for (tag, expected_purpose) in [
            (5u16, crate::expression::ExpressionPurpose::PolicyUsing),
            (6u16, crate::expression::ExpressionPurpose::PolicyCheck),
        ] {
            let Some(field) = policy_record.fields.iter().find(|field| field.tag == tag) else {
                continue;
            };
            let expression_id: [u8; 16] = field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let expression_key = id_catalog_key(7, expression_id);
            let expression_bytes = catalog
                .get(expression_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let expression = crate::expression::decode_catalog_expression(&expression_bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if expression.purpose != expected_purpose {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            writes.push(CatalogWrite {
                key: expression_key,
                expected_value: Some(expression_bytes),
                payload: None,
            });
            let expression_object_key = id_catalog_key(2, expression_id);
            let expression_object = catalog
                .get(expression_object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: expression_object_key,
                expected_value: Some(expression_object),
                payload: None,
            });
            writes.push(CatalogWrite {
                key: dependency_catalog_key(policy_id, expression_id, 2),
                expected_value: Some(
                    encode_envelope(
                        &family_schema(15).ok_or(TableLifecycleError::Storage)?,
                        &dependency_catalog_key(policy_id, expression_id, 2),
                        &[],
                    )
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                ),
                payload: None,
            });
            for dependency_id in expression.dependencies {
                let key = dependency_catalog_key(expression_id, dependency_id, 2);
                let value = catalog
                    .get(key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key,
                    expected_value: Some(value),
                    payload: None,
                });
            }
            expression_ids.push(expression_id);
        }

        let target_prefix = 14u16.to_be_bytes();
        let mut scanned_targets = 0usize;
        let mut has_recipient = false;
        for entry in catalog
            .range(target_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&target_prefix) {
                break;
            }
            scanned_targets += 1;
            if scanned_targets > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key.len() != 39 {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key[3..19] != policy_id {
                continue;
            }
            let target = decode_policy_target(value.value())
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if target.policy_id != policy_id {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if matches!(target.kind, 1 | 2) {
                has_recipient = true;
                let edge_key = dependency_catalog_key(policy_id, target.target_id, 5);
                let edge_value = catalog
                    .get(edge_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key: edge_key,
                    expected_value: Some(edge_value),
                    payload: None,
                });
            }
            writes.push(CatalogWrite {
                key,
                expected_value: Some(value.value().to_vec()),
                payload: None,
            });
        }
        if !has_recipient {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        for (key, value) in [
            (policy_key.clone(), policy_bytes),
            (
                id_catalog_key(2, policy_id),
                catalog
                    .get(id_catalog_key(2, policy_id).as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
            ),
        ] {
            writes.push(CatalogWrite {
                key,
                expected_value: Some(value),
                payload: None,
            });
        }
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_bytes,
                5,
                3,
                next_table_epoch,
            )?),
        });
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
        drop(catalog);
        drop(read);
        audit_event.target_ids = std::iter::once(policy_id).chain(expression_ids).collect();
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_policy_ddl(
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
        Ok(commit_sequence)
    }

    /// Remove one CHECK constraint and its private expression records under
    /// the table DDL fence.
    pub fn drop_check_constraint(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        constraint_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || constraint_id == [0; 16]
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
        let constraint_key = id_catalog_key(8, constraint_id);
        let constraint_bytes = catalog
            .get(constraint_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        read_catalog_record(&constraint_key, &constraint_bytes, 8)?;
        if read_catalog_u16(&constraint_key, &constraint_bytes, 8, 2)? != 4 {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let table_id = read_catalog_id(&constraint_key, &constraint_bytes, 8, 1)?;
        let expression_id = read_catalog_id(&constraint_key, &constraint_bytes, 8, 4)?;
        let expression_key = id_catalog_key(7, expression_id);
        let expression_bytes = catalog
            .get(expression_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let expression_record = crate::catalog::decode_registered_envelope(&expression_bytes)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let expression = crate::expression::decode_catalog_expression_record(&expression_record)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if expression.purpose != crate::expression::ExpressionPurpose::Check {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_table_epoch = table_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let mut writes = Vec::with_capacity(5 + expression.dependencies.len());
        for (object_id, key, bytes) in [
            (constraint_id, constraint_key, constraint_bytes),
            (expression_id, expression_key, expression_bytes),
        ] {
            writes.push(CatalogWrite {
                key,
                expected_value: Some(bytes),
                payload: None,
            });
            let object_key = id_catalog_key(2, object_id);
            let object_bytes = catalog
                .get(object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            });
        }
        for (dependent_id, referenced_id) in std::iter::once((constraint_id, expression_id)).chain(
            expression
                .dependencies
                .iter()
                .map(|dependency_id| (expression_id, *dependency_id)),
        ) {
            let edge = new_dependency_write(dependent_id, referenced_id, 2)?;
            let edge_bytes = catalog
                .get(edge.key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: edge.key,
                expected_value: Some(edge_bytes),
                payload: None,
            });
        }
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_bytes,
                5,
                3,
                next_table_epoch,
            )?),
        });
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
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![constraint_id, expression_id];
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
        Ok(commit_sequence)
    }

    /// Drop a UNIQUE constraint and its owned index. Referencing foreign keys
    /// are rejected by the final catalog dependency checks.
    pub fn drop_unique_constraint(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        constraint_id: [u8; 16],
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        self.drop_key_constraint(
            invocation_id,
            authenticated_actor_id,
            constraint_id,
            3,
            retention_deadline_us,
            audit_event,
        )
    }

    /// Drop a PRIMARY KEY and its owned index. Referencing foreign keys are
    /// rejected by the final catalog dependency checks.
    pub fn drop_primary_key(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        constraint_id: [u8; 16],
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        self.drop_key_constraint(
            invocation_id,
            authenticated_actor_id,
            constraint_id,
            2,
            retention_deadline_us,
            audit_event,
        )
    }

    fn drop_key_constraint(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        constraint_id: [u8; 16],
        expected_kind: u16,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || constraint_id == [0; 16]
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
        let constraint_key = id_catalog_key(8, constraint_id);
        let constraint_bytes = catalog
            .get(constraint_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        read_catalog_record(&constraint_key, &constraint_bytes, 8)?;
        if read_catalog_u16(&constraint_key, &constraint_bytes, 8, 2)? != expected_kind {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let table_id = read_catalog_id(&constraint_key, &constraint_bytes, 8, 1)?;
        let index_id = read_catalog_id(&constraint_key, &constraint_bytes, 8, 11)?;
        let index_key = id_catalog_key(9, index_id);
        let index_bytes = catalog
            .get(index_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&index_key, &index_bytes, 9)?;
        let edge_key = dependency_catalog_key(constraint_id, index_id, 4);
        let edge_bytes = catalog
            .get(edge_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let mut writes = Vec::with_capacity(6);
        for (key, bytes) in [
            (constraint_key.clone(), constraint_bytes),
            (index_key.clone(), index_bytes),
        ] {
            writes.push(CatalogWrite {
                key,
                expected_value: Some(bytes),
                payload: None,
            });
        }
        for object_id in [constraint_id, index_id] {
            let object_key = id_catalog_key(2, object_id);
            let object_bytes = catalog
                .get(object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            });
        }
        writes.push(CatalogWrite {
            key: edge_key,
            expected_value: Some(edge_bytes),
            payload: None,
        });
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_bytes,
                5,
                3,
                next_epoch,
            )?),
        });
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
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![constraint_id, index_id];
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

    /// Add a UNIQUE constraint and its owned unique index. The authorized
    /// constraint commit backfills and validates the index against the final
    /// table state under the table DDL fence.
    pub fn create_unique_constraint(
        &self,
        request: CreateUniqueConstraintRequest,
    ) -> Result<CreatedUniqueConstraint, TableLifecycleError> {
        self.create_key_constraint(request, 3, None)
    }

    pub fn create_named_unique_constraint(
        &self,
        request: CreateUniqueConstraintRequest,
        name: String,
    ) -> Result<CreatedUniqueConstraint, TableLifecycleError> {
        if !is_normalized_principal_name(&name) {
            return Err(TableLifecycleError::InvalidRequest);
        }
        self.create_key_constraint(request, 3, Some(name))
    }

    /// Add a PRIMARY KEY and its owned unique index. Existing rows are
    /// validated and the index is backfilled while the table DDL fence is held.
    pub fn create_primary_key(
        &self,
        request: CreatePrimaryKeyRequest,
    ) -> Result<CreatedPrimaryKey, TableLifecycleError> {
        let created = self.create_key_constraint(
            CreateUniqueConstraintRequest {
                invocation_id: request.invocation_id,
                authenticated_actor_id: request.authenticated_actor_id,
                table_id: request.table_id,
                column_ids: request.column_ids,
                retention_deadline_us: request.retention_deadline_us,
                audit_event: request.audit_event,
            },
            2,
            None,
        )?;
        Ok(CreatedPrimaryKey {
            constraint_id: created.constraint_id,
            index_id: created.index_id,
            commit_sequence: created.commit_sequence,
        })
    }

    pub fn create_named_primary_key(
        &self,
        request: CreatePrimaryKeyRequest,
        name: String,
    ) -> Result<CreatedPrimaryKey, TableLifecycleError> {
        if !is_normalized_principal_name(&name) {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let created = self.create_key_constraint(
            CreateUniqueConstraintRequest {
                invocation_id: request.invocation_id,
                authenticated_actor_id: request.authenticated_actor_id,
                table_id: request.table_id,
                column_ids: request.column_ids,
                retention_deadline_us: request.retention_deadline_us,
                audit_event: request.audit_event,
            },
            2,
            Some(name),
        )?;
        Ok(CreatedPrimaryKey {
            constraint_id: created.constraint_id,
            index_id: created.index_id,
            commit_sequence: created.commit_sequence,
        })
    }

    fn create_key_constraint(
        &self,
        request: CreateUniqueConstraintRequest,
        constraint_kind: u16,
        constraint_name: Option<String>,
    ) -> Result<CreatedUniqueConstraint, TableLifecycleError> {
        let CreateUniqueConstraintRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            column_ids,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_ids.is_empty()
            || column_ids.len() > crate::tuple::MAX_COLUMNS
            || column_ids.contains(&[0; 16])
            || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
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

        let constraint_id = self.generate_table_object_id()?;
        let mut index_id = self.generate_table_object_id()?;
        for _ in 0..8 {
            if index_id != constraint_id {
                break;
            }
            index_id = self.generate_table_object_id()?;
        }
        if index_id == constraint_id {
            return Err(TableLifecycleError::ObjectIdGenerationFailed);
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
        read_catalog_record(&table_key, &table_bytes, 5)?;
        let schema_version = read_catalog_u64(&table_key, &table_bytes, 5, 2)?;
        let table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_table_epoch = table_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;

        let table_object_key = id_catalog_key(2, table_id);
        let table_object_bytes = catalog
            .get(table_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&table_object_key, &table_object_bytes, 2, 4)?;

        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let constraint_key = id_catalog_key(8, constraint_id);
        let constraint_fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: constraint_kind.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: encode_ids(&column_ids),
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
            CatalogField {
                tag: 11,
                wire_type: WireType::Id,
                payload: index_id.to_vec(),
            },
        ];
        let constraint_payload = encode_envelope(
            &family_schema(8).ok_or(TableLifecycleError::Storage)?,
            &constraint_key,
            &constraint_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;

        let make_object_write =
            |object_id: [u8; 16], kind: u16, prefix: &str, name: Option<&str>| {
                let key = id_catalog_key(2, object_id);
                let name = name.map(str::to_owned).unwrap_or_else(|| {
                    format!(
                        "{prefix}_{}",
                        object_id
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                    )
                });
                let payload = encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: kind.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: name.into_bytes(),
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
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
                Ok::<_, TableLifecycleError>(CatalogWrite {
                    key,
                    expected_value: None,
                    payload: Some(payload),
                })
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
                payload: vec![1],
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Id,
                payload: constraint_id.to_vec(),
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
            &family_schema(9).ok_or(TableLifecycleError::Storage)?,
            &index_key,
            &index_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let writes = [
            CatalogWrite {
                key: constraint_key,
                expected_value: None,
                payload: Some(constraint_payload),
            },
            make_object_write(
                constraint_id,
                6,
                if constraint_kind == 2 {
                    "primary_key_constraint"
                } else {
                    "unique_constraint"
                },
                constraint_name.as_deref(),
            )?,
            CatalogWrite {
                key: index_key,
                expected_value: None,
                payload: Some(index_payload),
            },
            make_object_write(
                index_id,
                7,
                if constraint_kind == 2 {
                    "primary_key_index"
                } else {
                    "unique_index"
                },
                None,
            )?,
            new_dependency_write(constraint_id, index_id, 4)?,
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(
                    &table_key,
                    &table_bytes,
                    5,
                    3,
                    next_table_epoch,
                )?),
            },
        ];
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![constraint_id, index_id];
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
        Ok(CreatedUniqueConstraint {
            constraint_id,
            index_id,
            commit_sequence,
        })
    }

    /// Drop one foreign-key constraint and its dependency edge atomically.
    /// Both participating tables' write epochs advance so invocations pinned
    /// to the old referential graph cannot publish afterward.
    pub fn drop_foreign_key(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        foreign_key_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, CommitError> {
        if invocation_id == [0; 16]
            || authenticated_principal_id == [0; 16]
            || foreign_key_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(CommitError::InvalidWrite);
        }
        if audit_event.actor_id != Some(authenticated_principal_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(CommitError::InvalidAudit);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|error| CommitError::Storage(error.to_string()))?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|error| CommitError::Storage(error.to_string()))?;
        let foreign_key_key = id_catalog_key(8, foreign_key_id);
        let foreign_key_bytes = catalog
            .get(foreign_key_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if read_catalog_u16(&foreign_key_key, &foreign_key_bytes, 8, 2)
            .map_err(table_lifecycle_commit_error)?
            != 5
        {
            return Err(CommitError::InvalidCatalog);
        }
        let source_table_id = read_catalog_id(&foreign_key_key, &foreign_key_bytes, 8, 1)
            .map_err(table_lifecycle_commit_error)?;
        let target_constraint_id = read_catalog_id(&foreign_key_key, &foreign_key_bytes, 8, 5)
            .map_err(table_lifecycle_commit_error)?;
        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = catalog
            .get(target_constraint_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if !matches!(
            read_catalog_u16(&target_constraint_key, &target_constraint_bytes, 8, 2)
                .map_err(table_lifecycle_commit_error)?,
            2 | 3
        ) {
            return Err(CommitError::InvalidCatalog);
        }
        let target_table_id =
            read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)
                .map_err(table_lifecycle_commit_error)?;

        let object_key = id_catalog_key(2, foreign_key_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if read_catalog_u16(&object_key, &object_bytes, 2, 1)
            .map_err(table_lifecycle_commit_error)?
            != 6
        {
            return Err(CommitError::InvalidCatalog);
        }
        let dependency_key = dependency_catalog_key(foreign_key_id, target_constraint_id, 3);
        let dependency_bytes = catalog
            .get(dependency_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;

        let mut writes = vec![
            CatalogWrite {
                key: foreign_key_key,
                expected_value: Some(foreign_key_bytes),
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
        ];
        let mut table_ids = vec![source_table_id, target_table_id];
        table_ids.sort_unstable();
        table_ids.dedup();
        for table_id in table_ids {
            let table_key = id_catalog_key(5, table_id);
            let table_bytes = catalog
                .get(table_key.as_slice())
                .map_err(|error| CommitError::Storage(error.to_string()))?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidCatalog)?;
            let next_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)
                .map_err(table_lifecycle_commit_error)?
                .checked_add(1)
                .ok_or(CommitError::InvalidCatalog)?;
            writes.push(CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(
                    rewrite_catalog_u64_field(&table_key, &table_bytes, 5, 3, next_epoch)
                        .map_err(table_lifecycle_commit_error)?,
                ),
            });
        }
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![foreign_key_id];
        self.commit_authorized_foreign_key_ddl(
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
    }

    /// Authorize NOT NULL, PRIMARY KEY, UNIQUE, and CHECK constraint add/drop
    /// for an existing table. Key constraints atomically manage their owned
    /// unique backing indexes; additions validate stored rows before
    /// publication. The actor must own the table or hold inherited ALTER on
    /// the table/schema.
    pub fn commit_authorized_constraint_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        let mut writes = catalog_writes.to_vec();
        let mut retired_ids = HashSet::new();
        for write in catalog_writes {
            if write.key.starts_with(&17u16.to_be_bytes())
                && let Some(id) = write.key.get(3..19).and_then(|bytes| bytes.try_into().ok())
            {
                retired_ids.insert(id);
            }
            if !write.key.starts_with(&2u16.to_be_bytes()) || write.payload.is_some() {
                continue;
            }
            let Some(id) = write.key.get(3..19).and_then(|bytes| bytes.try_into().ok()) else {
                continue;
            };
            let Some(bytes) = write.expected_value.as_deref() else {
                continue;
            };
            let record = read_catalog_record(&write.key, bytes, 2)
                .map_err(|_| CommitError::InvalidCatalog)?;
            let kind = record
                .fields
                .iter()
                .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
                .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
                .map(u16::from_le_bytes);
            if kind.is_some_and(|kind| (5..=7).contains(&kind)) {
                retired_ids.insert(id);
            }
        }
        for id in retired_ids {
            let key = retired_object_id_catalog_key(id);
            if writes.iter().any(|write| write.key == key) {
                continue;
            }
            writes.push(CatalogWrite {
                payload: Some(
                    encode_envelope(
                        &family_schema(17).ok_or(CommitError::InvalidCatalog)?,
                        &key,
                        &[],
                    )
                    .map_err(|_| CommitError::InvalidCatalog)?,
                ),
                key,
                expected_value: None,
            });
        }
        commit_authorized_constraint_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            &writes,
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

    /// Resolve a retained terminal outcome for an already authenticated owner.
    /// Unknown, mismatched, and expired records all return `None`.
    pub fn lookup_invocation_outcome(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationOutcome>, Box<dyn std::error::Error>> {
        if self.requires_reopen_after_commit_error() {
            return Err(std::io::Error::other(
                "database requires reopen after an ambiguous commit error",
            )
            .into());
        }
        lookup_outcome(
            self.database,
            invocation_id,
            authenticated_principal_id,
            now_us,
        )
    }

    /// Return live state from this process registry or a retained durable
    /// outcome. Callers must authenticate the principal before this lookup;
    /// unknown IDs, expired outcomes, and other owners all return `None`.
    pub fn get_invocation_status(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationStatusSnapshot>, InvocationStatusError> {
        if invocation_id == [0; 16] || authenticated_principal_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        if let Some(live) = self.admissions.status(invocation_id) {
            if live.principal_id != authenticated_principal_id {
                return Ok(None);
            }
            if self.requires_reopen_after_commit_error() && !live.commit_uncertain {
                return Err(InvocationStatusError::Storage);
            }
            let status = match live.phase {
                LiveInvocationPhase::Active => InvocationStatus::Active {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
                LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: false,
                },
                LiveInvocationPhase::Committing => InvocationStatus::Committing {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
            };
            return Ok(Some(InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status,
            }));
        }
        if self.requires_reopen_after_commit_error() {
            return Err(InvocationStatusError::Storage);
        }
        lookup_outcome(
            self.database,
            invocation_id,
            authenticated_principal_id,
            now_us,
        )
        .map_err(|_| InvocationStatusError::Storage)
        .map(|outcome| {
            outcome.map(|outcome| InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status: InvocationStatus::Terminal(outcome),
            })
        })
    }

    /// Return one invocation's live or retained terminal status to a current
    /// database administrator. Authorization is checked on a fresh catalog
    /// snapshot before the invocation ID is inspected.
    pub fn get_administrative_invocation_status(
        &self,
        authenticated_administrator_id: [u8; 16],
        invocation_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationStatusSnapshot>, AdministrativeInvocationListError> {
        if authenticated_administrator_id == [0; 16] || invocation_id == [0; 16] || now_us <= 0 {
            return Err(AdministrativeInvocationListError::InvalidRequest);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let snapshot = RowSnapshot::new(read);
        let is_administrator = database_admin_authority(&snapshot, authenticated_administrator_id)
            .map_err(|error| match error {
                TokenLifecycleError::TokenUnavailable => {
                    AdministrativeInvocationListError::Unauthorized
                }
                _ => AdministrativeInvocationListError::Storage,
            })?;
        drop(snapshot);
        if !is_administrator {
            return Err(AdministrativeInvocationListError::Unauthorized);
        }

        if self.requires_reopen_after_commit_error() {
            return match self.admissions.status(invocation_id) {
                Some(live) if live.commit_uncertain => Ok(Some(InvocationStatusSnapshot {
                    observed_at_us: now_us,
                    consistency: InvocationStatusConsistency::BestEffort,
                    status: InvocationStatus::Committing {
                        started_at_us: live.started_at_us,
                        deadline_at_us: live.deadline_at_us,
                        cancellation_requested: live.cancellation_reason_code.is_some(),
                    },
                })),
                _ => Err(AdministrativeInvocationListError::Storage),
            };
        }

        if let Some(live) = self.admissions.status(invocation_id) {
            let status = match live.phase {
                LiveInvocationPhase::Active | LiveInvocationPhase::CancellationPending => {
                    InvocationStatus::Active {
                        started_at_us: live.started_at_us,
                        deadline_at_us: live.deadline_at_us,
                        cancellation_requested: live.cancellation_reason_code.is_some(),
                    }
                }
                LiveInvocationPhase::Committing => InvocationStatus::Committing {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
            };
            return Ok(Some(InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status,
            }));
        }

        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let bytes = read
            .open_table(OUTCOMES)
            .map_err(|_| AdministrativeInvocationListError::Storage)?
            .get(&invocation_id[..])
            .map_err(|_| AdministrativeInvocationListError::Storage)?
            .map(|guard| guard.value().to_vec());
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let outcome = InvocationOutcome::decode(&bytes)
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        if outcome.invocation_id != invocation_id {
            return Err(AdministrativeInvocationListError::Storage);
        }
        if now_us >= outcome.retention_deadline_us {
            return Ok(None);
        }
        Ok(Some(InvocationStatusSnapshot {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            status: InvocationStatus::Terminal(outcome),
        }))
    }

    /// Request cancellation for an invocation owned by the authenticated
    /// principal. The commit transition and cancellation request are
    /// serialized by the admission registry; unknown and foreign IDs match.
    pub fn cancel_owned_invocation(
        &self,
        authenticated_principal_id: [u8; 16],
        invocation_id: [u8; 16],
        now_us: i64,
    ) -> Result<OwnerCancellationResult, InvocationStatusError> {
        if authenticated_principal_id == [0; 16] || invocation_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        loop {
            let mut live = self
                .admissions
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let invocation = live.entries.get(&invocation_id).copied();
            match invocation {
                Some(invocation) if invocation.principal_id != authenticated_principal_id => {
                    return Ok(OwnerCancellationResult::Unknown);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Committing => {
                    return Ok(OwnerCancellationResult::TooLate);
                }
                Some(invocation)
                    if invocation.phase == LiveInvocationPhase::CancellationPending =>
                {
                    live = self
                        .admissions
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    drop(live);
                }
                Some(invocation) if invocation.cancellation_reason_code.is_some() => {
                    return Ok(OwnerCancellationResult::Accepted);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Active => {
                    if let Some(entry) = live.entries.get_mut(&invocation_id) {
                        entry.phase = LiveInvocationPhase::CancellationPending;
                    }
                    drop(live);
                    let mut live = self
                        .admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(entry) = live.entries.get_mut(&invocation_id) else {
                        self.admissions.changed.notify_all();
                        drop(live);
                        continue;
                    };
                    if entry.phase == LiveInvocationPhase::CancellationPending {
                        entry.phase = LiveInvocationPhase::Active;
                        entry.cancellation_reason_code = Some(1);
                        live.advance_owner_generation(authenticated_principal_id);
                    }
                    self.admissions.changed.notify_all();
                    return Ok(OwnerCancellationResult::Accepted);
                }
                None => {
                    drop(live);
                    return match lookup_outcome(
                        self.database,
                        invocation_id,
                        authenticated_principal_id,
                        now_us,
                    )
                    .map_err(|_| InvocationStatusError::Storage)?
                    {
                        Some(outcome) => Ok(OwnerCancellationResult::Terminal(outcome)),
                        None => Ok(OwnerCancellationResult::Unknown),
                    };
                }
                Some(_) => return Ok(OwnerCancellationResult::TooLate),
            }
        }
    }

    /// Return a bounded best-effort page of this authenticated owner's live
    /// invocations. Terminal outcomes remain available through point status.
    /// A change to this owner's entries invalidates an outstanding
    /// continuation so pages never silently skip or repeat this owner's rows.
    pub fn list_own_invocations(
        &self,
        authenticated_principal_id: [u8; 16],
        now_us: i64,
        page_limit: usize,
        continuation: Option<&InvocationListCursor>,
    ) -> Result<InvocationStatusPage, InvocationStatusError> {
        if authenticated_principal_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        if page_limit == 0 || page_limit > MAX_OWN_INVOCATION_PAGE {
            return Err(InvocationStatusError::InvalidPageLimit);
        }
        if continuation.is_some_and(|cursor| {
            cursor.principal_id == [0; 16] || cursor.after_invocation_id == [0; 16]
        }) {
            return Err(InvocationStatusError::InvalidCursor);
        }
        let (invocations, continuation) =
            self.admissions
                .own_page(authenticated_principal_id, page_limit, continuation)?;
        Ok(InvocationStatusPage {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            invocations,
            continuation,
        })
    }

    /// Return a bounded best-effort page of live invocations for a current
    /// database administrator. Authority is revalidated from the latest
    /// catalog snapshot on every call; no global total is returned.
    pub fn list_runtime_invocations(
        &self,
        authenticated_administrator_id: [u8; 16],
        now_us: i64,
        page_limit: usize,
        continuation: Option<&AdministrativeInvocationCursor>,
    ) -> Result<AdministrativeInvocationPage, AdministrativeInvocationListError> {
        if authenticated_administrator_id == [0; 16] || now_us <= 0 {
            return Err(AdministrativeInvocationListError::InvalidRequest);
        }
        if page_limit == 0 || page_limit > MAX_ADMIN_INVOCATION_PAGE {
            return Err(AdministrativeInvocationListError::InvalidPageLimit);
        }
        if continuation.is_some_and(|cursor| {
            cursor.administrator_id == [0; 16] || cursor.after_invocation_id == [0; 16]
        }) {
            return Err(AdministrativeInvocationListError::InvalidCursor);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let snapshot = RowSnapshot::new(read);
        let is_administrator = database_admin_authority(&snapshot, authenticated_administrator_id)
            .map_err(|error| match error {
                TokenLifecycleError::TokenUnavailable => {
                    AdministrativeInvocationListError::Unauthorized
                }
                _ => AdministrativeInvocationListError::Storage,
            })?;
        drop(snapshot);
        if !is_administrator {
            return Err(AdministrativeInvocationListError::Unauthorized);
        }
        let observed_at = Instant::now();
        let projection = self.admissions.administrative_page(
            authenticated_administrator_id,
            now_us,
            observed_at,
            page_limit,
            continuation,
        )?;
        let (fence_waits, fence_waits_truncated) = self.ddl_fences.wait_snapshot();
        Ok(AdministrativeInvocationPage {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            invocations: projection.invocations,
            pinned_snapshot_count: projection.pinned_snapshot_count,
            oldest_snapshot_pin_age_us: projection.oldest_snapshot_pin_age_us,
            invocations_admitted_total: projection.invocations_admitted_total,
            invocation_conflicts_total: projection.invocation_conflicts_total,
            admin_cancellations_accepted_total: projection.admin_cancellations_accepted_total,
            admin_cancellations_denied_total: projection.admin_cancellations_denied_total,
            invocation_policy_denials_total: projection.invocation_policy_denials_total,
            invocation_staged_value_bytes_total: projection.invocation_staged_value_bytes_total,
            invocation_commit_latency_buckets: projection.invocation_commit_latency_buckets,
            invocation_redb_write_acquisition_buckets: projection
                .invocation_redb_write_acquisition_buckets,
            outcome_maintenance_passes_total: projection.outcome_maintenance_passes_total,
            outcome_maintenance_failures_total: projection.outcome_maintenance_failures_total,
            outcomes_pruned_total: projection.outcomes_pruned_total,
            fence_waits,
            fence_waits_truncated,
            continuation: projection.continuation,
        })
    }

    /// Return a bounded page of durable AUD1 records in exclusive event-ID
    /// order. The cursor is bound to the administrator and authority is
    /// checked against the same redb snapshot used to read the audit table.
    pub fn list_audit_events(
        &self,
        authenticated_administrator_id: [u8; 16],
        page_limit: usize,
        continuation: Option<&AdministrativeAuditCursor>,
    ) -> Result<AdministrativeAuditPage, AdministrativeAuditListError> {
        if authenticated_administrator_id == [0; 16] {
            return Err(AdministrativeAuditListError::InvalidRequest);
        }
        if page_limit == 0 || page_limit > MAX_ADMIN_AUDIT_PAGE {
            return Err(AdministrativeAuditListError::InvalidPageLimit);
        }
        if continuation.is_some_and(|cursor| {
            cursor.administrator_id != authenticated_administrator_id
                || cursor.after_event_id == [0; 16]
        }) {
            return Err(AdministrativeAuditListError::InvalidCursor);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeAuditListError::Storage)?;
        let snapshot = RowSnapshot::new(read);
        let is_administrator = database_admin_authority(&snapshot, authenticated_administrator_id)
            .map_err(|error| match error {
                TokenLifecycleError::TokenUnavailable => AdministrativeAuditListError::Unauthorized,
                _ => AdministrativeAuditListError::Storage,
            })?;
        if !is_administrator {
            return Err(AdministrativeAuditListError::Unauthorized);
        }
        let audit = snapshot
            .read_transaction()
            .open_table(AUDIT)
            .map_err(|_| AdministrativeAuditListError::Storage)?;
        let mut events = Vec::with_capacity(page_limit);
        let mut has_more = false;
        let first_event_id = [0; 16];
        let lower_bound = continuation
            .map(|cursor| std::ops::Bound::Excluded(cursor.after_event_id.as_slice()))
            .unwrap_or(std::ops::Bound::Included(first_event_id.as_slice()));
        let entries = audit
            .range::<&[u8]>((lower_bound, std::ops::Bound::Unbounded))
            .map_err(|_| AdministrativeAuditListError::Storage)?;
        for entry in entries {
            let (key, value) = entry.map_err(|_| AdministrativeAuditListError::Storage)?;
            let key: [u8; 16] = key
                .value()
                .try_into()
                .map_err(|_| AdministrativeAuditListError::Storage)?;
            let event = AuditEvent::decode(value.value())
                .map_err(|_| AdministrativeAuditListError::Storage)?;
            if event.event_id != key {
                return Err(AdministrativeAuditListError::Storage);
            }
            if events.len() == page_limit {
                has_more = true;
                break;
            }
            events.push(AdministrativeAuditEvent {
                event_id: event.event_id,
                actor_id: event.actor_id,
                correlation_id: event.correlation_id,
                occurred_at_us: event.occurred_at_us,
                action: event.action,
                result: event.result,
                target_ids: event.target_ids,
            });
        }
        let continuation = has_more.then(|| AdministrativeAuditCursor {
            administrator_id: authenticated_administrator_id,
            after_event_id: events.last().expect("nonempty page has more").event_id,
        });
        Ok(AdministrativeAuditPage {
            events,
            continuation,
        })
    }

    /// Request audited cancellation for a live invocation. A short-lived
    /// pending state makes commit wait for the durable audit decision without
    /// holding the registry lock during redb I/O.
    pub fn cancel_invocation(
        &self,
        authenticated_administrator_id: [u8; 16],
        invocation_id: [u8; 16],
        reason_code: u16,
        now_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<AdministrativeCancellationResult, AdministrativeCancellationError> {
        if authenticated_administrator_id == [0; 16]
            || invocation_id == [0; 16]
            || reason_code == 0
            || now_us <= 0
        {
            return Err(AdministrativeCancellationError::InvalidRequest);
        }
        self.admin_cancellation_requests
            .consume(authenticated_administrator_id)
            .map_err(|_| AdministrativeCancellationError::RateLimited)?;
        if audit_event.actor_id != Some(authenticated_administrator_id)
            || audit_event.action != AuditAction::AdministrativeCancellation
            || audit_event.target_ids.as_slice() != [invocation_id]
        {
            return Err(AdministrativeCancellationError::InvalidAudit);
        }

        loop {
            let mut live = self
                .admissions
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let invocation = live.entries.get(&invocation_id).copied();
            match invocation {
                Some(invocation)
                    if invocation.phase == LiveInvocationPhase::CancellationPending =>
                {
                    live = self
                        .admissions
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    drop(live);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Committing => {
                    drop(live);
                    audit_event.result = AuditResult::Denied;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    self.admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .record_admin_cancellation_denial();
                    return Ok(AdministrativeCancellationResult::TooLate);
                }
                Some(invocation) if invocation.cancellation_reason_code.is_some() => {
                    drop(live);
                    audit_event.result = AuditResult::Accepted;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    return Ok(AdministrativeCancellationResult::Accepted);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Active => {
                    if let Some(entry) = live.entries.get_mut(&invocation_id) {
                        entry.phase = LiveInvocationPhase::CancellationPending;
                    }
                    drop(live);

                    audit_event.result = AuditResult::Accepted;
                    let audit_result = persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    );

                    let mut live = self
                        .admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let mut changed = false;
                    if let Some(entry) = live.entries.get_mut(&invocation_id)
                        && entry.phase == LiveInvocationPhase::CancellationPending
                    {
                        entry.phase = LiveInvocationPhase::Active;
                        if audit_result.is_ok() {
                            entry.cancellation_reason_code = Some(reason_code);
                            changed = true;
                        }
                    }
                    if changed {
                        live.record_admin_cancellation();
                        live.advance_owner_generation(invocation.principal_id);
                    }
                    self.admissions.changed.notify_all();
                    audit_result?;
                    return Ok(AdministrativeCancellationResult::Accepted);
                }
                None => {
                    drop(live);
                    audit_event.result = AuditResult::Denied;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    self.admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .record_admin_cancellation_denial();
                    return match lookup_any_outcome(self.database, invocation_id, now_us)? {
                        Some(outcome) => Ok(AdministrativeCancellationResult::Terminal(outcome)),
                        None => Ok(AdministrativeCancellationResult::Unknown),
                    };
                }
                Some(_) => return Ok(AdministrativeCancellationResult::TooLate),
            }
        }
    }
}

fn generate_query_connection_id() -> Result<[u8; 16], TransactionError> {
    for _ in 0..8 {
        let mut connection_id = [0u8; 16];
        getrandom::fill(&mut connection_id).map_err(|_| TransactionError::RandomnessUnavailable)?;
        if connection_id != [0; 16] {
            return Ok(connection_id);
        }
    }
    Err(TransactionError::RandomnessUnavailable)
}

fn database_admin_authority(
    snapshot: &RowSnapshot,
    actor_id: [u8; 16],
) -> Result<bool, TokenLifecycleError> {
    let metadata_bytes = snapshot
        .get_engine_metadata_value()
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
        .ok_or_else(|| TokenLifecycleError::Storage("engine metadata is missing".into()))?;
    let metadata = EngineMetadata::decode(&metadata_bytes)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
        return Err(TokenLifecycleError::Storage(
            "database is not ready for runtime listing".into(),
        ));
    }
    let roles = token_actor_roles(snapshot, actor_id)?;
    let mut database_admin_role = [0x80; 16];
    database_admin_role[15] = 3;
    Ok(roles.contains(&database_admin_role))
}

fn persist_authentication_failure_audit(
    database: &Database,
    attempted_token_id: [u8; 16],
) -> Result<(), TransactionError> {
    let event_id = generate_query_connection_id()?;
    let correlation_id = generate_query_connection_id()?;
    let occurred_at_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
    let event = AuditEvent {
        event_id,
        actor_id: None,
        correlation_id,
        occurred_at_us,
        action: AuditAction::AuthenticationFailure,
        result: AuditResult::Denied,
        target_ids: (attempted_token_id != [0; 16])
            .then_some(attempted_token_id)
            .into_iter()
            .collect(),
    };
    let encoded = event
        .encode()
        .map_err(|_| TransactionError::Storage("invalid authentication audit event".into()))?;
    let storage_error = |error: &dyn std::fmt::Display| {
        mark_database_uncertain(database);
        TransactionError::Storage(error.to_string())
    };
    let mut write = database
        .begin_write()
        .map_err(|error| storage_error(&error))?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|error| storage_error(&error))?;
    write.set_two_phase_commit(true);
    {
        let mut audit = write
            .open_table(AUDIT)
            .map_err(|error| storage_error(&error))?;
        let current_events = usize::try_from(audit.len().map_err(|error| storage_error(&error))?)
            .map_err(|_| TransactionError::AuditCapacityExceeded)?;
        if !crate::audit::has_audit_capacity(current_events, 1) {
            return Err(TransactionError::AuditCapacityExceeded);
        }
        audit
            .insert(event.event_id.as_slice(), encoded.as_slice())
            .map_err(|error| storage_error(&error))?;
    }
    write.commit().map_err(|error| storage_error(&error))
}

fn persist_admin_cancellation_audit(
    database: &Database,
    administrator_id: [u8; 16],
    event: &AuditEvent,
) -> Result<(), AdministrativeCancellationError> {
    let storage_error = || {
        mark_database_uncertain(database);
        AdministrativeCancellationError::Storage
    };
    if event.actor_id != Some(administrator_id)
        || event.action != AuditAction::AdministrativeCancellation
        || !matches!(event.result, AuditResult::Accepted | AuditResult::Denied)
    {
        return Err(AdministrativeCancellationError::InvalidAudit);
    }
    let encoded = event
        .encode()
        .map_err(|_| AdministrativeCancellationError::InvalidAudit)?;

    let read = database.begin_read().map_err(|_| storage_error())?;
    let snapshot = RowSnapshot::new(read);
    let is_administrator =
        database_admin_authority(&snapshot, administrator_id).map_err(|error| match error {
            TokenLifecycleError::TokenUnavailable => AdministrativeCancellationError::Unauthorized,
            _ => storage_error(),
        })?;
    if !is_administrator {
        return Err(AdministrativeCancellationError::Unauthorized);
    }

    let membership_records = snapshot
        .scan_catalog_family(11, MAX_CATALOG_FAMILY_SCAN)
        .map_err(|_| storage_error())?;
    let mut guarded_values = HashMap::<Vec<u8>, Vec<u8>>::new();
    let mut principal_ids = HashSet::from([administrator_id]);
    for record in membership_records {
        let key = record.logical_key;
        if key.len() != 36 || key[2] != 1 || key[19] != 1 {
            return Err(storage_error());
        }
        let member_id: [u8; 16] = key[3..19].try_into().map_err(|_| storage_error())?;
        let role_id: [u8; 16] = key[20..36].try_into().map_err(|_| storage_error())?;
        principal_ids.insert(member_id);
        principal_ids.insert(role_id);
        let value = snapshot
            .get_catalog_value(&key)
            .map_err(|_| storage_error())?
            .ok_or_else(&storage_error)?;
        guarded_values.insert(key, value);
    }
    for principal_id in principal_ids {
        let key = principal_catalog_key(principal_id);
        let value = snapshot
            .get_catalog_value(&key)
            .map_err(|_| storage_error())?
            .ok_or_else(&storage_error)?;
        guarded_values.insert(key, value);
    }
    drop(snapshot);

    let mut write = database.begin_write().map_err(|_| storage_error())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|_| storage_error())?;
    write.set_two_phase_commit(true);
    {
        let catalog = write.open_table(CATALOG).map_err(|_| storage_error())?;
        for (key, expected) in guarded_values {
            let current = catalog
                .get(key.as_slice())
                .map_err(|_| storage_error())?
                .map(|value| value.value().to_vec());
            if current.as_deref() != Some(expected.as_slice()) {
                return Err(AdministrativeCancellationError::Unauthorized);
            }
        }
    }
    {
        let mut audit = write.open_table(AUDIT).map_err(|_| storage_error())?;
        let current_events = usize::try_from(audit.len().map_err(|_| storage_error())?)
            .map_err(|_| AdministrativeCancellationError::AuditCapacityExceeded)?;
        if !crate::audit::has_audit_capacity(current_events, 1) {
            return Err(AdministrativeCancellationError::AuditCapacityExceeded);
        }
        if audit
            .get(event.event_id.as_slice())
            .map_err(|_| storage_error())?
            .is_some()
        {
            return Err(AdministrativeCancellationError::InvalidAudit);
        }
        audit
            .insert(event.event_id.as_slice(), encoded.as_slice())
            .map_err(|_| storage_error())?;
    }
    match write.commit() {
        Ok(()) => Ok(()),
        Err(_) => Err(storage_error()),
    }
}

fn lookup_any_outcome(
    database: &Database,
    invocation_id: [u8; 16],
    now_us: i64,
) -> Result<Option<InvocationOutcome>, AdministrativeCancellationError> {
    let read = database
        .begin_read()
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    let outcomes = read
        .open_table(OUTCOMES)
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    let Some(value) = outcomes
        .get(invocation_id.as_slice())
        .map_err(|_| AdministrativeCancellationError::Storage)?
    else {
        return Ok(None);
    };
    let outcome = InvocationOutcome::decode(value.value())
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    if outcome.invocation_id != invocation_id {
        return Err(AdministrativeCancellationError::Storage);
    }
    Ok((now_us < outcome.retention_deadline_us).then_some(outcome))
}

fn token_actor_roles(
    snapshot: &RowSnapshot,
    actor_id: [u8; 16],
) -> Result<HashSet<[u8; 16]>, TokenLifecycleError> {
    let actor = snapshot
        .get_catalog_record(&principal_catalog_key(actor_id))
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
        .ok_or(TokenLifecycleError::TokenUnavailable)?;
    if token_catalog_u16(&actor, 1)? != 1
        || token_catalog_field(&actor, 3)? != [1]
        || token_catalog_u64(&actor, 4)? == 0
    {
        return Err(TokenLifecycleError::TokenUnavailable);
    }

    let membership_records = snapshot
        .scan_catalog_family(11, MAX_CATALOG_FAMILY_SCAN)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    if membership_records.len() > MAX_MEMBERSHIP_EDGES {
        return Err(TokenLifecycleError::Storage(
            "membership graph exceeds its configured limit".into(),
        ));
    }
    let mut memberships = Vec::with_capacity(membership_records.len());
    let mut principal_ids = HashSet::from([actor_id]);
    for record in membership_records {
        let key = &record.logical_key;
        if key.len() != 36 || key[2] != 1 || key[19] != 1 {
            return Err(TokenLifecycleError::Storage(
                "invalid membership catalog key".into(),
            ));
        }
        if token_catalog_id(&record, 1)? == [0; 16] || token_catalog_u64(&record, 2)? == 0 {
            return Err(TokenLifecycleError::Storage(
                "invalid membership catalog record".into(),
            ));
        }
        let member_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| TokenLifecycleError::Storage("invalid membership key".into()))?;
        let role_id: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| TokenLifecycleError::Storage("invalid membership key".into()))?;
        memberships.push(Membership { member_id, role_id });
        principal_ids.insert(member_id);
        principal_ids.insert(role_id);
    }
    for principal_id in &principal_ids {
        let record = snapshot
            .get_catalog_record(&principal_catalog_key(*principal_id))
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
            .ok_or_else(|| {
                TokenLifecycleError::Storage("membership references a missing principal".into())
            })?;
        let kind = token_catalog_u16(&record, 1)?;
        let login = token_catalog_field(&record, 3)?;
        if !matches!(kind, 1 | 2)
            || !matches!(login, [0] | [1])
            || token_catalog_u64(&record, 4)? == 0
            || (kind == 2 && login != [0])
            || (memberships.iter().any(|edge| edge.role_id == *principal_id) && kind != 2)
        {
            return Err(TokenLifecycleError::Storage(
                "invalid membership principal".into(),
            ));
        }
    }
    let roles = resolve_role_closure(actor_id, &memberships)
        .map_err(|_| TokenLifecycleError::Storage("invalid membership graph".into()))?;
    Ok(roles)
}

fn token_inventory_authority(
    snapshot: &RowSnapshot,
    actor_id: [u8; 16],
    user_id: [u8; 16],
) -> Result<bool, TokenLifecycleError> {
    let metadata_bytes = snapshot
        .get_engine_metadata_value()
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
        .ok_or_else(|| TokenLifecycleError::Storage("engine metadata is missing".into()))?;
    let metadata = EngineMetadata::decode(&metadata_bytes)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
        return Err(TokenLifecycleError::Storage(
            "database is not ready for token inventory".into(),
        ));
    }

    let roles = token_actor_roles(snapshot, actor_id)?;
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(actor_id);
    let mut database_admin_role = [0x80; 16];
    database_admin_role[15] = 3;
    let is_database_admin = roles.contains(&database_admin_role);

    if actor_id == user_id {
        return Ok(true);
    }
    if is_database_admin
        || token_inventory_has_manage_users(snapshot, metadata.database_id, &authority_subjects)?
    {
        let target = snapshot
            .get_catalog_record(&principal_catalog_key(user_id))
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        return match target {
            Some(record) if token_catalog_u16(&record, 1)? == 1 => Ok(true),
            _ => Ok(false),
        };
    }
    Ok(false)
}

fn token_inventory_has_manage_users(
    snapshot: &RowSnapshot,
    database_id: [u8; 16],
    authority_subjects: &HashSet<[u8; 16]>,
) -> Result<bool, TokenLifecycleError> {
    for subject_id in authority_subjects {
        let key = system_privilege_catalog_key(*subject_id, database_id, 12);
        let Some(record) = snapshot
            .get_catalog_record(&key)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
        else {
            continue;
        };
        if token_catalog_id(&record, 1)? == [0; 16] || token_catalog_u64(&record, 2)? == 0 {
            return Err(TokenLifecycleError::Storage("invalid system grant".into()));
        }
        return Ok(true);
    }
    Ok(false)
}

fn principal_catalog_key(principal_id: [u8; 16]) -> Vec<u8> {
    let mut key = 10u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&principal_id);
    key
}

fn id_catalog_key(family: u16, object_id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

fn read_catalog_record(
    key: &[u8],
    bytes: &[u8],
    family: u16,
) -> Result<CatalogRecord, TableLifecycleError> {
    let record = decode_registered_envelope(bytes)
        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
    if record.family_code != family || record.logical_key != key {
        return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
    }
    Ok(record)
}

fn decode_catalog_id_list(bytes: &[u8]) -> Result<Vec<[u8; 16]>, TableLifecycleError> {
    let invalid = || TableLifecycleError::Commit(CommitError::InvalidCatalog);
    let count_bytes: [u8; 4] = bytes
        .get(..4)
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    let payload_len = count.checked_mul(16).ok_or_else(invalid)?;
    if bytes.len() != 4usize.checked_add(payload_len).ok_or_else(invalid)? {
        return Err(invalid());
    }
    bytes[4..]
        .chunks_exact(16)
        .map(|chunk| chunk.try_into().map_err(|_| invalid()))
        .collect()
}

fn read_catalog_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<Vec<u8>, TableLifecycleError> {
    read_catalog_record(key, bytes, family)?
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.clone())
        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

fn read_catalog_id(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<[u8; 16], TableLifecycleError> {
    read_catalog_field(key, bytes, family, tag)?
        .as_slice()
        .try_into()
        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

fn read_catalog_u32(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u32, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u32::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

fn read_catalog_u16(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u16, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u16::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

fn read_catalog_u64(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u64, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u64::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

fn rewrite_catalog_u32_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: u32,
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_fields(
        key,
        bytes,
        family,
        &[(tag, WireType::U32, value.to_le_bytes().to_vec())],
    )
}

fn rewrite_catalog_u64_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: u64,
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_u64_fields(key, bytes, family, &[(tag, value)])
}

fn rewrite_catalog_u64_fields(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    values: &[(u16, u64)],
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_fields(
        key,
        bytes,
        family,
        &values
            .iter()
            .map(|(tag, value)| (*tag, WireType::U64, value.to_le_bytes().to_vec()))
            .collect::<Vec<_>>(),
    )
}

fn rewrite_catalog_fields(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    values: &[(u16, WireType, Vec<u8>)],
) -> Result<Vec<u8>, TableLifecycleError> {
    let mut record = read_catalog_record(key, bytes, family)?;
    for (tag, wire_type, payload) in values {
        let field = record
            .fields
            .iter_mut()
            .find(|field| field.tag == *tag)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if field.wire_type != *wire_type {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        field.payload.clone_from(payload);
    }
    encode_envelope(
        &family_schema(family).ok_or(TableLifecycleError::Storage)?,
        key,
        &record.fields,
    )
    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

fn rewrite_optional_catalog_id_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: Option<[u8; 16]>,
) -> Result<Vec<u8>, TableLifecycleError> {
    let mut record = read_catalog_record(key, bytes, family)?;
    if let Some(index) = record.fields.iter().position(|field| field.tag == tag) {
        if record.fields[index].wire_type != WireType::Id {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        match value {
            Some(value) => record.fields[index].payload = value.to_vec(),
            None => {
                record.fields.remove(index);
            }
        }
    } else if let Some(value) = value {
        let index = record
            .fields
            .binary_search_by_key(&tag, |field| field.tag)
            .unwrap_or_else(|index| index);
        record.fields.insert(
            index,
            CatalogField {
                tag,
                wire_type: WireType::Id,
                payload: value.to_vec(),
            },
        );
    }
    encode_envelope(
        &family_schema(family).ok_or(TableLifecycleError::Storage)?,
        key,
        &record.fields,
    )
    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

fn new_dependency_write(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    kind: u16,
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = dependency_catalog_key(dependent_id, referenced_id, kind);
    let payload = encode_envelope(
        &family_schema(15).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[],
    )
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

fn policy_expression_dependencies(
    expression: &crate::authorization::Expr,
    column_ids: &[[u8; 16]],
) -> Result<Vec<[u8; 16]>, TableLifecycleError> {
    fn visit(
        expression: &crate::authorization::Expr,
        column_ids: &[[u8; 16]],
        dependencies: &mut HashSet<[u8; 16]>,
    ) -> Result<(), TableLifecycleError> {
        use crate::authorization::Expr;
        match expression {
            Expr::Literal(_) | Expr::CurrentPrincipalId => {}
            Expr::Field(ordinal) => {
                let column_id = column_ids
                    .get(*ordinal)
                    .copied()
                    .ok_or(TableLifecycleError::InvalidRequest)?;
                dependencies.insert(column_id);
            }
            Expr::HasRole(role_id) => {
                if *role_id == [0; 16] {
                    return Err(TableLifecycleError::InvalidRequest);
                }
                dependencies.insert(*role_id);
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
                visit(left, column_ids, dependencies)?;
                visit(right, column_ids, dependencies)?;
            }
            Expr::And(expressions) | Expr::Or(expressions) => {
                for expression in expressions {
                    visit(expression, column_ids, dependencies)?;
                }
            }
            Expr::Not(inner) | Expr::IsNull(inner) | Expr::Negate(inner) => {
                visit(inner, column_ids, dependencies)?;
            }
        }
        Ok(())
    }

    let mut dependencies = HashSet::new();
    visit(expression, column_ids, &mut dependencies)?;
    let mut dependencies = dependencies.into_iter().collect::<Vec<_>>();
    dependencies.sort_unstable();
    Ok(dependencies)
}

fn new_default_expression_object_write(
    expression_id: [u8; 16],
    ordinal: usize,
    owner_id: [u8; 16],
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = id_catalog_key(2, expression_id);
    let payload = encode_envelope(
        &family_schema(2).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 5u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: format!("default_{ordinal}").into_bytes(),
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
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

fn new_default_expression_write(
    expression_id: [u8; 16],
    type_id: [u8; 16],
    type_parameters: [u8; 8],
    expression: crate::authorization::Expr,
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = id_catalog_key(7, expression_id);
    let ir = crate::expression::encode_expression(&expression)
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
    let payload = encode_envelope(
        &family_schema(7).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Id,
                payload: type_id.to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bytes,
                payload: type_parameters.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: ir,
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            },
        ],
    )
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

fn scalar_type_kind(scalar_type: ScalarType) -> u16 {
    match scalar_type {
        ScalarType::Boolean => 1,
        ScalarType::SmallInt => 2,
        ScalarType::Integer => 3,
        ScalarType::BigInt => 4,
        ScalarType::Decimal { .. } => 5,
        ScalarType::Text => 6,
        ScalarType::Varchar { .. } => 7,
        ScalarType::Bytea => 8,
        ScalarType::Uuid => 9,
        ScalarType::Date => 10,
        ScalarType::TimestampTz => 11,
    }
}

fn scalar_type_parameters(scalar_type: ScalarType) -> [u8; 8] {
    let mut parameters = [0; 8];
    match scalar_type {
        ScalarType::Decimal { precision, scale } => {
            parameters[..2].copy_from_slice(&u16::from(precision).to_le_bytes());
            parameters[2..4].copy_from_slice(&u16::from(scale).to_le_bytes());
        }
        ScalarType::Varchar { max_chars } => {
            parameters[4..].copy_from_slice(&max_chars.to_le_bytes());
        }
        _ => {}
    }
    parameters
}

fn load_builtin_type_definitions(
    database: &Database,
) -> Result<HashMap<u16, ([u8; 16], CatalogRecord)>, TableLifecycleError> {
    let read = database
        .begin_read()
        .map_err(|_| TableLifecycleError::Storage)?;
    let metadata_bytes = read
        .open_table(ENGINE_META)
        .map_err(|_| TableLifecycleError::Storage)?
        .get(&b"format"[..])
        .map_err(|_| TableLifecycleError::Storage)?
        .ok_or(TableLifecycleError::Storage)?
        .value()
        .to_vec();
    let metadata =
        EngineMetadata::decode(&metadata_bytes).map_err(|_| TableLifecycleError::Storage)?;
    if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
        return Err(TableLifecycleError::Storage);
    }
    let database_key = id_catalog_key(1, metadata.database_id);
    let catalog = read
        .open_table(CATALOG)
        .map_err(|_| TableLifecycleError::Storage)?;
    let database_record = catalog
        .get(database_key.as_slice())
        .map_err(|_| TableLifecycleError::Storage)?
        .map(|value| decode_registered_envelope(value.value()))
        .transpose()
        .map_err(|_| TableLifecycleError::Storage)?
        .ok_or(TableLifecycleError::Storage)?;
    if database_record.family_code != 1 || database_record.logical_key != database_key {
        return Err(TableLifecycleError::Storage);
    }
    let builtins = database_record
        .fields
        .iter()
        .find(|field| field.tag == 3 && field.wire_type == WireType::Bytes)
        .map(|field| field.payload.as_slice())
        .ok_or(TableLifecycleError::Storage)?;
    if builtins.len() < 4 {
        return Err(TableLifecycleError::Storage);
    }
    let count = u32::from_le_bytes(builtins[..4].try_into().unwrap()) as usize;
    if count != 31 || builtins.len() != 4 + count * 20 {
        return Err(TableLifecycleError::Storage);
    }
    let mut type_ids = HashMap::with_capacity(11);
    for entry in builtins[4..].chunks_exact(20) {
        let code = u16::from_le_bytes(entry[..2].try_into().unwrap());
        let reserved = u16::from_le_bytes(entry[2..4].try_into().unwrap());
        if (101..=111).contains(&code) {
            let type_id: [u8; 16] = entry[4..20].try_into().unwrap();
            if reserved != 0 || type_id == [0; 16] || type_ids.insert(code - 100, type_id).is_some()
            {
                return Err(TableLifecycleError::Storage);
            }
        }
    }
    if type_ids.len() != 11 {
        return Err(TableLifecycleError::Storage);
    }
    let mut result = HashMap::with_capacity(11);
    for (kind, type_id) in type_ids {
        let key = id_catalog_key(4, type_id);
        let record = catalog
            .get(key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| decode_registered_envelope(value.value()))
            .transpose()
            .map_err(|_| TableLifecycleError::Storage)?
            .ok_or(TableLifecycleError::Storage)?;
        if record.family_code != 4 || record.logical_key != key {
            return Err(TableLifecycleError::Storage);
        }
        result.insert(kind, (type_id, record));
    }
    Ok(result)
}

fn identity_object_catalog_key(object_id: [u8; 16]) -> Vec<u8> {
    let mut key = 2u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

fn retired_object_id_catalog_key(object_id: [u8; 16]) -> Vec<u8> {
    let mut key = 17u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

fn membership_catalog_key(member_id: [u8; 16], role_id: [u8; 16]) -> Vec<u8> {
    let mut key = 11u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&member_id);
    key.push(1);
    key.extend_from_slice(&role_id);
    key
}

fn dependency_catalog_key(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    dependency_kind: u16,
) -> Vec<u8> {
    let mut key = 15u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&dependent_id);
    key.push(1);
    key.extend_from_slice(&referenced_id);
    key.push(1);
    key.extend_from_slice(&dependency_kind.to_be_bytes());
    key
}

fn object_grant_catalog_key(
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

fn is_normalized_principal_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

fn system_privilege_catalog_key(
    grantee_id: [u8; 16],
    database_id: [u8; 16],
    privilege: u16,
) -> Vec<u8> {
    let mut key = 12u16.to_be_bytes().to_vec();
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&grantee_id);
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&database_id);
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&privilege.to_be_bytes());
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&1u16.to_be_bytes());
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&database_id);
    key
}

fn token_catalog_field(record: &CatalogRecord, tag: u16) -> Result<&[u8], TokenLifecycleError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.as_slice())
        .ok_or_else(|| TokenLifecycleError::Storage("invalid catalog payload".into()))
}

fn token_catalog_id(record: &CatalogRecord, tag: u16) -> Result<[u8; 16], TokenLifecycleError> {
    token_catalog_field(record, tag)?
        .try_into()
        .map_err(|_| TokenLifecycleError::Storage("invalid catalog ID".into()))
}

fn token_catalog_u16(record: &CatalogRecord, tag: u16) -> Result<u16, TokenLifecycleError> {
    token_catalog_field(record, tag)?
        .try_into()
        .map(u16::from_le_bytes)
        .map_err(|_| TokenLifecycleError::Storage("invalid catalog integer".into()))
}

fn token_catalog_u64(record: &CatalogRecord, tag: u16) -> Result<u64, TokenLifecycleError> {
    token_catalog_field(record, tag)?
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| TokenLifecycleError::Storage("invalid catalog integer".into()))
}

fn credential_catalog_key(token_id: [u8; 16]) -> Vec<u8> {
    let mut key = 16u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&token_id);
    key
}

fn credential_token_exists(
    database: &Database,
    token_id: [u8; 16],
) -> Result<bool, TokenLifecycleError> {
    let read = database
        .begin_read()
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    let table = read
        .open_table(CATALOG)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    table
        .get(credential_catalog_key(token_id).as_slice())
        .map(|value| value.is_some())
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))
}

fn lookup_credential_principal(
    database: &Database,
    token_id: [u8; 16],
) -> Result<[u8; 16], TransactionError> {
    if token_id == [0; 16] {
        return Err(TransactionError::AuthenticationFailed);
    }
    let read = database
        .begin_read()
        .map_err(|error| TransactionError::Storage(error.to_string()))?;
    let table = read
        .open_table(CATALOG)
        .map_err(|error| TransactionError::Storage(error.to_string()))?;
    let key = credential_catalog_key(token_id);
    let bytes = table
        .get(key.as_slice())
        .map_err(|error| TransactionError::Storage(error.to_string()))?
        .map(|value| value.value().to_vec())
        .ok_or(TransactionError::AuthenticationFailed)?;
    let record =
        decode_registered_envelope(&bytes).map_err(|_| TransactionError::AuthenticationFailed)?;
    if record.logical_key != key {
        return Err(TransactionError::AuthenticationFailed);
    }
    let credential =
        decode_verifier_record(&record).map_err(|_| TransactionError::AuthenticationFailed)?;
    Ok(credential.user_id)
}

#[cfg(test)]
mod commit_cancellation_boundary_tests {
    use super::{
        AdmissionError, InvocationAdmissionState, InvocationListCursor, LiveInvocationRegistry,
        OwnerCancellationResult, StorageManager, StorageManagerConfig, bounded_invocation_ids,
    };
    use redb::Database;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn invocation_page_selection_copies_only_a_sorted_bounded_prefix() {
        let ids = [
            [9; 16], [2; 16], [7; 16], [1; 16], [8; 16], [3; 16], [6; 16], [4; 16],
        ];
        assert_eq!(
            bounded_invocation_ids(ids, Some([2; 16]), 2),
            vec![[3; 16], [4; 16], [6; 16]]
        );
        assert_eq!(bounded_invocation_ids(ids, None, 0), Vec::<[u8; 16]>::new());
    }

    #[test]
    fn owner_invocation_cursor_round_trips_canonically() {
        let cursor = InvocationListCursor {
            principal_id: [0xab; 16],
            owner_generation: 0x1234,
            after_invocation_id: [0xcd; 16],
        };
        let wire = cursor.to_wire();
        assert_eq!(InvocationListCursor::from_wire(&wire), Some(cursor));
        assert!(InvocationListCursor::from_wire(&wire.to_uppercase()).is_none());
        assert!(InvocationListCursor::from_wire(&wire[..96]).is_none());
    }

    #[test]
    fn analytical_admission_cap_preserves_operational_capacity() {
        let state = std::sync::Arc::new(InvocationAdmissionState {
            database_key: 0,
            limit: std::sync::atomic::AtomicUsize::new(3),
            analytical_limit: std::sync::atomic::AtomicUsize::new(1),
            statistics_collection_limit: std::sync::atomic::AtomicUsize::new(
                crate::statistics::MAX_COMMITTED_POPULATION_ROWS as usize,
            ),
            statistics_collection_active: std::sync::atomic::AtomicBool::new(false),
            database_uncertain: std::sync::atomic::AtomicBool::new(false),
            live: std::sync::Mutex::new(LiveInvocationRegistry {
                generation: 1,
                invocations_admitted_total: 0,
                invocation_conflicts_total: 0,
                admin_cancellations_accepted_total: 0,
                admin_cancellations_denied_total: 0,
                invocation_policy_denials_total: 0,
                invocation_staged_value_bytes_total: 0,
                invocation_commit_latency_buckets: [0; 6],
                invocation_redb_write_acquisition_buckets: [0; 6],
                outcome_maintenance_passes_total: 0,
                outcome_maintenance_failures_total: 0,
                outcomes_pruned_total: 0,
                entries: std::collections::HashMap::new(),
            }),
            changed: std::sync::Condvar::new(),
        });
        let analytical = state
            .acquire(
                [0x71; 16],
                [0x81; 16],
                crate::supervisor::WorkloadClass::Analytical,
                1_000,
                Duration::from_secs(60),
            )
            .expect("first analytical invocation should be admitted");
        assert_eq!(
            state
                .acquire(
                    [0x72; 16],
                    [0x82; 16],
                    crate::supervisor::WorkloadClass::Analytical,
                    1_001,
                    Duration::from_secs(60),
                )
                .err(),
            Some(AdmissionError::Limit)
        );
        let operational = state
            .acquire(
                [0x73; 16],
                [0x83; 16],
                crate::supervisor::WorkloadClass::Operational,
                1_002,
                Duration::from_secs(60),
            )
            .expect("operational invocation should use reserved global capacity");
        drop((analytical, operational));
    }

    #[test]
    fn admission_conflict_cancellation_and_policy_counters_saturate_at_u64_max() {
        let state = std::sync::Arc::new(InvocationAdmissionState {
            database_key: 0,
            limit: std::sync::atomic::AtomicUsize::new(1),
            analytical_limit: std::sync::atomic::AtomicUsize::new(1),
            statistics_collection_limit: std::sync::atomic::AtomicUsize::new(
                crate::statistics::MAX_COMMITTED_POPULATION_ROWS as usize,
            ),
            statistics_collection_active: std::sync::atomic::AtomicBool::new(false),
            database_uncertain: std::sync::atomic::AtomicBool::new(false),
            live: std::sync::Mutex::new(LiveInvocationRegistry {
                generation: 1,
                invocations_admitted_total: u64::MAX,
                invocation_conflicts_total: u64::MAX,
                admin_cancellations_accepted_total: u64::MAX,
                admin_cancellations_denied_total: u64::MAX,
                invocation_policy_denials_total: u64::MAX,
                invocation_staged_value_bytes_total: u64::MAX,
                invocation_commit_latency_buckets: [u64::MAX; 6],
                invocation_redb_write_acquisition_buckets: [u64::MAX; 6],
                outcome_maintenance_passes_total: u64::MAX,
                outcome_maintenance_failures_total: u64::MAX,
                outcomes_pruned_total: u64::MAX,
                entries: std::collections::HashMap::new(),
            }),
            changed: std::sync::Condvar::new(),
        });
        let permit = state
            .acquire(
                [0x74; 16],
                [0x84; 16],
                crate::supervisor::WorkloadClass::Operational,
                1_003,
                Duration::from_secs(60),
            )
            .expect("admission should succeed at a saturated counter");
        permit.record_conflict();
        state.live.lock().unwrap().record_admin_cancellation();
        state
            .live
            .lock()
            .unwrap()
            .record_admin_cancellation_denial();
        permit.metrics().record_policy_denial();
        permit.record_staged_value_bytes(1);
        permit
            .metrics()
            .record_commit_latency(Duration::from_micros(1_001));
        state.live.lock().unwrap().record_outcome_maintenance(&Ok(
            crate::outcome::OutcomePruneResult {
                scanned: 2,
                removed: 1,
                cycle_complete: true,
            },
        ));
        state.live.lock().unwrap().record_outcome_maintenance(&Err(
            crate::outcome::OutcomeMaintenanceError::InvalidLimit,
        ));
        assert_eq!(
            {
                let live = state.live.lock().unwrap();
                (
                    live.invocations_admitted_total,
                    live.invocation_conflicts_total,
                    live.admin_cancellations_accepted_total,
                    live.admin_cancellations_denied_total,
                    live.invocation_policy_denials_total,
                    live.invocation_staged_value_bytes_total,
                    live.invocation_commit_latency_buckets,
                    live.outcome_maintenance_passes_total,
                    live.outcome_maintenance_failures_total,
                    live.outcomes_pruned_total,
                )
            },
            (
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                [u64::MAX; 6],
                u64::MAX,
                u64::MAX,
                u64::MAX
            )
        );
        drop(permit);
    }

    #[test]
    fn invocation_commit_latency_uses_fixed_non_cumulative_buckets() {
        let mut live = LiveInvocationRegistry {
            generation: 1,
            invocations_admitted_total: 0,
            invocation_conflicts_total: 0,
            admin_cancellations_accepted_total: 0,
            admin_cancellations_denied_total: 0,
            invocation_policy_denials_total: 0,
            invocation_staged_value_bytes_total: 0,
            invocation_commit_latency_buckets: [0; 6],
            invocation_redb_write_acquisition_buckets: [0; 6],
            outcome_maintenance_passes_total: 0,
            outcome_maintenance_failures_total: 0,
            outcomes_pruned_total: 0,
            entries: std::collections::HashMap::new(),
        };
        for micros in [
            1_000, 1_001, 5_000, 5_001, 10_000, 10_001, 50_000, 50_001, 100_000, 100_001,
        ] {
            live.record_invocation_commit_latency(Duration::from_micros(micros));
        }
        live.record_invocation_commit_latency(
            Duration::from_micros(1_000) + Duration::from_nanos(1),
        );
        assert_eq!(live.invocation_commit_latency_buckets, [1, 3, 2, 2, 2, 1]);
    }

    #[test]
    fn invocation_redb_write_acquisition_uses_fixed_non_cumulative_buckets() {
        let mut live = LiveInvocationRegistry {
            generation: 1,
            invocations_admitted_total: 0,
            invocation_conflicts_total: 0,
            admin_cancellations_accepted_total: 0,
            admin_cancellations_denied_total: 0,
            invocation_policy_denials_total: 0,
            invocation_staged_value_bytes_total: 0,
            invocation_commit_latency_buckets: [0; 6],
            invocation_redb_write_acquisition_buckets: [0; 6],
            outcome_maintenance_passes_total: 0,
            outcome_maintenance_failures_total: 0,
            outcomes_pruned_total: 0,
            entries: std::collections::HashMap::new(),
        };
        for micros in [
            1_000, 1_001, 5_000, 5_001, 10_000, 10_001, 50_000, 50_001, 100_000, 100_001,
        ] {
            live.record_invocation_redb_write_acquisition(Duration::from_micros(micros));
        }
        assert_eq!(
            live.invocation_redb_write_acquisition_buckets,
            [1, 2, 2, 2, 2, 1]
        );
        live.invocation_redb_write_acquisition_buckets[5] = u64::MAX;
        live.record_invocation_redb_write_acquisition(Duration::from_secs(1));
        assert_eq!(live.invocation_redb_write_acquisition_buckets[5], u64::MAX);
    }

    #[test]
    fn outcome_maintenance_counters_count_passes_failures_and_removals() {
        let mut live = LiveInvocationRegistry {
            generation: 1,
            invocations_admitted_total: 0,
            invocation_conflicts_total: 0,
            admin_cancellations_accepted_total: 0,
            admin_cancellations_denied_total: 0,
            invocation_policy_denials_total: 0,
            invocation_staged_value_bytes_total: 0,
            invocation_commit_latency_buckets: [0; 6],
            invocation_redb_write_acquisition_buckets: [0; 6],
            outcome_maintenance_passes_total: 0,
            outcome_maintenance_failures_total: 0,
            outcomes_pruned_total: 0,
            entries: std::collections::HashMap::new(),
        };

        live.record_outcome_maintenance(&Ok(crate::outcome::OutcomePruneResult {
            scanned: 4,
            removed: 3,
            cycle_complete: false,
        }));
        live.record_outcome_maintenance(&Ok(crate::outcome::OutcomePruneResult {
            scanned: 0,
            removed: 0,
            cycle_complete: true,
        }));
        live.record_outcome_maintenance(&Err(
            crate::outcome::OutcomeMaintenanceError::InvalidLimit,
        ));

        assert_eq!(live.outcome_maintenance_passes_total, 3);
        assert_eq!(live.outcome_maintenance_failures_total, 1);
        assert_eq!(live.outcomes_pruned_total, 3);
    }

    #[test]
    fn outcome_maintenance_api_updates_process_metrics() -> Result<(), Box<dyn std::error::Error>> {
        use crate::outcome::{InvocationOutcome, OutcomeStatus};
        use crate::schema::{OUTCOMES, create_tables};
        let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "prdb-outcome-metrics-{}-{fixture_id}",
            std::process::id()
        ));
        std::fs::create_dir(&directory)?;
        let database = Database::create(directory.join("metrics.redb"))?;
        let write = database.begin_write()?;
        create_tables(&write)?;
        {
            let mut outcomes = write.open_table(OUTCOMES)?;
            let expired = InvocationOutcome {
                status: OutcomeStatus::Aborted,
                invocation_id: [0x71; 16],
                principal_id: [0x72; 16],
                commit_sequence: 0,
                retention_deadline_us: 10,
            }
            .encode()?;
            outcomes.insert([0x71; 16].as_slice(), expired.as_slice())?;
        }
        write.commit()?;

        let storage =
            StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
        assert_eq!(storage.prune_expired_outcomes(11, 1)?.removed, 1);
        assert!(matches!(
            storage.prune_expired_outcomes(11, 0),
            Err(crate::outcome::OutcomeMaintenanceError::InvalidLimit)
        ));
        let live = storage
            .admissions
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(live.outcome_maintenance_passes_total, 2);
        assert_eq!(live.outcome_maintenance_failures_total, 1);
        assert_eq!(live.outcomes_pruned_total, 1);
        drop(live);
        drop(storage);
        drop(database);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn cancellation_after_committing_transition_is_too_late()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "prdb-cancel-boundary-{}-{fixture_id}",
            std::process::id()
        ));
        std::fs::create_dir(&directory)?;
        let database = Database::create(directory.join("boundary.redb"))?;
        let storage =
            StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
        let invocation_id = [0x73; 16];
        let principal_id = [0x74; 16];
        let permit = storage
            .admissions
            .acquire(
                invocation_id,
                principal_id,
                crate::supervisor::WorkloadClass::Operational,
                1_000,
                Duration::from_secs(60),
            )
            .map_err(|error| {
                std::io::Error::other(format!("unexpected admission error: {error:?}"))
            })?;
        let (transition_sender, transition_receiver) = mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = mpsc::sync_channel(0);

        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let transition_thread = scope.spawn(move || {
                permit.try_mark_committing_then(|| {
                    transition_sender.send(()).unwrap();
                    resume_receiver.recv().unwrap();
                })
            });
            transition_receiver.recv()?;
            assert_eq!(
                storage.cancel_owned_invocation(principal_id, invocation_id, 1_001)?,
                OwnerCancellationResult::TooLate
            );
            resume_sender.send(())?;
            assert!(
                transition_thread
                    .join()
                    .map_err(|_| std::io::Error::other("commit transition thread panicked"))?
            );
            Ok(())
        })?;

        drop(storage);
        drop(database);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}

#[cfg(test)]
mod worker_storage_lifetime_tests {
    use super::{
        InvocationStatus, StorageManagerConfig, SupervisorChannelState, SupervisorInvocation,
        SupervisorInvocationError,
    };
    use std::collections::HashSet;
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn worker_exit_waits_for_both_channel_handlers_to_drain()
    -> Result<(), Box<dyn std::error::Error>> {
        let invocation = SupervisorInvocation {
            invocation_id: [0x81; 16],
            principal_id: [0x82; 16],
            role_ids: HashSet::new(),
            transaction: Arc::new(Mutex::new(None)),
            channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
            deadline: None,
            abort_on_drop: true,
        };
        let (planner_worker, planner_storage) = UnixStream::pair()?;
        let (executor_worker, executor_storage) = UnixStream::pair()?;
        let (optimizer_worker, optimizer_storage) = UnixStream::pair()?;

        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let service = scope.spawn(|| {
                invocation.serve_worker_channels(
                    planner_storage,
                    executor_storage,
                    optimizer_storage,
                )
            });
            let active_channel_deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let active = invocation
                    .channels
                    .lock()
                    .map_err(|_| std::io::Error::other("channel state poisoned"))?
                    .active_worker_channels;
                if active == 3 {
                    break;
                }
                assert!(Instant::now() < active_channel_deadline);
                std::thread::yield_now();
            }

            let mut worker = std::process::Command::new("true").spawn()?;
            assert!(matches!(
                invocation.observe_worker_exit(&mut worker),
                Err(SupervisorInvocationError::WorkerChannelsActive)
            ));
            drop(planner_worker);
            drop(executor_worker);
            drop(optimizer_worker);
            let service_result = service
                .join()
                .map_err(|_| std::io::Error::other("worker-channel service panicked"))?;
            assert!(
                service_result.is_ok(),
                "worker-channel service failed: {service_result:?}"
            );
            {
                let channels = invocation.channels.lock().unwrap();
                assert_eq!(channels.active_worker_channels, 0);
                assert!(!channels.worker_exit_succeeded);
                assert!(!channels.terminal_requested);
            }
            assert!(worker.wait()?.success());
            let exit_result = invocation.observe_worker_exit(&mut worker);
            assert!(
                exit_result.is_ok(),
                "worker exit observation failed: {exit_result:?}"
            );
            assert!(exit_result?.success());
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn cancellation_keeps_snapshot_pinned_until_in_flight_storage_request_drains()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::outcome::OutcomeStatus;
        use crate::storage_manager::OwnerCancellationResult;
        use std::sync::mpsc;
        use std::time::{SystemTime, UNIX_EPOCH};
        use zeroize::Zeroize;

        struct TempDirectory(std::path::PathBuf);
        impl Drop for TempDirectory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let directory = TempDirectory(std::env::temp_dir().join(format!(
            "prdb-worker-storage-pin-{}-{nonce}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        )));
        std::fs::create_dir(&directory.0)?;
        let instance_directory = directory.0.join("instance");
        let mut bootstrap =
            crate::bootstrap::initialize(&instance_directory, "pin_admin", i64::MAX)?;
        let admin_id = bootstrap.admin_user_id();
        let token_id = bootstrap.token_id();
        let mut token_secret = bootstrap.take_secret().ok_or("bootstrap secret missing")?;
        let instance = bootstrap.acknowledge()?;
        let storage = instance.storage_manager(StorageManagerConfig::default())?;
        let now_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
        let invocation_id = [0x91; 16];
        let invocation = storage.admit_supervisor_invocation(
            invocation_id,
            token_id,
            &token_secret,
            now_us + 60_000_000,
            4,
            1024,
        )?;
        token_secret.zeroize();

        let (request_entered_tx, request_entered_rx) = mpsc::channel();
        let (resume_request_tx, resume_request_rx) = mpsc::channel();
        let (checkpoint_started_tx, checkpoint_started_rx) = mpsc::channel();
        let (checkpoint_done_tx, checkpoint_done_rx) = mpsc::channel();
        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let invocation_ref = &invocation;
            let request = scope.spawn(move || {
                invocation_ref.with_owned_transaction(|transaction| {
                    request_entered_tx
                        .send(())
                        .expect("signal in-flight storage request");
                    resume_request_rx
                        .recv()
                        .expect("resume in-flight storage request");
                    transaction
                        .read(crate::overlay::RowId {
                            table: [0x92; 16],
                            row: [0x93; 16],
                        })
                        .map(|_| ())
                })
            });
            request_entered_rx.recv_timeout(Duration::from_secs(2))?;
            assert_eq!(
                storage.cancel_owned_invocation(admin_id, invocation_id, now_us + 1)?,
                OwnerCancellationResult::Accepted
            );

            let invocation_ref = &invocation;
            let checkpoint = scope.spawn(move || {
                checkpoint_started_tx
                    .send(())
                    .expect("signal cancellation checkpoint");
                let result = invocation_ref.check_deadline();
                checkpoint_done_tx
                    .send(result.is_err())
                    .expect("report cancellation checkpoint completion");
                result
            });
            checkpoint_started_rx.recv_timeout(Duration::from_secs(2))?;

            let pinned_page = storage.list_runtime_invocations(admin_id, now_us + 2, 128, None)?;
            let pinned = pinned_page
                .invocations
                .iter()
                .find(|item| item.invocation_id == invocation_id)
                .ok_or("cancellation target disappeared while storage request was active")?;
            assert!(matches!(
                &pinned.status,
                InvocationStatus::Active {
                    cancellation_requested: true,
                    ..
                }
            ));
            assert_eq!(pinned.snapshot_pin_count, 1);

            let checkpoint_wait = checkpoint_done_rx.recv_timeout(Duration::from_millis(25));
            resume_request_tx.send(())?;
            let request_result = request.join().map_err(|_| "storage request panicked")?;
            assert!(matches!(
                request_result,
                Ok(()) | Err(SupervisorInvocationError::Transaction(_))
            ));
            assert!(
                checkpoint_wait.is_err(),
                "checkpoint must wait for the request lock"
            );
            assert!(
                checkpoint
                    .join()
                    .map_err(|_| "cancellation checkpoint panicked")?
                    .is_err()
            );
            Ok(())
        })?;

        assert!(matches!(
            storage.lookup_invocation_outcome(invocation_id, admin_id, now_us + 3)?,
            Some(outcome) if outcome.status == OutcomeStatus::Aborted
        ));
        drop(invocation);
        drop(storage);
        drop(instance);
        Ok(())
    }
}
