use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::catalog_index::lookup_schema_id_by_name;
use prdb_engine::commit::{
    CatalogWrite, CommitError, IndexWrite, RowWrite, commit_catalog_and_audit, commit_rows,
    commit_rows_and_indexes,
};
use prdb_engine::metadata::EngineMetadata;
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus, lookup_outcome};
use prdb_engine::schema::{self, AUDIT, CATALOG, ENGINE_META, ROWS};
use prdb_engine::storage_manager::{
    AdministrativeCancellationError, StorageManager, StorageManagerConfig, StorageManagerInitError,
};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadableDatabase};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::helpers::*;

pub(super) fn denied_monitoring_pressure_does_not_starve_commit_or_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::with_config(
        &database,
        prdb_engine::storage_manager::StorageManagerConfig {
            max_active_invocations: 256,
            ..prdb_engine::storage_manager::StorageManagerConfig::default()
        },
    )?;
    let admin_id = [0xd4; 16];
    let ordinary_id = [0xd3; 16];
    let cancel_id = [0xd1; 16];
    let commit_id = [0xd2; 16];
    let admin_role = builtin_owner_id();
    let admin_records = [
        principal_write_with_name(admin_id, 1, 1, "monitoring_pressure_admin"),
        object_write_with_name(admin_id, 8, "monitoring_pressure_admin"),
        membership_write_with_grantor(admin_id, admin_role, admin_role),
        dependency_write(admin_id, admin_role, 5),
    ];
    let mut setup_audit = audit_event(230);
    setup_audit.actor_id = Some(admin_role);
    storage.commit_catalog_and_audit(id(230), admin_role, 9_999, &admin_records, &[setup_audit])?;
    let audit_history = (1..=129)
        .map(|event_number| AuditEvent {
            event_id: [event_number; 16],
            actor_id: Some(admin_id),
            correlation_id: [event_number + 1; 16],
            occurred_at_us: 1_700_000_000_000_000,
            action: AuditAction::Ddl,
            result: AuditResult::Succeeded,
            target_ids: vec![[0xfe; 16]],
        })
        .collect::<Vec<_>>();
    storage.commit_catalog_and_audit(id(231), admin_id, 9_999, &[], &audit_history)?;
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;
    let mut cancellation =
        storage.begin_invocation(cancel_id, ordinary_id, retention_deadline_us, 2, 64)?;
    let mut retained_invocations = Vec::with_capacity(129);
    for invocation_number in 1..=129 {
        retained_invocations.push(storage.begin_invocation(
            id(invocation_number),
            ordinary_id,
            retention_deadline_us,
            2,
            64,
        )?);
    }
    let stop_pressure = std::sync::atomic::AtomicBool::new(false);
    let denied_runtime_requests = std::sync::atomic::AtomicUsize::new(0);
    let denied_audit_requests = std::sync::atomic::AtomicUsize::new(0);
    let own_status_requests = std::sync::atomic::AtomicUsize::new(0);
    let owner_point_status_requests = std::sync::atomic::AtomicUsize::new(0);
    let cross_owner_status_requests = std::sync::atomic::AtomicUsize::new(0);
    let admin_point_status_requests = std::sync::atomic::AtomicUsize::new(0);
    let admin_runtime_requests = std::sync::atomic::AtomicUsize::new(0);
    let admin_audit_requests = std::sync::atomic::AtomicUsize::new(0);

    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let pressure_storage = storage.clone();
        let pressure_stop = &stop_pressure;
        let runtime_count = &denied_runtime_requests;
        let audit_count = &denied_audit_requests;
        let own_count = &own_status_requests;
        let owner_point_count = &owner_point_status_requests;
        let cross_owner_count = &cross_owner_status_requests;
        let admin_point_count = &admin_point_status_requests;
        let admin_runtime_count = &admin_runtime_requests;
        let admin_audit_count = &admin_audit_requests;
        let pressure = scope.spawn(move || {
            let mut signaled_started = false;
            loop {
                assert!(matches!(
                    pressure_storage.list_runtime_invocations(ordinary_id, now_us, 1, None),
                    Err(prdb_engine::storage_manager::AdministrativeInvocationListError::Unauthorized)
                ));
                assert!(matches!(
                    pressure_storage.list_audit_events(ordinary_id, 1, None),
                    Err(prdb_engine::storage_manager::AdministrativeAuditListError::Unauthorized)
                ));
                let runtime_total =
                    runtime_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                let audit_total = audit_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                let own_page = pressure_storage
                    .list_own_invocations(
                        ordinary_id,
                        now_us,
                        prdb_engine::storage_manager::MAX_OWN_INVOCATION_PAGE,
                        None,
                    )
                    .expect("owner status page remains available under pressure");
                assert_eq!(
                    own_page.invocations.len(),
                    prdb_engine::storage_manager::MAX_OWN_INVOCATION_PAGE
                );
                assert!(own_page.continuation.is_some());
                own_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let owner_status = pressure_storage
                    .get_invocation_status(cancel_id, ordinary_id, now_us)
                    .expect("owner point status remains available under pressure")
                    .expect("live or retained invocation status remains available");
                assert!(matches!(
                    owner_status.status,
                    prdb_engine::storage_manager::InvocationStatus::Active { .. }
                        | prdb_engine::storage_manager::InvocationStatus::Committing { .. }
                        | prdb_engine::storage_manager::InvocationStatus::Terminal(_)
                ));
                owner_point_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(pressure_storage
                    .get_invocation_status(cancel_id, admin_id, now_us)
                    .expect("cross-owner status denial remains available under pressure")
                    .is_none());
                cross_owner_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let admin_status = pressure_storage
                    .get_administrative_invocation_status(admin_id, cancel_id, now_us)
                    .expect("authorized administrator point status remains available under pressure")
                    .expect("administrator can inspect the live or retained invocation");
                assert!(matches!(
                    admin_status.status,
                    prdb_engine::storage_manager::InvocationStatus::Active { .. }
                        | prdb_engine::storage_manager::InvocationStatus::Committing { .. }
                        | prdb_engine::storage_manager::InvocationStatus::Terminal(_)
                ));
                admin_point_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let runtime_page = pressure_storage
                    .list_runtime_invocations(
                        admin_id,
                        now_us,
                        prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
                        None,
                    )
                    .expect("authorized runtime page remains available under pressure");
                assert_eq!(
                    runtime_page.invocations.len(),
                    prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE
                );
                assert!(runtime_page.continuation.is_some());
                admin_runtime_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let audit_page = pressure_storage
                    .list_audit_events(
                        admin_id,
                        prdb_engine::storage_manager::MAX_ADMIN_AUDIT_PAGE,
                        None,
                    )
                    .expect("authorized audit page remains available under pressure");
                assert_eq!(
                    audit_page.events.len(),
                    prdb_engine::storage_manager::MAX_ADMIN_AUDIT_PAGE
                );
                assert!(audit_page.continuation.is_some());
                admin_audit_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if !signaled_started {
                    started_tx
                        .send(())
                        .expect("signal monitoring pressure started");
                    signaled_started = true;
                }
                if pressure_stop.load(std::sync::atomic::Ordering::Acquire)
                    && runtime_total >= 512
                    && audit_total >= 512
                    && own_count.load(std::sync::atomic::Ordering::Relaxed) >= 512
                    && owner_point_count.load(std::sync::atomic::Ordering::Relaxed) >= 512
                    && cross_owner_count.load(std::sync::atomic::Ordering::Relaxed) >= 512
                    && admin_point_count.load(std::sync::atomic::Ordering::Relaxed) >= 512
                    && admin_runtime_count.load(std::sync::atomic::Ordering::Relaxed) >= 512
                    && admin_audit_count.load(std::sync::atomic::Ordering::Relaxed) >= 512
                {
                    break;
                }
                std::thread::yield_now();
            }
        });
        started_rx.recv_timeout(std::time::Duration::from_secs(2))?;

        storage
            .begin_invocation(commit_id, admin_id, retention_deadline_us, 2, 64)?
            .commit()?;
        assert_eq!(
            storage.cancel_owned_invocation(ordinary_id, cancel_id, now_us + 1)?,
            prdb_engine::storage_manager::OwnerCancellationResult::Accepted
        );
        assert!(
            cancellation
                .open_executor_metadata_connection()
                .check_cancellation()?
        );
        stop_pressure.store(true, std::sync::atomic::Ordering::Release);
        pressure
            .join()
            .map_err(|_| std::io::Error::other("denial pressure thread panicked"))?;
        Ok(())
    })?;

    assert!(denied_runtime_requests.load(std::sync::atomic::Ordering::Relaxed) >= 512);
    assert!(denied_audit_requests.load(std::sync::atomic::Ordering::Relaxed) >= 512);
    assert!(own_status_requests.load(std::sync::atomic::Ordering::Relaxed) >= 512);
    assert!(owner_point_status_requests.load(std::sync::atomic::Ordering::Relaxed) >= 512);
    assert!(cross_owner_status_requests.load(std::sync::atomic::Ordering::Relaxed) >= 512);
    assert!(admin_point_status_requests.load(std::sync::atomic::Ordering::Relaxed) >= 512);
    assert!(admin_runtime_requests.load(std::sync::atomic::Ordering::Relaxed) >= 512);
    assert!(admin_audit_requests.load(std::sync::atomic::Ordering::Relaxed) >= 512);
    assert!(matches!(
        storage.lookup_invocation_outcome(cancel_id, ordinary_id, now_us + 2)?,
        Some(outcome) if outcome.status == OutcomeStatus::Aborted
    ));
    Ok(())
}

pub(super) fn storage_manager_audits_cancellation_before_commit_and_preserves_terminal_outcomes()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::audit::{AuditEvent, AuditResult};
    use prdb_engine::storage_manager::{
        AdministrativeCancellationError, AdministrativeCancellationResult, InvocationStatus,
    };
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_active_analytical_invocations: 1,
            max_invocation_age: Duration::from_secs(15 * 60),
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_authorized_index_range_candidates: 128,
            max_analytical_authorized_index_range_candidates: None,
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_analytical_authorized_index_range_bytes: None,
            max_analytical_invocation_scan_work: None,
            max_planner_estimate_requests:
                prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_analytical_planner_estimate_requests: None,
            max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
            max_analytical_planner_estimate_candidates: None,
            max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS
                as usize,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 4,
        },
    )?;
    let admin_id = id(240);
    let owner_id = id(241);
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(admin_id, 1, 1, "cancel_admin"),
        object_write_with_name(admin_id, 8, "cancel_admin"),
        principal_write_with_name(owner_id, 1, 1, "cancel_owner"),
        object_write_with_name(owner_id, 8, "cancel_owner"),
        membership_write_with_grantor(admin_id, admin_role, admin_role),
        dependency_write(admin_id, admin_role, 5),
        credential_verifier_write(id(242), admin_id, &[0x6b; 32], 1, i64::MAX, None, 1),
    ];
    let mut setup_audit = audit_event(238);
    setup_audit.actor_id = Some(admin_role);
    storage.commit_catalog_and_audit(id(238), admin_role, 9_999, &records, &[setup_audit])?;

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;
    let cancellation_audit = |event_id, actor_id, target_id| AuditEvent {
        event_id: id(event_id),
        actor_id: Some(actor_id),
        correlation_id: id(event_id.wrapping_add(1)),
        occurred_at_us: now_us,
        action: AuditAction::AdministrativeCancellation,
        result: AuditResult::Denied,
        target_ids: vec![target_id],
    };

    let mut pending = storage.begin_invocation(id(243), owner_id, retention_deadline_us, 2, 64)?;
    {
        use prdb_engine::ipc::{Operation, ServiceRole};
        let request = prdb_engine::ipc::IpcFrame {
            operation: Operation::ExecutorCheckCancellation as u16,
            request_id: 1,
            payload: Vec::new(),
        };
        let mut executor = pending.open_executor_metadata_connection();
        let response = prdb_engine::executor_ipc::dispatch_executor_request(
            ServiceRole::Executor,
            &mut executor,
            &request,
        )?;
        assert_eq!(response.payload, [0, 1, 0]);
    }
    let cancelled_row_key = table_row_key(id(250), id(251));
    pending.stage_put(
        prdb_engine::overlay::RowId {
            table: id(250),
            row: id(251),
        },
        b"must be discarded",
    )?;
    assert!(matches!(
        storage
            .clone()
            .begin_invocation(id(252), owner_id, retention_deadline_us, 2, 64),
        Err(TransactionError::AdmissionLimitExceeded)
    ));
    assert!(matches!(
        storage.cancel_invocation(
            owner_id,
            id(243),
            7,
            now_us,
            cancellation_audit(244, owner_id, id(243)),
        ),
        Err(AdministrativeCancellationError::Unauthorized)
    ));
    assert!(matches!(
        storage
            .get_invocation_status(id(243), owner_id, now_us)?
            .unwrap()
            .status,
        InvocationStatus::Active {
            cancellation_requested: false,
            ..
        }
    ));
    assert_eq!(
        storage.cancel_invocation(
            admin_id,
            id(243),
            7,
            now_us,
            cancellation_audit(245, admin_id, id(243)),
        )?,
        AdministrativeCancellationResult::Accepted
    );
    assert_eq!(
        storage.cancel_invocation(
            admin_id,
            id(243),
            7,
            now_us,
            cancellation_audit(250, admin_id, id(243)),
        )?,
        AdministrativeCancellationResult::Accepted
    );
    let administrative_page = storage.list_runtime_invocations(admin_id, now_us, 1, None)?;
    assert_eq!(
        administrative_page.invocations[0].cancellation_reason_code,
        Some(7)
    );
    assert_eq!(administrative_page.admin_cancellations_accepted_total, 1);
    assert_eq!(administrative_page.admin_cancellations_denied_total, 0);
    assert!(matches!(
        storage
            .get_invocation_status(id(243), owner_id, now_us)?
            .unwrap()
            .status,
        InvocationStatus::Active {
            cancellation_requested: true,
            ..
        }
    ));
    {
        use prdb_engine::ipc::{Operation, ServiceRole};
        let request = prdb_engine::ipc::IpcFrame {
            operation: Operation::ExecutorCheckCancellation as u16,
            request_id: 2,
            payload: Vec::new(),
        };
        let mut executor = pending.open_executor_metadata_connection();
        let response = prdb_engine::executor_ipc::dispatch_executor_request(
            ServiceRole::Executor,
            &mut executor,
            &request,
        )?;
        assert_eq!(response.payload, [0, 1, 1]);
    }
    assert_eq!(
        lookup_outcome(&database, id(243), owner_id, now_us)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert!(row(&database, &cancelled_row_key)?.is_none());
    let mut after_cancel =
        storage.begin_invocation(id(253), owner_id, retention_deadline_us, 2, 64)?;
    after_cancel.abort()?;
    let audit_read = database.begin_read()?;
    let audit_table = audit_read.open_table(schema::AUDIT)?;
    let audit = audit_table
        .get(id(245).as_slice())?
        .ok_or("missing accepted cancellation audit")?;
    assert_eq!(
        AuditEvent::decode(audit.value())?.result,
        AuditResult::Accepted
    );
    drop(audit);
    drop(audit_table);
    drop(audit_read);

    let committed = storage.begin_invocation(id(246), owner_id, retention_deadline_us, 2, 64)?;
    let commit_sequence = committed.commit()?;
    assert!(matches!(
        storage.cancel_invocation(
            admin_id,
            id(246),
            8,
            now_us,
            cancellation_audit(247, admin_id, id(246)),
        )?,
        AdministrativeCancellationResult::Terminal(InvocationOutcome {
            status: OutcomeStatus::Committed,
            commit_sequence: committed_sequence,
            ..
        }) if committed_sequence == commit_sequence
    ));
    assert_eq!(
        storage
            .list_runtime_invocations(admin_id, now_us, 1, None)?
            .admin_cancellations_denied_total,
        1
    );
    assert!(matches!(
        storage.cancel_invocation(
            admin_id,
            id(248),
            9,
            now_us,
            cancellation_audit(249, admin_id, id(248)),
        ),
        Ok(AdministrativeCancellationResult::Unknown)
    ));
    assert_eq!(
        storage
            .list_runtime_invocations(admin_id, now_us, 1, None)?
            .admin_cancellations_denied_total,
        2
    );
    assert!(matches!(
        storage.cancel_invocation(
            admin_id,
            id(248),
            9,
            now_us,
            cancellation_audit(251, admin_id, id(248)),
        ),
        Err(AdministrativeCancellationError::RateLimited)
    ));
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn admin_cancellation_sync_failure_quarantines_until_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    struct AcceptMetadata;
    impl prdb_engine::supervisor::SubmissionMetadataValidator for AcceptMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &prdb_engine::supervisor::SubmissionMetadata) -> bool {
            true
        }
    }

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_ADMIN_CANCEL_SYNC_FAILURE_DATABASE";
    const ADMIN_ID: [u8; 16] = [0xf0; 16];
    const OWNER_ID: [u8; 16] = [0xf1; 16];
    const INVOCATION_ID: [u8; 16] = [0xf2; 16];
    const AUDIT_ID: [u8; 16] = [0xf3; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let fail_next_sync = Arc::new(AtomicBool::new(false));
        let database = Database::builder().create_with_backend(FailNextSyncBackend {
            inner: redb::backends::FileBackend::new(file)?,
            fail_next_sync: fail_next_sync.clone(),
        })?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let _pending = storage.begin_invocation(INVOCATION_ID, OWNER_ID, i64::MAX, 1, 64)?;
        let now_us = 1_700_000_000_000_000;
        let cancellation_audit = AuditEvent {
            event_id: AUDIT_ID,
            actor_id: Some(ADMIN_ID),
            correlation_id: [0xf4; 16],
            occurred_at_us: now_us,
            action: AuditAction::AdministrativeCancellation,
            result: AuditResult::Denied,
            target_ids: vec![INVOCATION_ID],
        };

        fail_next_sync.store(true, Ordering::SeqCst);
        assert!(matches!(
            storage.cancel_invocation(ADMIN_ID, INVOCATION_ID, 7, now_us, cancellation_audit,),
            Err(AdministrativeCancellationError::Storage)
        ));
        assert!(!fail_next_sync.load(Ordering::SeqCst));
        assert!(storage.requires_reopen_after_commit_error());
        assert!(matches!(
            storage.begin_invocation([0xf5; 16], OWNER_ID, i64::MAX, 1, 64),
            Err(prdb_engine::transaction::TransactionError::Storage(_))
        ));
        assert!(matches!(
            storage.get_invocation_status(INVOCATION_ID, OWNER_ID, now_us),
            Err(prdb_engine::storage_manager::InvocationStatusError::Storage)
        ));
        let socket_path =
            std::path::PathBuf::from(format!("{}.quarantine.sock", database_path.display()));
        let listener = prdb_engine::supervisor_ipc::bind_gateway_service_socket(&socket_path)?;
        let staging = prdb_engine::supervisor::StagingStore::new(
            prdb_engine::supervisor::StagingLimits::default(),
            Arc::new(AcceptMetadata),
        )?;
        let runtime = prdb_engine::supervisor_ipc::GatewayServiceRuntimeConfig {
            admission: prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
                retention_period_us: i64::MAX,
                max_rows: 1,
                max_bytes: 64,
            },
            executor: prdb_engine::supervisor_ipc::GatewayExecutorConfig {
                cgroup_parent: std::path::Path::new(&database_path)
                    .parent()
                    .ok_or("database fixture has no parent")?
                    .to_path_buf(),
                resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
                    cpu_quota_micros: 100_000,
                    cpu_period_micros: 100_000,
                    memory_max_bytes: 64 * 1024 * 1024,
                    max_processes: 16,
                },
                analytical_resource_limits: None,
                launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
                    executor_path: std::env::current_exe()?,
                    optimizer_path: std::env::current_exe()?,
                    scratch_parent: std::env::temp_dir(),
                    runtime_read_execute: Vec::new(),
                    runtime_read_only: Vec::new(),
                    spill_broker: None,
                    max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
                    analytical_max_total_result_bytes: None,
                    max_sql_index_probe_candidates:
                        prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
                    analytical_max_sql_index_probe_candidates: None,
                    cleanup_timeout: Duration::from_millis(100),
                },
            },
            listener: prdb_engine::supervisor_ipc::GatewayServiceListenerConfig {
                expected_gateway_uid: unsafe { libc::geteuid() },
                max_connections: 1,
                idle_timeout: Duration::from_secs(1),
                accept_poll_interval: Duration::from_millis(1),
                outcome_maintenance_interval: Duration::from_secs(60),
                outcome_maintenance_batch: prdb_engine::outcome::MAX_OUTCOME_PRUNE_BATCH,
                statistics_maintenance_interval: Duration::from_secs(60),
            },
        };
        assert!(matches!(
            prdb_engine::supervisor_ipc::serve_gateway_service_listener(
                listener,
                &storage,
                &staging,
                runtime,
                Arc::new(AtomicBool::new(false)),
                |_manager, _request_id, _stream, _invocation, _submission, _executor| {},
            ),
            Err(prdb_engine::supervisor_ipc::GatewayServiceListenerError::StorageReopenRequired)
        ));
        std::fs::remove_file(socket_path)?;
        // Let process teardown close redb and discard its process-local quarantine.
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    let database = initialize(&database_path)?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(ADMIN_ID, 1, 1, "sync_admin"),
        object_write_with_name(ADMIN_ID, 8, "sync_admin"),
        principal_write_with_name(OWNER_ID, 1, 1, "sync_owner"),
        object_write_with_name(OWNER_ID, 8, "sync_owner"),
        membership_write_with_grantor(ADMIN_ID, admin_role, admin_role),
        dependency_write(ADMIN_ID, admin_role, 5),
        credential_verifier_write([0xf6; 16], ADMIN_ID, &[0x71; 32], 1, i64::MAX, None, 1),
    ];
    let mut setup_audit = audit_event(0xf7);
    setup_audit.actor_id = Some(admin_role);
    storage.commit_catalog_and_audit(id(0xf7), admin_role, i64::MAX, &records, &[setup_audit])?;
    drop(storage);
    drop(database);

    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("admin_cancellation_sync_failure_quarantines_until_reopen")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .status()?;
    assert!(
        child_status.success(),
        "cancellation fault-injection child exited with {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    assert!(!storage.requires_reopen_after_commit_error());
    let mut resumed = storage.begin_invocation([0xf8; 16], OWNER_ID, i64::MAX, 1, 64)?;
    resumed.abort()?;
    let read = reopened.begin_read()?;
    let audit_table = read.open_table(AUDIT)?;
    if let Some(audit) = audit_table.get(&AUDIT_ID[..])? {
        assert_eq!(
            AuditEvent::decode(audit.value())?.result,
            AuditResult::Accepted
        );
    }
    Ok(())
}

pub(super) fn schema_create_sync_failure_recovers_atomic_catalog_audit_and_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_SCHEMA_CREATE_SYNC_FAILURE_DATABASE";
    const ACTOR_ID: [u8; 16] = [0xfa; 16];
    const SCHEMA_INVOCATION_ID: [u8; 16] = [0xfb; 16];
    const SETUP_INVOCATION_ID: [u8; 16] = [0xfc; 16];
    const SETUP_AUDIT_CODE: u8 = 0xfd;
    const SCHEMA_AUDIT_CODE: u8 = 0xfe;
    const SCHEMA_AUDIT_ID: [u8; 16] = [SCHEMA_AUDIT_CODE; 16];
    const SCHEMA_NAME: &str = "sync_failure_schema";

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let fail_next_sync = Arc::new(AtomicBool::new(false));
        let database = Database::builder().create_with_backend(FailNextSyncBackend {
            inner: redb::backends::FileBackend::new(file)?,
            fail_next_sync: fail_next_sync.clone(),
        })?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let mut audit = identity_audit_event(SCHEMA_AUDIT_CODE, ACTOR_ID, ACTOR_ID);
        audit.correlation_id = SCHEMA_INVOCATION_ID;
        fail_next_sync.store(true, Ordering::SeqCst);
        assert!(
            storage
                .create_schema(
                    SCHEMA_INVOCATION_ID,
                    ACTOR_ID,
                    SCHEMA_NAME,
                    ACTOR_ID,
                    9_999,
                    audit,
                )
                .is_err()
        );
        assert!(!fail_next_sync.load(Ordering::SeqCst));
        assert!(storage.requires_reopen_after_commit_error());
        assert!(
            storage
                .create_schema(
                    [0x11; 16],
                    ACTOR_ID,
                    "must_not_publish_while_quarantined",
                    ACTOR_ID,
                    9_999,
                    identity_audit_event(0x12, ACTOR_ID, ACTOR_ID),
                )
                .is_err()
        );
        assert!(storage.requires_reopen_after_commit_error());
        drop(storage);
        drop(database);
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    let database = initialize(&database_path)?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor_name = "sync_schema_actor";
    let setup = [
        principal_write_with_name(ACTOR_ID, 1, 1, actor_name),
        object_write_with_name(ACTOR_ID, 8, "sync_schema_actor"),
        grant_write(ACTOR_ID, id(0x33), 14, 1, id(0x33), admin_role),
        dependency_write(ACTOR_ID, id(0x33), 5),
    ];
    let mut setup_audit = identity_audit_event(SETUP_AUDIT_CODE, admin_role, ACTOR_ID);
    setup_audit.correlation_id = SETUP_INVOCATION_ID;
    storage.commit_catalog_and_audit(
        SETUP_INVOCATION_ID,
        admin_role,
        9_999,
        &setup,
        &[setup_audit],
    )?;
    drop(storage);
    drop(database);

    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("schema_create_sync_failure_recovers_atomic_catalog_audit_and_outcome")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .status()?;
    assert!(
        child_status.success(),
        "schema DDL sync-failure child exited with {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let recovered_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    assert!(!recovered_storage.requires_reopen_after_commit_error());
    assert!(prdb_engine::startup::validate_persisted_records(&reopened).is_ok());
    let read = reopened.begin_read()?;
    let schema_id = lookup_schema_id_by_name(&read, SCHEMA_NAME)?;
    let schema_record = read
        .open_table(CATALOG)?
        .get(&id_catalog_key(3, schema_id.unwrap_or([0; 16]))[..])?
        .is_some();
    let object_record = read
        .open_table(CATALOG)?
        .get(&id_catalog_key(2, schema_id.unwrap_or([0; 16]))[..])?
        .is_some();
    let outcome = lookup_outcome(&reopened, SCHEMA_INVOCATION_ID, ACTOR_ID, 1)?;
    let audit = read
        .open_table(AUDIT)?
        .get(&SCHEMA_AUDIT_ID[..])?
        .map(|record| AuditEvent::decode(record.value()))
        .transpose()?;
    let metadata = read
        .open_table(ENGINE_META)?
        .get(&&b"format"[..])?
        .ok_or("missing metadata after schema-create recovery")?;
    let sequence = EngineMetadata::decode(metadata.value())?.commit_sequence;
    match (
        schema_id,
        schema_record,
        object_record,
        outcome,
        audit,
        sequence,
    ) {
        (Some(_), true, true, Some(outcome), Some(_), 2) => {
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, 2);
        }
        (None, false, false, None, None, 1) => {}
        (schema_id, schema_record, object_record, outcome, audit, sequence) => {
            return Err(format!(
                "schema-create sync recovery exposed a partial publication: schema_id={schema_id:?}, schema_record={schema_record}, object_record={object_record}, outcome={outcome:?}, audit={audit:?}, sequence={sequence}"
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn identity_drop_sync_failure_recovers_tombstone_and_token_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_IDENTITY_DROP_SYNC_FAILURE_DATABASE";
    const ACTOR_ID: [u8; 16] = [0xe0; 16];
    const TARGET_ID: [u8; 16] = [0xe1; 16];
    const ACTOR_TOKEN_ID: [u8; 16] = [0xe2; 16];
    const TARGET_TOKEN_ID: [u8; 16] = [0xe3; 16];
    const SETUP_INVOCATION_ID: [u8; 16] = [0xe4; 16];
    const DROP_INVOCATION_ID: [u8; 16] = [0xe5; 16];
    const DROP_AUDIT_ID: [u8; 16] = [0xe6; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let fail_next_sync = Arc::new(AtomicBool::new(false));
        let database = Database::builder().create_with_backend(FailNextSyncBackend {
            inner: redb::backends::FileBackend::new(file)?,
            fail_next_sync: fail_next_sync.clone(),
        })?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let mut audit = identity_audit_event(DROP_AUDIT_ID[0], ACTOR_ID, TARGET_ID);
        audit.correlation_id = DROP_INVOCATION_ID;
        fail_next_sync.store(true, Ordering::SeqCst);
        assert!(
            storage
                .drop_identity(DROP_INVOCATION_ID, ACTOR_ID, TARGET_ID, i64::MAX, audit,)
                .is_err()
        );
        assert!(!fail_next_sync.load(Ordering::SeqCst));
        assert!(storage.requires_reopen_after_commit_error());
        drop(storage);
        drop(database);
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    let database = initialize(&database_path)?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(SETUP_INVOCATION_ID[0], admin_role, ACTOR_ID);
    setup_audit.action = AuditAction::AuthorityChange;
    setup_audit.correlation_id = SETUP_INVOCATION_ID;
    storage.commit_catalog_and_audit(
        SETUP_INVOCATION_ID,
        admin_role,
        i64::MAX,
        &[
            principal_write_with_name(ACTOR_ID, 1, 1, "sync_drop_actor"),
            object_write_with_name(ACTOR_ID, 8, "sync_drop_actor"),
            principal_write_with_name(TARGET_ID, 1, 1, "sync_drop_target"),
            object_write_with_parent_and_owner_and_name(
                TARGET_ID,
                8,
                None,
                ACTOR_ID,
                "sync_drop_target",
            ),
            membership_write_with_grantor(ACTOR_ID, admin_role, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
            credential_verifier_write(ACTOR_TOKEN_ID, ACTOR_ID, &[0xe2; 32], 1, i64::MAX, None, 1),
            credential_verifier_write(
                TARGET_TOKEN_ID,
                TARGET_ID,
                &[0xe3; 32],
                1,
                i64::MAX,
                None,
                1,
            ),
        ],
        &[setup_audit],
    )?;
    drop(storage);
    drop(database);

    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("identity_drop_sync_failure_recovers_tombstone_and_token_atomically")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .status()?;
    assert!(
        child_status.success(),
        "identity-drop sync-failure child exited with {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let recovered_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    assert!(!recovered_storage.requires_reopen_after_commit_error());
    prdb_engine::startup::validate_persisted_records(&reopened)?;
    let read = reopened.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let target_object = catalog
        .get(id_catalog_key(2, TARGET_ID).as_slice())?
        .map(|value| value.value().to_vec());
    let target_principal = catalog
        .get(id_catalog_key(10, TARGET_ID).as_slice())?
        .map(|value| value.value().to_vec());
    let tombstone = catalog
        .get(id_catalog_key(17, TARGET_ID).as_slice())?
        .map(|value| value.value().to_vec());
    let target_token = catalog
        .get(id_catalog_key(16, TARGET_TOKEN_ID).as_slice())?
        .map(|value| value.value().to_vec());
    let audit = read
        .open_table(AUDIT)?
        .get(&DROP_AUDIT_ID[..])?
        .map(|value| AuditEvent::decode(value.value()))
        .transpose()?;
    let outcome = lookup_outcome(&reopened, DROP_INVOCATION_ID, ACTOR_ID, 1)?;
    let sequence = commit_sequence(&reopened)?;
    match (
        target_object,
        target_principal,
        tombstone,
        target_token,
        audit,
        outcome,
        sequence,
    ) {
        (None, None, Some(_), Some(token), Some(_), Some(outcome), 2) => {
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            let token_record = decode_registered_envelope(&token)?;
            assert!(
                token_record
                    .fields
                    .iter()
                    .any(|field| field.tag == 7 && field.payload == [1])
            );
        }
        (Some(_), Some(_), None, Some(_), None, None, 1) => {}
        (object, principal, tombstone, token, audit, outcome, sequence) => {
            return Err(format!(
                "identity-drop recovery exposed partial publication: object={}, principal={}, tombstone={}, token={}, audit={audit:?}, outcome={outcome:?}, sequence={sequence}",
                object.is_some(), principal.is_some(), tombstone.is_some(), token.is_some()
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn role_drop_sync_failure_recovers_tombstone_audit_and_outcome_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_ROLE_DROP_SYNC_FAILURE_DATABASE";
    const ACTOR_ID: [u8; 16] = [0xe8; 16];
    const TARGET_ID: [u8; 16] = [0xe9; 16];
    const SETUP_INVOCATION_ID: [u8; 16] = [0xea; 16];
    const DROP_INVOCATION_ID: [u8; 16] = [0xeb; 16];
    const DROP_AUDIT_ID: [u8; 16] = [0xec; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let fail_next_sync = Arc::new(AtomicBool::new(false));
        let database = Database::builder().create_with_backend(FailNextSyncBackend {
            inner: redb::backends::FileBackend::new(file)?,
            fail_next_sync: fail_next_sync.clone(),
        })?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let mut audit = identity_audit_event(DROP_AUDIT_ID[0], ACTOR_ID, TARGET_ID);
        audit.correlation_id = DROP_INVOCATION_ID;
        fail_next_sync.store(true, Ordering::SeqCst);
        assert!(
            storage
                .drop_identity(DROP_INVOCATION_ID, ACTOR_ID, TARGET_ID, i64::MAX, audit)
                .is_err()
        );
        assert!(!fail_next_sync.load(Ordering::SeqCst));
        assert!(storage.requires_reopen_after_commit_error());
        drop(storage);
        drop(database);
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    let database = initialize(&database_path)?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(SETUP_INVOCATION_ID[0], admin_role, ACTOR_ID);
    setup_audit.action = AuditAction::AuthorityChange;
    setup_audit.correlation_id = SETUP_INVOCATION_ID;
    storage.commit_catalog_and_audit(
        SETUP_INVOCATION_ID,
        admin_role,
        i64::MAX,
        &[
            principal_write_with_name(ACTOR_ID, 1, 1, "sync_role_drop_actor"),
            object_write_with_name(ACTOR_ID, 8, "sync_role_drop_actor"),
            principal_write_with_name(TARGET_ID, 2, 1, "sync_role_drop_target"),
            object_write_with_parent_and_owner_and_name(
                TARGET_ID,
                9,
                None,
                ACTOR_ID,
                "sync_role_drop_target",
            ),
            membership_write_with_grantor(ACTOR_ID, admin_role, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    drop(storage);
    drop(database);

    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("role_drop_sync_failure_recovers_tombstone_audit_and_outcome_atomically")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .status()?;
    assert!(
        child_status.success(),
        "role-drop sync-failure child exited with {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let recovered_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    assert!(!recovered_storage.requires_reopen_after_commit_error());
    prdb_engine::startup::validate_persisted_records(&reopened)?;
    let read = reopened.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let target_object = catalog
        .get(id_catalog_key(2, TARGET_ID).as_slice())?
        .map(|value| value.value().to_vec());
    let target_principal = catalog
        .get(id_catalog_key(10, TARGET_ID).as_slice())?
        .map(|value| value.value().to_vec());
    let tombstone = catalog
        .get(id_catalog_key(17, TARGET_ID).as_slice())?
        .map(|value| value.value().to_vec());
    let audit = read
        .open_table(AUDIT)?
        .get(&DROP_AUDIT_ID[..])?
        .map(|value| AuditEvent::decode(value.value()))
        .transpose()?;
    let outcome = lookup_outcome(&reopened, DROP_INVOCATION_ID, ACTOR_ID, 1)?;
    let sequence = commit_sequence(&reopened)?;
    match (
        target_object,
        target_principal,
        tombstone,
        audit,
        outcome,
        sequence,
    ) {
        (None, None, Some(_), Some(_), Some(outcome), 2) => {
            assert_eq!(outcome.status, OutcomeStatus::Committed);
        }
        (Some(_), Some(_), None, None, None, 1) => {}
        (object, principal, tombstone, audit, outcome, sequence) => {
            return Err(format!(
                "role-drop recovery exposed partial publication: object={}, principal={}, tombstone={}, audit={audit:?}, outcome={outcome:?}, sequence={sequence}",
                object.is_some(), principal.is_some(), tombstone.is_some()
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn storage_manager_rejects_invalid_recovered_images_before_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&key(195)[..], &b"not a row record"[..])?;
    write.commit()?;

    assert!(matches!(
        StorageManager::new(&database, Duration::from_millis(100)),
        Err(StorageManagerInitError::Startup(
            prdb_engine::startup::StartupError::InvalidRow
        ))
    ));
    Ok(())
}

pub(super) fn startup_rejects_missing_bootstrap_descriptor_before_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let metadata = database
        .begin_read()?
        .open_table(ENGINE_META)?
        .get(&b"format"[..])?
        .ok_or("missing engine metadata")?
        .value()
        .to_vec();
    let database_id = EngineMetadata::decode(&metadata)?.database_id;
    let descriptor_key = id_catalog_key(1, database_id);
    let write = database.begin_write()?;
    let mut catalog = write.open_table(CATALOG)?;
    assert!(catalog.remove(descriptor_key.as_slice())?.is_some());
    drop(catalog);
    write.commit()?;

    assert_eq!(
        prdb_engine::startup::validate_persisted_records(&database),
        Err(prdb_engine::startup::StartupError::InvalidCatalog)
    );
    assert!(matches!(
        StorageManager::new(&database, Duration::from_millis(100)),
        Err(StorageManagerInitError::Startup(
            prdb_engine::startup::StartupError::InvalidCatalog
        ))
    ));
    Ok(())
}

pub(super) fn startup_rejects_principal_without_matching_identity_object()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let principal_id = id(0xd1);
    let principal = principal_write_with_name(principal_id, 1, 1, "orphan_principal");
    let object = object_write_with_name(principal_id, 8, "orphan_principal");
    let administrator = builtin_owner_id();
    let invocation_id = id(0xd2);
    let mut audit = identity_audit_event(0xd3, administrator, principal_id);
    audit.action = AuditAction::AuthorityChange;
    audit.correlation_id = invocation_id;
    commit_catalog_and_audit(
        &database,
        invocation_id,
        administrator,
        9_999,
        &[principal, object.clone()],
        &[audit],
    )?;

    let write = database.begin_write()?;
    let mut catalog = write.open_table(CATALOG)?;
    assert!(catalog.remove(object.key.as_slice())?.is_some());
    drop(catalog);
    write.commit()?;

    assert_eq!(
        prdb_engine::startup::validate_persisted_records(&database),
        Err(prdb_engine::startup::StartupError::InvalidCatalog)
    );
    assert!(matches!(
        StorageManager::new(&database, Duration::from_millis(100)),
        Err(StorageManagerInitError::Startup(
            prdb_engine::startup::StartupError::InvalidCatalog
        ))
    ));
    Ok(())
}

pub(super) fn catalog_commit_requires_backing_index_dependency_edges()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(90),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, false),
            &[audit_event(90)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(91),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, true),
            &[audit_event(91)],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn catalog_commit_rejects_not_null_when_an_existing_row_is_null()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let schema_id = id(80);
    let table_id = id(82);
    let column_id = id(84);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(0xa1),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, true),
            &[audit_event(0xa1)],
        )?,
        1
    );
    let parent_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"present".to_vec()))],
    )?;
    let parent_key = table_row_key(id(81), id(0xa9));
    let parent_index_key = prdb_engine::index_key::derive_index_key(
        id(87),
        id(0xa9),
        1,
        &[id(83)],
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        }],
        &parent_tuple,
        &[id(83)],
    )?
    .key;
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(0xa2),
            id(9),
            9_999,
            &[RowWrite {
                key: parent_key,
                expected_version: None,
                payload: Some(parent_tuple),
            }],
            &[IndexWrite {
                key: parent_index_key,
                row_key: parent_key,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: false,
            }],
        )?,
        2
    );
    let null_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[None],
    )?;
    assert_eq!(
        commit_rows(
            &database,
            id(0xa8),
            id(9),
            9_999,
            &[RowWrite {
                key: table_row_key(table_id, id(0xa3)),
                expected_version: None,
                payload: Some(null_tuple),
            }],
        )?,
        3
    );

    let row_key = table_row_key(table_id, id(0xa3));
    let mut column = column_write_not_null(column_id, table_id, "object_84");
    column.expected_value = column_write(column_id, table_id, "object_84").payload;
    let mut table = table_write_with_versions(table_id, schema_id, 1, 2);
    table.expected_value = table_write(table_id, schema_id).payload;
    let constraint_id = id(0xa4);
    let changes = [
        constraint_write(constraint_id, table_id, 1, &[column_id], None, &[], None),
        object_write(constraint_id, 6),
        column,
        table,
    ];
    assert_eq!(
        storage.commit_catalog_and_audit(id(0xa5), id(9), 9_999, &changes, &[audit_event(0xa5)],),
        Err(CommitError::NotNullViolation)
    );
    assert_eq!(commit_sequence(&database)?, 3);
    assert_eq!(
        lookup_outcome(&database, id(0xa5), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(0xa0),
            id(9),
            9_999,
            &changes,
            &[audit_event(0xa0)],
        ),
        Err(CommitError::NotNullViolation)
    );
    assert_eq!(commit_sequence(&database)?, 3);

    let non_null_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"present".to_vec()))],
    )?;
    assert_eq!(
        commit_rows(
            &database,
            id(0xa6),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(3),
                payload: Some(non_null_tuple),
            }],
        )?,
        4
    );
    assert_eq!(
        storage.commit_catalog_and_audit(id(0xa7), id(9), 9_999, &changes, &[audit_event(0xa7)],)?,
        5
    );
    assert_eq!(commit_sequence(&database)?, 5);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn catalog_commit_requires_expression_reference_dependency_edges()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::Expr;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let column_id = id(120);
    let default_expression_id = id(121);
    let field_expression_id = id(122);
    let add_column_default_id = id(128);
    let policy_id = id(126);
    let policy_expression_id = id(127);
    let policy_key = id_catalog_key(13, policy_id);
    let policy_payload = encode_envelope(
        &family_schema(13).unwrap(),
        &policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: id(123).to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let base_writes = vec![
        column_write_with_defaults(
            column_id,
            id(123),
            "object_120",
            Some(default_expression_id),
            Some(add_column_default_id),
        ),
        expression_write(default_expression_id, 1, &[], &Expr::CurrentPrincipalId),
        expression_write(field_expression_id, 2, &[column_id], &Expr::Field(0)),
        expression_write(add_column_default_id, 1, &[], &Expr::Literal(None)),
        expression_write(policy_expression_id, 3, &[], &Expr::CurrentPrincipalId),
        CatalogWrite {
            key: policy_key,
            expected_value: None,
            payload: Some(policy_payload),
        },
        object_write(column_id, 4),
        object_write(default_expression_id, 5),
        object_write(field_expression_id, 5),
        object_write(add_column_default_id, 5),
        object_write(policy_id, 10),
        object_write(policy_expression_id, 5),
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(124),
            id(9),
            9_999,
            &base_writes,
            &[audit_event(124)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let mut complete_writes = base_writes;
    complete_writes.push(dependency_write(column_id, default_expression_id, 2));
    complete_writes.push(dependency_write(column_id, add_column_default_id, 2));
    complete_writes.push(dependency_write(field_expression_id, column_id, 2));
    complete_writes.push(dependency_write(policy_id, policy_expression_id, 2));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(125),
            id(9),
            9_999,
            &complete_writes,
            &[audit_event(125)],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn catalog_commit_rejects_unknown_dependency_edge_kinds()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let mut dependency = dependency_write(id(32), id(33), 5);
    dependency.key[37..39].copy_from_slice(&6u16.to_be_bytes());
    dependency.payload.as_mut().unwrap()[32 + 37..32 + 39].copy_from_slice(&6u16.to_be_bytes());
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(64),
            id(9),
            9_999,
            &[object_write(id(32), 3), object_write(id(33), 4), dependency,],
            &[audit_event(64)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_requires_object_owner_principals()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let owner_id = id(71);
    let object = object_write_with_owner(id(70), 3, owner_id);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(66),
            id(9),
            9_999,
            std::slice::from_ref(&object),
            &[audit_event(66)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let principal = principal_write(owner_id, 1);
    let principal_object = principal_object_write(owner_id, 1);
    let table = table_write(id(70), builtin_id(1));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(67),
            id(9),
            9_999,
            &[
                object.clone(),
                table,
                principal.clone(),
                principal_object.clone(),
            ],
            &[audit_event(67)],
        )?,
        1
    );

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(68),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: principal.key,
                    expected_value: principal.payload,
                    payload: None,
                },
                CatalogWrite {
                    key: principal_object.key,
                    expected_value: principal_object.payload,
                    payload: None,
                },
            ],
            &[audit_event(68)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn catalog_commit_requires_index_object_identity_and_kind()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let (missing_key, missing_payload) = index_definition(id(55), id(16));
    let missing_index = CatalogWrite {
        key: missing_key,
        expected_value: None,
        payload: Some(missing_payload),
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(60),
            id(9),
            9_999,
            std::slice::from_ref(&missing_index),
            &[audit_event(60)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let (index_key, index_payload) = index_definition(id(56), id(16));
    let index = CatalogWrite {
        key: index_key,
        expected_value: None,
        payload: Some(index_payload),
    };
    let index_object = object_write(id(56), 7);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(61),
            id(9),
            9_999,
            &[index, index_object.clone()],
            &[audit_event(61)],
        )?,
        1
    );

    let delete_object = CatalogWrite {
        key: index_object.key.clone(),
        expected_value: index_object.payload,
        payload: None,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(62),
            id(9),
            9_999,
            &[delete_object],
            &[audit_event(62)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let (wrong_key, wrong_payload) = index_definition(id(57), id(16));
    let wrong_index = CatalogWrite {
        key: wrong_key,
        expected_value: None,
        payload: Some(wrong_payload),
    };
    let wrong_object = object_write(id(57), 6);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(63),
            id(9),
            9_999,
            &[wrong_index, wrong_object],
            &[audit_event(63)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn catalog_commit_rejects_indexes_with_missing_or_mistabled_columns()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;

    let missing_column_id = id(90);
    let (missing_key, missing_payload) =
        index_definition_with_columns(id(89), id(16), &[missing_column_id]);
    let missing_index = CatalogWrite {
        key: missing_key,
        expected_value: None,
        payload: Some(missing_payload),
    };
    let missing_index_object = object_write(id(89), 7);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(90),
            id(9),
            9_999,
            &[missing_index, missing_index_object],
            &[audit_event(90)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let other_table_id = id(92);
    let other_column_id = id(93);
    let (mismatched_key, mismatched_payload) =
        index_definition_with_columns(id(94), id(16), &[other_column_id]);
    let mismatched_index = CatalogWrite {
        key: mismatched_key,
        expected_value: None,
        payload: Some(mismatched_payload),
    };
    let other_column = column_write(other_column_id, other_table_id, "object_93");
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(91),
            id(9),
            9_999,
            &[
                mismatched_index,
                object_write(id(94), 7),
                object_write(other_table_id, 3),
                table_write(other_table_id, id(95)),
                object_write(other_column_id, 4),
                other_column,
            ],
            &[audit_event(91)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let actual_schema_id = id(0xa0);
    let wrong_schema_id = id(0xa1);
    let table_id = id(0xa2);
    let column_id = id(0xa3);
    let index_id = id(0xa4);
    let (index_key, index_payload) =
        index_definition_with_columns(index_id, table_id, &[column_id]);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(0xa5),
            id(9),
            9_999,
            &[
                object_write(actual_schema_id, 1),
                schema_write(actual_schema_id),
                object_write(wrong_schema_id, 1),
                schema_write(wrong_schema_id),
                object_write_with_parent(table_id, 3, Some(actual_schema_id)),
                table_write(table_id, actual_schema_id),
                dependency_write(table_id, actual_schema_id, 1),
                object_write(column_id, 4),
                column_write(column_id, table_id, "object_163"),
                object_write_with_parent(index_id, 7, Some(wrong_schema_id)),
                CatalogWrite {
                    key: index_key,
                    expected_value: None,
                    payload: Some(index_payload),
                },
                dependency_write(index_id, wrong_schema_id, 1),
            ],
            &[audit_event(0xa5)],
        ),
        Err(CommitError::InvalidCatalog),
        "an index parent's schema must match its typed table's schema"
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let (valid_key, valid_payload) = index_definition_with_columns(id(96), id(16), &[id(0x66)]);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(92),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: valid_key,
                    expected_value: None,
                    payload: Some(valid_payload),
                },
                object_write(id(96), 7),
            ],
            &[audit_event(92)],
        )?,
        1,
    );
    Ok(())
}
