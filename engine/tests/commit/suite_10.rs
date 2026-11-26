use prdb_engine::audit::AuditAction;
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus, lookup_outcome};
use prdb_engine::storage_manager::{StorageManager, StorageManagerConfig, StorageManagerInitError};
use redb::Database;
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_admission_requires_a_login_enabled_catalog_user()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::AuthorizationError;
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_invocation_age: Duration::from_secs(30),
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_authorized_index_range_candidates: 128,
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_planner_estimate_requests:
                prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
            max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS
                as usize,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        },
    )?;
    let enabled_user = id(229);
    let disabled_user = id(230);
    let writes = [
        object_write_with_name(enabled_user, 8, "admitted_user"),
        principal_write_with_name(enabled_user, 1, 1, "admitted_user"),
        object_write_with_name(disabled_user, 8, "disabled_user"),
        principal_write_with_login(disabled_user, 1, false, 1, "disabled_user"),
    ];
    storage.commit_catalog_and_audit(id(231), id(9), 9_999, &writes, &[audit_event(231)])?;

    let (mut admitted, roles) =
        storage.begin_authenticated_invocation(id(232), enabled_user, 9_999, 2, 64)?;
    assert!(roles.is_empty());
    admitted.abort()?;

    for (invocation_id, principal_id) in [(id(233), disabled_user), (id(234), id(235))] {
        assert!(matches!(
            storage.begin_authenticated_invocation(invocation_id, principal_id, 9_999, 2, 64,),
            Err(TransactionError::Authorization(AuthorizationError::Denied))
        ));
        assert_eq!(
            lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted
        );
    }

    let (mut admitted_again, _) =
        storage.begin_authenticated_invocation(id(236), enabled_user, 9_999, 2, 64)?;
    admitted_again.abort()?;
    Ok(())
}

pub(super) fn authenticated_role_closure_budget_ignores_unreachable_memberships()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let user_id = id(0xa1);
    let first_role = id(0xa2);
    let second_role = id(0xa3);
    let third_role = id(0xa4);
    let mut records = vec![
        principal_write_with_name(user_id, 1, 1, "role_budget_user"),
        object_write_with_name(user_id, 8, "role_budget_user"),
        principal_write_with_name(first_role, 2, 1, "role_budget_first"),
        object_write_with_name(first_role, 9, "role_budget_first"),
        principal_write_with_name(second_role, 2, 1, "role_budget_second"),
        object_write_with_name(second_role, 9, "role_budget_second"),
        principal_write_with_name(third_role, 2, 1, "role_budget_third"),
        object_write_with_name(third_role, 9, "role_budget_third"),
    ];
    for (member_id, role_id) in [(first_role, second_role), (second_role, third_role)] {
        records.push(membership_write(member_id, role_id));
        records.push(dependency_write(member_id, role_id, 5));
    }
    let mut audit = identity_audit_event(0xa5, builtin_owner_id(), user_id);
    audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xa5), builtin_owner_id(), 9_999, &records, &[audit])?;

    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            max_invocation_scan_work: 1,
            max_authorized_index_range_candidates: 128,
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_planner_estimate_requests:
                prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
            max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS
                as usize,
            ..StorageManagerConfig::default()
        },
    )?;
    let (mut invocation, roles) =
        storage.begin_authenticated_invocation(id(0xa6), user_id, 9_999, 1, 64)?;
    assert!(roles.is_empty());
    invocation.abort()?;
    assert_eq!(
        lookup_outcome(&database, id(0xa6), user_id, 1)?
            .ok_or("missing role-closure admission outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn catalog_grant_loading_budget_ignores_unrelated_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let user_id = id(0xb1);
    let other_user = id(0xb2);
    let grantor = builtin_owner_id();
    let records = [
        principal_write_with_name(user_id, 1, 1, "grant_budget_user"),
        object_write_with_name(user_id, 8, "grant_budget_user"),
        principal_write_with_name(other_user, 1, 1, "grant_budget_other"),
        object_write_with_name(other_user, 8, "grant_budget_other"),
        grant_write(other_user, id(16), 2, 1, id(16), grantor),
        grant_write(other_user, id(16), 4, 1, id(16), grantor),
        dependency_write(other_user, id(16), 5),
    ];
    let mut audit = identity_audit_event(0xb3, grantor, user_id);
    audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb3), grantor, 9_999, &records, &[audit])?;

    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            max_invocation_scan_work: 1,
            max_authorized_index_range_candidates: 128,
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_planner_estimate_requests:
                prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
            max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS
                as usize,
            ..StorageManagerConfig::default()
        },
    )?;
    let (mut invocation, roles) =
        storage.begin_authenticated_invocation(id(0xb4), user_id, 9_999, 1, 64)?;
    assert!(roles.is_empty());
    let (loaded_roles, grants) = invocation.load_catalog_schema_usage_grants(user_id)?;
    assert!(loaded_roles.is_empty());
    assert!(grants.is_empty());
    invocation.abort()?;
    assert_eq!(
        lookup_outcome(&database, id(0xb4), user_id, 1)?
            .ok_or("missing grant-loader admission outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn storage_manager_bounds_active_invocation_snapshots_across_clones()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                max_authorized_index_range_bytes: 0,
                ..StorageManagerConfig::default()
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroAuthorizedIndexRangeByteLimit,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 0,
                max_invocation_age: Duration::from_secs(30),
                max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
                max_authorized_index_range_candidates: 128,
                max_authorized_index_range_bytes: 16 * 1024 * 1024,
                max_planner_estimate_requests:
                    prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
                max_planner_estimate_candidates:
                    prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
                max_statistics_collection_rows:
                    prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS as usize,
                max_token_lifecycle_requests_per_minute: 30,
                max_admin_cancellation_requests_per_minute: 30,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroActiveInvocationLimit,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 1,
                max_invocation_age: Duration::ZERO,
                max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
                max_authorized_index_range_candidates: 128,
                max_authorized_index_range_bytes: 16 * 1024 * 1024,
                max_planner_estimate_requests:
                    prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
                max_planner_estimate_candidates:
                    prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
                max_statistics_collection_rows:
                    prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS as usize,
                max_token_lifecycle_requests_per_minute: 30,
                max_admin_cancellation_requests_per_minute: 30,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroInvocationAge,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 1,
                max_invocation_age: Duration::from_secs(30),
                max_invocation_scan_work: 0,
                max_authorized_index_range_candidates: 128,
                max_authorized_index_range_bytes: 16 * 1024 * 1024,
                max_planner_estimate_requests:
                    prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
                max_planner_estimate_candidates:
                    prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
                max_statistics_collection_rows:
                    prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS as usize,
                max_token_lifecycle_requests_per_minute: 30,
                max_admin_cancellation_requests_per_minute: 30,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroInvocationScanWorkLimit,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                max_authorized_index_range_candidates: 0,
                ..StorageManagerConfig::default()
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroAuthorizedIndexRangeCandidateLimit,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                max_authorized_index_range_candidates:
                    prdb_engine::query_abi::MAX_SQL_INDEX_PROBE_CANDIDATES as usize + 1,
                ..StorageManagerConfig::default()
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::AuthorizedIndexRangeCandidateLimitTooLarge,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 1,
                max_invocation_age: Duration::from_secs(30),
                max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
                max_authorized_index_range_candidates: 128,
                max_authorized_index_range_bytes: 16 * 1024 * 1024,
                max_planner_estimate_requests:
                    prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
                max_planner_estimate_candidates:
                    prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
                max_statistics_collection_rows:
                    prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS as usize,
                max_token_lifecycle_requests_per_minute: 0,
                max_admin_cancellation_requests_per_minute: 30,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroTokenLifecycleRequestLimit,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 1,
                max_invocation_age: Duration::from_secs(30),
                max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
                max_authorized_index_range_candidates: 128,
                max_authorized_index_range_bytes: 16 * 1024 * 1024,
                max_planner_estimate_requests: prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
                max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
                max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS as usize,
                max_token_lifecycle_requests_per_minute: 30,
                max_admin_cancellation_requests_per_minute: 0,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroAdminCancellationRequestLimit,
        ))
    );
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_invocation_age: Duration::from_secs(30),
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_authorized_index_range_candidates: 512,
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_planner_estimate_requests:
                prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
            max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS
                as usize,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        },
    )?;
    let independent_handle = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(independent_handle.max_active_invocations(), 1);
    let op_profile = prdb_engine::workload_profile::WorkloadProfileLimits::small_operational();
    let large_profile = prdb_engine::workload_profile::WorkloadProfileLimits::large_analytical();
    assert_eq!(
        storage.max_authorized_index_range_candidates_for_profile(&op_profile),
        usize::try_from(op_profile.max_sql_index_probe_candidates)
            .unwrap()
            .min(512)
    );
    assert_eq!(
        storage.max_authorized_index_range_candidates_for_profile(&large_profile),
        512
    );
    let mut first = storage.begin_invocation(id(192), id(9), 9_999, 2, 64)?;
    let cloned_storage = storage.clone();
    assert!(matches!(
        cloned_storage.begin_invocation(id(193), id(9), 9_999, 2, 64),
        Err(prdb_engine::transaction::TransactionError::AdmissionLimitExceeded)
    ));
    assert!(matches!(
        independent_handle.begin_invocation(id(195), id(9), 9_999, 2, 64),
        Err(prdb_engine::transaction::TransactionError::AdmissionLimitExceeded)
    ));

    first.abort()?;
    let next = cloned_storage.begin_invocation(id(194), id(9), 9_999, 2, 64)?;
    drop(next);
    drop(first);
    Ok(())
}

pub(super) fn storage_manager_expires_idle_invocations_and_releases_the_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_invocation_age: Duration::from_millis(1),
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_authorized_index_range_candidates: 128,
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_planner_estimate_requests:
                prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
            max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS
                as usize,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        },
    )?;
    let mut expired = storage.begin_invocation(id(196), id(9), 9_999, 2, 64)?;
    std::thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        expired.check_deadline(),
        Err(prdb_engine::transaction::TransactionError::Expired)
    ));
    assert_eq!(
        lookup_outcome(&database, id(196), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let next = storage.begin_invocation(id(197), id(9), 9_999, 2, 64)?;
    drop(next);
    drop(expired);

    let expired_commit = storage.begin_invocation(id(198), id(9), 9_999, 2, 64)?;
    std::thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        expired_commit.commit(),
        Err(prdb_engine::transaction::TransactionError::Expired)
    ));
    assert_eq!(
        lookup_outcome(&database, id(198), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn storage_manager_resolves_only_retained_outcomes_for_the_authenticated_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let mut aborted = storage.begin_invocation(id(199), id(9), 9_999, 2, 64)?;
    aborted.abort()?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(199), id(9), 9_998)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let mut committed = storage.begin_invocation(id(200), id(9), 9_999, 2, 64)?;
    committed.stage_put(
        prdb_engine::overlay::RowId {
            table: id(16),
            row: id(201),
        },
        b"status lookup",
    )?;
    committed.commit()?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(200), id(9), 9_998)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        storage.lookup_invocation_outcome(id(200), id(10), 9_998)?,
        None
    );
    assert_eq!(
        storage.lookup_invocation_outcome(id(200), id(9), 9_999)?,
        None
    );
    assert_eq!(
        storage.lookup_invocation_outcome(id(202), id(9), 9_998)?,
        None
    );
    Ok(())
}

pub(super) fn storage_manager_reports_live_and_durable_invocation_status_to_the_owner()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::InvocationStatus;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let status_storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;

    let mut active = storage.begin_invocation(id(203), id(9), retention_deadline_us, 2, 64)?;
    assert!(matches!(
        storage.begin_invocation(id(203), id(9), retention_deadline_us, 2, 64),
        Err(prdb_engine::transaction::TransactionError::DuplicateInvocation)
    ));
    let live = status_storage
        .get_invocation_status(id(203), id(9), now_us)?
        .unwrap();
    assert!(matches!(live.status, InvocationStatus::Active { .. }));
    assert_eq!(live.observed_at_us, now_us);
    assert_eq!(
        status_storage.get_invocation_status(id(203), id(10), now_us)?,
        None
    );
    active.abort()?;
    drop(active);
    assert!(matches!(
        storage
            .get_invocation_status(id(203), id(9), now_us)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(InvocationOutcome {
            status: OutcomeStatus::Aborted,
            ..
        })
    ));

    let mut pending_commit =
        storage.begin_invocation(id(204), id(9), retention_deadline_us, 2, 64)?;
    let cancelled_row_key = table_row_key(id(205), id(206));
    pending_commit.stage_put(
        prdb_engine::overlay::RowId {
            table: id(205),
            row: id(206),
        },
        b"must not commit",
    )?;
    let writer = database.begin_write()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let commit_thread = scope.spawn(move || pending_commit.commit());
        assert_eq!(
            storage.cancel_owned_invocation(id(9), id(204), now_us)?,
            prdb_engine::storage_manager::OwnerCancellationResult::Accepted
        );
        assert!(matches!(
            status_storage
                .get_invocation_status(id(204), id(9), now_us)?
                .unwrap()
                .status,
            InvocationStatus::Active {
                cancellation_requested: true,
                ..
            }
        ));
        writer.abort()?;
        assert!(matches!(
            commit_thread
                .join()
                .map_err(|_| std::io::Error::other("commit thread panicked"))?,
            Err(prdb_engine::transaction::TransactionError::CancellationRequested)
        ));
        Ok(())
    })?;
    assert!(matches!(
        storage
            .get_invocation_status(id(204), id(9), now_us)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(InvocationOutcome {
            status: OutcomeStatus::Aborted,
            ..
        })
    ));
    assert!(row(&database, &cancelled_row_key)?.is_none());
    let committed = storage.begin_invocation(id(207), id(9), retention_deadline_us, 2, 64)?;
    committed.commit()?;
    drop(storage);
    drop(status_storage);
    drop(database);

    let reopened = Database::open(fixture.path())?;
    let restarted_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    assert!(matches!(
        restarted_storage
            .get_invocation_status(id(204), id(9), now_us)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(InvocationOutcome {
            status: OutcomeStatus::Aborted,
            ..
        })
    ));
    assert!(matches!(
        restarted_storage
            .get_invocation_status(id(207), id(9), now_us)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(InvocationOutcome {
            status: OutcomeStatus::Committed,
            ..
        })
    ));
    assert_eq!(
        restarted_storage.get_invocation_status(id(204), id(10), now_us)?,
        None
    );
    Ok(())
}

pub(super) fn storage_manager_lists_only_owner_live_invocations_with_generation_bound_pages()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        InvocationStatus, InvocationStatusConsistency, InvocationStatusError,
        MAX_OWN_INVOCATION_PAGE,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;
    let mut first = storage.begin_invocation(id(220), id(9), retention_deadline_us, 2, 64)?;
    let mut hidden = storage.begin_invocation(id(221), id(10), retention_deadline_us, 2, 64)?;
    let mut second = storage.begin_invocation(id(222), id(9), retention_deadline_us, 2, 64)?;
    first.stage_put(
        prdb_engine::overlay::RowId {
            table: id(225),
            row: id(226),
        },
        b"own-usage",
    )?;

    let page = storage.list_own_invocations(id(9), now_us, 1, None)?;
    assert_eq!(page.invocations.len(), 1);
    assert_eq!(page.invocations[0].invocation_id, id(220));
    assert_eq!(page.invocations[0].staged_rows, 1);
    assert_eq!(page.invocations[0].staged_value_bytes, 9);
    assert!(matches!(
        page.invocations[0].status,
        InvocationStatus::Active { .. }
    ));
    assert_eq!(page.observed_at_us, now_us);
    assert_eq!(page.consistency, InvocationStatusConsistency::BestEffort);
    let cursor = page.continuation.unwrap();
    let mut unrelated = storage.begin_invocation(id(224), id(10), retention_deadline_us, 2, 64)?;
    assert!(matches!(
        storage.list_own_invocations(id(10), now_us, 1, Some(&cursor)),
        Err(InvocationStatusError::InvalidCursor)
    ));
    let next = storage.list_own_invocations(id(9), now_us, 1, Some(&cursor))?;
    assert_eq!(next.invocations.len(), 1);
    assert_eq!(next.invocations[0].invocation_id, id(222));
    assert_eq!(next.continuation, None);
    assert!(matches!(
        storage.list_own_invocations(id(9), now_us, 0, None),
        Err(InvocationStatusError::InvalidPageLimit)
    ));
    assert!(
        storage
            .list_own_invocations(id(9), now_us, MAX_OWN_INVOCATION_PAGE + 1, None)
            .is_err()
    );

    let stale = storage
        .list_own_invocations(id(9), now_us, 1, None)?
        .continuation
        .unwrap();
    first.stage_put(
        prdb_engine::overlay::RowId {
            table: id(225),
            row: id(226),
        },
        b"changed",
    )?;
    assert!(matches!(
        storage.list_own_invocations(id(9), now_us, 1, Some(&stale)),
        Err(InvocationStatusError::StaleCursor)
    ));
    let refreshed = storage.list_own_invocations(id(9), now_us, 1, None)?;
    assert_eq!(refreshed.invocations[0].staged_rows, 1);
    assert_eq!(refreshed.invocations[0].staged_value_bytes, 7);
    let stale = refreshed.continuation.unwrap();
    let mut added = storage.begin_invocation(id(223), id(9), retention_deadline_us, 2, 64)?;
    assert!(matches!(
        storage.list_own_invocations(id(9), now_us, 1, Some(&stale)),
        Err(InvocationStatusError::StaleCursor)
    ));

    added.abort()?;
    unrelated.abort()?;
    second.abort()?;
    hidden.abort()?;
    first.abort()?;
    Ok(())
}
