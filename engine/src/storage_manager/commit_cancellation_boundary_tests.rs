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
    state
        .live
        .lock()
        .unwrap()
        .record_outcome_maintenance(&Err(crate::outcome::OutcomeMaintenanceError::InvalidLimit));
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
    live.record_invocation_commit_latency(Duration::from_micros(1_000) + Duration::from_nanos(1));
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
    live.record_outcome_maintenance(&Err(crate::outcome::OutcomeMaintenanceError::InvalidLimit));

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
fn cancellation_after_committing_transition_is_too_late() -> Result<(), Box<dyn std::error::Error>>
{
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
        .map_err(|error| std::io::Error::other(format!("unexpected admission error: {error:?}")))?;
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
