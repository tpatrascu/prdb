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
fn invocation_work_budgets_are_class_scoped_and_inherit_by_default() {
    let defaults = StorageManagerConfig::default();
    assert_eq!(
        defaults.max_planner_estimate_requests_for(crate::supervisor::WorkloadClass::Operational),
        DEFAULT_PLANNER_ESTIMATE_REQUESTS
    );
    assert_eq!(
        defaults.max_planner_estimate_requests_for(crate::supervisor::WorkloadClass::Analytical),
        DEFAULT_PLANNER_ESTIMATE_REQUESTS
    );
    assert_eq!(
        defaults.max_planner_estimate_candidates_for(crate::supervisor::WorkloadClass::Operational),
        DEFAULT_ESTIMATE_CANDIDATES
    );
    assert_eq!(
        defaults.max_planner_estimate_candidates_for(crate::supervisor::WorkloadClass::Analytical),
        DEFAULT_ESTIMATE_CANDIDATES
    );
    assert_eq!(
        defaults.max_invocation_scan_work_for(crate::supervisor::WorkloadClass::Operational),
        crate::transaction::MAX_INVOCATION_SCAN_WORK
    );
    assert_eq!(
        defaults.max_invocation_scan_work_for(crate::supervisor::WorkloadClass::Analytical),
        crate::transaction::MAX_INVOCATION_SCAN_WORK
    );
    assert_eq!(
        defaults.max_authorized_index_range_candidates_for(
            crate::supervisor::WorkloadClass::Operational
        ),
        crate::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES as usize
    );
    assert_eq!(
        defaults.max_authorized_index_range_candidates_for(
            crate::supervisor::WorkloadClass::Analytical
        ),
        crate::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES as usize
    );
    assert_eq!(
        defaults
            .max_authorized_index_range_bytes_for(crate::supervisor::WorkloadClass::Operational),
        crate::transaction::MAX_AUTHORIZED_INDEX_RANGE_BYTES
    );
    assert_eq!(
        defaults.max_authorized_index_range_bytes_for(crate::supervisor::WorkloadClass::Analytical),
        crate::transaction::MAX_AUTHORIZED_INDEX_RANGE_BYTES
    );
    let elevated = StorageManagerConfig {
        max_planner_estimate_requests: 32,
        max_analytical_planner_estimate_requests: Some(1024),
        max_planner_estimate_candidates: 1024,
        max_analytical_planner_estimate_candidates: Some(65_536),
        max_invocation_scan_work: 65_536,
        max_analytical_invocation_scan_work: Some(1_048_576),
        max_authorized_index_range_candidates: 512,
        max_analytical_authorized_index_range_candidates: Some(2_048),
        max_authorized_index_range_bytes: 64 * 1024 * 1024,
        max_analytical_authorized_index_range_bytes: Some(512 * 1024 * 1024),
        max_statistics_collection_rows: 65_536,
        ..defaults
    };
    assert_eq!(
        elevated.max_planner_estimate_requests_for(crate::supervisor::WorkloadClass::Operational),
        32
    );
    assert_eq!(
        elevated.max_planner_estimate_requests_for(crate::supervisor::WorkloadClass::Analytical),
        1024
    );
    assert_eq!(
        elevated.max_planner_estimate_candidates_for(crate::supervisor::WorkloadClass::Operational),
        1024
    );
    assert_eq!(
        elevated.max_planner_estimate_candidates_for(crate::supervisor::WorkloadClass::Analytical),
        65_536
    );
    assert_eq!(
        elevated.max_invocation_scan_work_for(crate::supervisor::WorkloadClass::Operational),
        65_536
    );
    assert_eq!(
        elevated.max_invocation_scan_work_for(crate::supervisor::WorkloadClass::Analytical),
        1_048_576
    );
    assert_eq!(
        elevated.max_authorized_index_range_candidates_for(
            crate::supervisor::WorkloadClass::Operational
        ),
        512
    );
    assert_eq!(
        elevated.max_authorized_index_range_candidates_for(
            crate::supervisor::WorkloadClass::Analytical
        ),
        2_048
    );
    assert_eq!(
        elevated
            .max_authorized_index_range_bytes_for(crate::supervisor::WorkloadClass::Operational),
        64 * 1024 * 1024
    );
    assert_eq!(
        elevated.max_authorized_index_range_bytes_for(crate::supervisor::WorkloadClass::Analytical),
        512 * 1024 * 1024
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
