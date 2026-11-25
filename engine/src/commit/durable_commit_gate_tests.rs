use super::{CommitError, CommitRequest, RowWrite, commit_storage_changes_with_gate};
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::outcome::{InvocationOutcome, OutcomeStatus, prune_expired_outcomes};
use crate::schema::{self, ENGINE_META, OUTCOMES, ROWS};
use crate::storage_manager::{InvocationStatus, StorageManager, StorageManagerConfig};
use crate::transaction::TransactionError;
use redb::{Database, ReadableDatabase, ReadableTableMetadata, StorageBackend};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct FaultInjectedBackend {
    inner: redb::backends::FileBackend,
    fail_next_sync: Arc<AtomicBool>,
    exit_after_sync: Arc<AtomicBool>,
    exit_after_write: Arc<AtomicBool>,
    fail_next_write: Arc<AtomicBool>,
    fail_next_read: Arc<AtomicBool>,
    fail_next_set_len: Arc<AtomicBool>,
    fail_next_len: Arc<AtomicBool>,
}

impl StorageBackend for FaultInjectedBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        if self.fail_next_len.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected storage length failure"));
        }
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        if self.fail_next_read.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected storage read failure"));
        }
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        if self.fail_next_set_len.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected storage resize failure"));
        }
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        if self.fail_next_sync.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected commit sync failure"));
        }
        self.inner.sync_data()?;
        if self.exit_after_sync.swap(false, Ordering::SeqCst) {
            std::process::exit(86);
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected storage write failure"));
        }
        self.inner.write(offset, data)?;
        if self.exit_after_write.swap(false, Ordering::SeqCst) {
            std::process::exit(87);
        }
        Ok(())
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.inner.close()
    }
}

fn run_process_crash_boundary(
    boundary: &str,
    expected_exit: i32,
    child_test_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_COMMIT_CRASH_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_COMMIT_CRASH_BOUNDARY";
    const INVOCATION_ID: [u8; 16] = [0x86; 16];
    const PRINCIPAL_ID: [u8; 16] = [0x87; 16];
    let row_key = [0x88; 16].into_iter().chain([0x89; 16]).collect::<Vec<_>>();

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let exit_after_sync = Arc::new(AtomicBool::new(false));
        let exit_after_write = Arc::new(AtomicBool::new(false));
        let database = Database::builder().create_with_backend(FaultInjectedBackend {
            inner: redb::backends::FileBackend::new(file)?,
            fail_next_sync: Arc::new(AtomicBool::new(false)),
            exit_after_sync: exit_after_sync.clone(),
            exit_after_write: exit_after_write.clone(),
            fail_next_write: Arc::new(AtomicBool::new(false)),
            fail_next_read: Arc::new(AtomicBool::new(false)),
            fail_next_set_len: Arc::new(AtomicBool::new(false)),
            fail_next_len: Arc::new(AtomicBool::new(false)),
        })?;
        let storage =
            StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
        let mut invocation =
            storage.begin_invocation(INVOCATION_ID, PRINCIPAL_ID, i64::MAX, 1, 1024)?;
        invocation.stage_put(
            crate::overlay::RowId {
                table: [0x88; 16],
                row: [0x89; 16],
            },
            b"crash-boundary row",
        )?;
        match std::env::var(CHILD_BOUNDARY_ENV)?.as_str() {
            "sync" => exit_after_sync.store(true, Ordering::SeqCst),
            "write" => exit_after_write.store(true, Ordering::SeqCst),
            other => return Err(format!("unknown crash boundary {other}").into()),
        }
        invocation.commit()?;
        return Err("child commit returned without the injected process exit".into());
    }

    let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "prdb-process-crash-{}-{fixture_id}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let path = directory.join("crash.redb");
    {
        let database = Database::create(&path)?;
        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        schema::create_tables(&write)?;
        let metadata = EngineMetadata {
            database_id: [0x33; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        write
            .open_table(ENGINE_META)?
            .insert(&b"format"[..], &metadata[..])?;
        write.commit()?;
    }

    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg(child_test_name)
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &path)
        .env(CHILD_BOUNDARY_ENV, boundary)
        .status()?;
    assert_eq!(
        child_status.code(),
        Some(expected_exit),
        "unexpected child exit: {child_status}"
    );

    let reopened = Database::open(&path)?;
    let read = reopened.begin_read()?;
    let outcome = read
        .open_table(OUTCOMES)?
        .get(&INVOCATION_ID[..])?
        .map(|value| InvocationOutcome::decode(value.value()))
        .transpose()?;
    let metadata = read
        .open_table(ENGINE_META)?
        .get(&&b"format"[..])?
        .ok_or("crash recovery lost engine metadata")?;
    let sequence = EngineMetadata::decode(metadata.value())?.commit_sequence;
    let row_exists = read.open_table(ROWS)?.get(row_key.as_slice())?.is_some();
    match (outcome, sequence, row_exists) {
        (Some(outcome), 1, true) => {
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, 1);
        }
        (None, 0, false) => {}
        (outcome, sequence, row_exists) => {
            return Err(format!(
                    "process-crash recovery exposed a partial commit: outcome={outcome:?}, sequence={sequence}, row={row_exists}"
                )
                .into());
        }
    }

    drop(read);
    drop(reopened);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn process_crash_after_durable_sync_recovers_atomic_commit()
-> Result<(), Box<dyn std::error::Error>> {
    run_process_crash_boundary(
        "sync",
        86,
        "commit::durable_commit_gate_tests::process_crash_after_durable_sync_recovers_atomic_commit",
    )
}

#[test]
fn process_crash_during_backend_write_recovers_atomic_commit()
-> Result<(), Box<dyn std::error::Error>> {
    run_process_crash_boundary(
        "write",
        87,
        "commit::durable_commit_gate_tests::process_crash_during_backend_write_recovers_atomic_commit",
    )
}

#[test]
fn denied_gate_discards_staged_records_and_granted_gate_publishes_them()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "prdb-commit-gate-{}-{fixture_id}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let path = directory.join("gate.redb");
    let database = Database::create(&path)?;
    {
        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        schema::create_tables(&write)?;
        let metadata = EngineMetadata {
            database_id: [0x33; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        write
            .open_table(ENGINE_META)?
            .insert(&b"format"[..], &metadata[..])?;
        write.commit()?;
    }

    let row_key = [0x44; 32];
    let row_write = RowWrite {
        key: row_key,
        expected_version: None,
        payload: Some(b"staged row".to_vec()),
    };
    let request = |invocation_id| CommitRequest {
        invocation_id,
        principal_id: [0x55; 16],
        retention_deadline_us: 10_000,
        rows: std::slice::from_ref(&row_write),
        indexes: &[],
        foreign_keys: &[],
        catalog_guards: &[],
        catalog: &[],
        audit: &[],
    };
    let mut gate_calls = 0;
    assert_eq!(
        commit_storage_changes_with_gate(
            &database,
            request([0x66; 16]),
            || {
                gate_calls += 1;
                false
            },
            |_| {}
        ),
        Err(CommitError::InvocationCancelled)
    );
    assert_eq!(gate_calls, 1);
    {
        let read = database.begin_read()?;
        assert!(read.open_table(ROWS)?.get(&row_key[..])?.is_none());
        assert!(read.open_table(OUTCOMES)?.get(&[0x66; 16][..])?.is_none());
        let metadata = read
            .open_table(ENGINE_META)?
            .get(&&b"format"[..])?
            .ok_or("missing engine metadata")?;
        assert_eq!(EngineMetadata::decode(metadata.value())?.commit_sequence, 0);
    }

    assert_eq!(
        commit_storage_changes_with_gate(&database, request([0x77; 16]), || true, |_| {}),
        Ok(1)
    );
    {
        let read = database.begin_read()?;
        assert!(read.open_table(ROWS)?.get(&row_key[..])?.is_some());
        assert!(read.open_table(OUTCOMES)?.get(&[0x77; 16][..])?.is_some());
        let metadata = read
            .open_table(ENGINE_META)?
            .get(&&b"format"[..])?
            .ok_or("missing committed engine metadata")?;
        assert_eq!(EngineMetadata::decode(metadata.value())?.commit_sequence, 1);
    }

    drop(database);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn ambiguous_redb_commit_error_requires_reopen_and_never_records_aborted()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "prdb-commit-error-{}-{fixture_id}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let path = directory.join("ambiguous.redb");
    {
        let database = Database::create(&path)?;
        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        schema::create_tables(&write)?;
        let metadata = EngineMetadata {
            database_id: [0x33; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        write
            .open_table(ENGINE_META)?
            .insert(&b"format"[..], &metadata[..])?;
        write.commit()?;
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let fail_next_sync = Arc::new(AtomicBool::new(false));
    let database = Database::builder().create_with_backend(FaultInjectedBackend {
        inner: redb::backends::FileBackend::new(file)?,
        fail_next_sync: fail_next_sync.clone(),
        exit_after_sync: Arc::new(AtomicBool::new(false)),
        exit_after_write: Arc::new(AtomicBool::new(false)),
        fail_next_write: Arc::new(AtomicBool::new(false)),
        fail_next_read: Arc::new(AtomicBool::new(false)),
        fail_next_set_len: Arc::new(AtomicBool::new(false)),
        fail_next_len: Arc::new(AtomicBool::new(false)),
    })?;
    let storage =
        StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
    let invocation_id = [0x66; 16];
    let principal_id = [0x55; 16];
    let mut invocation =
        storage.begin_invocation(invocation_id, principal_id, i64::MAX, 1, 1024)?;
    invocation.stage_put(
        crate::overlay::RowId {
            table: [0x41; 16],
            row: [0x42; 16],
        },
        b"fault-injected row write",
    )?;
    fail_next_sync.store(true, Ordering::SeqCst);
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Storage(_)))
    ));
    assert!(!fail_next_sync.load(Ordering::SeqCst));
    let live_status = storage
        .get_invocation_status(invocation_id, principal_id, 1)?
        .ok_or("ambiguous invocation status disappeared before reopen")?;
    assert!(matches!(
        live_status.status,
        InvocationStatus::Committing { .. }
    ));
    assert!(storage.requires_reopen_after_commit_error());
    assert!(matches!(
        storage.begin_invocation([0x67; 16], principal_id, i64::MAX, 1, 1024),
        Err(TransactionError::Storage(_))
    ));
    let closed_database_key = &database as *const Database as usize;
    drop(storage);
    let rebuilt_before_reopen =
        StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
    assert!(rebuilt_before_reopen.requires_reopen_after_commit_error());
    assert!(matches!(
        rebuilt_before_reopen.begin_invocation([0x68; 16], principal_id, i64::MAX, 1, 1024),
        Err(TransactionError::Storage(_))
    ));
    assert!(matches!(
        rebuilt_before_reopen.get_invocation_status([0x69; 16], principal_id, 1),
        Err(crate::storage_manager::InvocationStatusError::Storage)
    ));
    assert!(
        rebuilt_before_reopen
            .lookup_invocation_outcome(invocation_id, principal_id, 1)
            .is_err()
    );
    drop(rebuilt_before_reopen);
    drop(database);

    // redb requires a fresh handle after a non-poisoned commit I/O error.
    let reopened = Database::open(&path)?;
    crate::storage_manager::clear_uncertain_database_key(closed_database_key);
    let recovered_storage =
        StorageManager::from_validated_database(&reopened, StorageManagerConfig::default())?;
    assert!(!recovered_storage.requires_reopen_after_commit_error());
    let read = reopened.begin_read()?;
    let outcome = read
        .open_table(OUTCOMES)?
        .get(&invocation_id[..])?
        .map(|value| crate::outcome::InvocationOutcome::decode(value.value()))
        .transpose()?;
    let metadata = read
        .open_table(ENGINE_META)?
        .get(&&b"format"[..])?
        .ok_or("missing metadata after redb recovery")?;
    let sequence = EngineMetadata::decode(metadata.value())?.commit_sequence;
    let row_key = [0x41; 16].into_iter().chain([0x42; 16]).collect::<Vec<_>>();
    let row_exists = read.open_table(ROWS)?.get(row_key.as_slice())?.is_some();
    match (outcome, sequence) {
        (Some(outcome), 1) => {
            assert_eq!(outcome.status, crate::outcome::OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, 1);
            assert!(row_exists);
        }
        (None, 0) => assert!(!row_exists),
        (outcome, sequence) => {
            return Err(format!(
                "redb recovery exposed a partial commit: outcome={outcome:?}, sequence={sequence}"
            )
            .into());
        }
    }

    drop(read);
    drop(recovered_storage);
    drop(reopened);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn redb_storage_write_error_quarantines_and_recovers_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "prdb-storage-write-error-{}-{fixture_id}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let path = directory.join("write-error.redb");
    {
        let database = Database::create(&path)?;
        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        schema::create_tables(&write)?;
        let metadata = EngineMetadata {
            database_id: [0x33; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        write
            .open_table(ENGINE_META)?
            .insert(&b"format"[..], &metadata[..])?;
        write.commit()?;
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let fail_next_write = Arc::new(AtomicBool::new(false));
    let database = Database::builder().create_with_backend(FaultInjectedBackend {
        inner: redb::backends::FileBackend::new(file)?,
        fail_next_sync: Arc::new(AtomicBool::new(false)),
        exit_after_sync: Arc::new(AtomicBool::new(false)),
        exit_after_write: Arc::new(AtomicBool::new(false)),
        fail_next_write: fail_next_write.clone(),
        fail_next_read: Arc::new(AtomicBool::new(false)),
        fail_next_set_len: Arc::new(AtomicBool::new(false)),
        fail_next_len: Arc::new(AtomicBool::new(false)),
    })?;
    let storage =
        StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
    let invocation_id = [0x76; 16];
    let principal_id = [0x65; 16];
    let mut invocation =
        storage.begin_invocation(invocation_id, principal_id, i64::MAX, 1, 1024)?;
    invocation.stage_put(
        crate::overlay::RowId {
            table: [0x51; 16],
            row: [0x52; 16],
        },
        b"fault-injected storage write",
    )?;
    fail_next_write.store(true, Ordering::SeqCst);
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Storage(_)))
    ));
    assert!(!fail_next_write.load(Ordering::SeqCst));
    assert!(storage.requires_reopen_after_commit_error());
    assert!(matches!(
        storage.begin_invocation([0x77; 16], principal_id, i64::MAX, 1, 1024),
        Err(TransactionError::Storage(_))
    ));

    let closed_database_key = &database as *const Database as usize;
    drop(storage);
    drop(database);
    let reopened = Database::open(&path)?;
    crate::storage_manager::clear_uncertain_database_key(closed_database_key);
    let recovered_storage =
        StorageManager::from_validated_database(&reopened, StorageManagerConfig::default())?;
    assert!(!recovered_storage.requires_reopen_after_commit_error());
    let read = reopened.begin_read()?;
    let outcome = read
        .open_table(OUTCOMES)?
        .get(&invocation_id[..])?
        .map(|value| crate::outcome::InvocationOutcome::decode(value.value()))
        .transpose()?;
    let metadata = read
        .open_table(ENGINE_META)?
        .get(&&b"format"[..])?
        .ok_or("missing engine metadata after write error recovery")?;
    let sequence = EngineMetadata::decode(metadata.value())?.commit_sequence;
    let row_key = [0x51; 16].into_iter().chain([0x52; 16]).collect::<Vec<_>>();
    let row_exists = read.open_table(ROWS)?.get(row_key.as_slice())?.is_some();
    match (outcome, sequence) {
        (Some(outcome), 1) => {
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, 1);
            assert!(row_exists);
        }
        (None, 0) => assert!(!row_exists),
        (outcome, sequence) => {
            return Err(format!(
                    "redb write-error recovery exposed a partial commit: outcome={outcome:?}, sequence={sequence}"
                )
                .into());
        }
    }

    drop(read);
    drop(recovered_storage);
    drop(reopened);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn redb_storage_read_or_len_error_rejects_database_open() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "prdb-storage-read-error-{}-{fixture_id}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let path = directory.join("read-error.redb");
    {
        let database = Database::create(&path)?;
        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        schema::create_tables(&write)?;
        let metadata = EngineMetadata {
            database_id: [0x33; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        write
            .open_table(ENGINE_META)?
            .insert(&b"format"[..], &metadata[..])?;
        write.commit()?;
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let fail_next_read = Arc::new(AtomicBool::new(false));
    fail_next_read.store(true, Ordering::SeqCst);
    let opened = Database::builder().create_with_backend(FaultInjectedBackend {
        inner: redb::backends::FileBackend::new(file)?,
        fail_next_sync: Arc::new(AtomicBool::new(false)),
        exit_after_sync: Arc::new(AtomicBool::new(false)),
        exit_after_write: Arc::new(AtomicBool::new(false)),
        fail_next_write: Arc::new(AtomicBool::new(false)),
        fail_next_read: fail_next_read.clone(),
        fail_next_set_len: Arc::new(AtomicBool::new(false)),
        fail_next_len: Arc::new(AtomicBool::new(false)),
    });
    assert!(opened.is_err());
    assert!(!fail_next_read.load(Ordering::SeqCst));

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let fail_next_len = Arc::new(AtomicBool::new(true));
    let opened = Database::builder().create_with_backend(FaultInjectedBackend {
        inner: redb::backends::FileBackend::new(file)?,
        fail_next_sync: Arc::new(AtomicBool::new(false)),
        exit_after_sync: Arc::new(AtomicBool::new(false)),
        exit_after_write: Arc::new(AtomicBool::new(false)),
        fail_next_write: Arc::new(AtomicBool::new(false)),
        fail_next_read: Arc::new(AtomicBool::new(false)),
        fail_next_set_len: Arc::new(AtomicBool::new(false)),
        fail_next_len: fail_next_len.clone(),
    });
    assert!(opened.is_err());
    assert!(!fail_next_len.load(Ordering::SeqCst));

    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn redb_compaction_resize_error_preserves_reopenable_data() -> Result<(), Box<dyn std::error::Error>>
{
    const ROW_COUNT: u8 = 64;
    const DELETE_COUNT: u8 = 58;
    let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "prdb-compaction-resize-error-{}-{fixture_id}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let path = directory.join("compact.redb");
    {
        let database = Database::create(&path)?;
        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        schema::create_tables(&write)?;
        let metadata = EngineMetadata {
            database_id: [0x33; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        write
            .open_table(ENGINE_META)?
            .insert(&b"format"[..], &metadata[..])?;
        {
            let mut rows = write.open_table(ROWS)?;
            let value = vec![0x5a; 100 * 1024];
            for id in 0..ROW_COUNT {
                let key = [id; 32];
                rows.insert(key.as_slice(), value.as_slice())?;
            }
        }
        write.commit()?;

        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        {
            let mut rows = write.open_table(ROWS)?;
            for id in 0..DELETE_COUNT {
                let key = [id; 32];
                rows.remove(key.as_slice())?;
            }
        }
        write.commit()?;
        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        write.commit()?;
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let fail_next_set_len = Arc::new(AtomicBool::new(false));
    let mut database = Database::builder().create_with_backend(FaultInjectedBackend {
        inner: redb::backends::FileBackend::new(file)?,
        fail_next_sync: Arc::new(AtomicBool::new(false)),
        exit_after_sync: Arc::new(AtomicBool::new(false)),
        exit_after_write: Arc::new(AtomicBool::new(false)),
        fail_next_write: Arc::new(AtomicBool::new(false)),
        fail_next_read: Arc::new(AtomicBool::new(false)),
        fail_next_set_len: fail_next_set_len.clone(),
        fail_next_len: Arc::new(AtomicBool::new(false)),
    })?;
    fail_next_set_len.store(true, Ordering::SeqCst);
    assert!(matches!(
        database.compact(),
        Err(redb::CompactionError::Storage(_))
    ));
    assert!(!fail_next_set_len.load(Ordering::SeqCst));
    drop(database);

    let reopened = Database::open(&path)?;
    let read = reopened.begin_read()?;
    let rows = read.open_table(ROWS)?;
    assert_eq!(rows.len()?, u64::from(ROW_COUNT - DELETE_COUNT));
    for id in 0..DELETE_COUNT {
        let key = [id; 32];
        assert!(rows.get(key.as_slice())?.is_none());
    }
    for id in DELETE_COUNT..ROW_COUNT {
        let key = [id; 32];
        assert_eq!(
            rows.get(key.as_slice())?
                .ok_or("reopened database lost a live row")?
                .value(),
            vec![0x5a; 100 * 1024]
        );
    }

    drop(rows);
    drop(read);
    drop(reopened);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn outcome_prune_commit_error_quarantines_database() -> Result<(), Box<dyn std::error::Error>> {
    let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "prdb-outcome-prune-error-{}-{fixture_id}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let path = directory.join("prune.redb");
    {
        let database = Database::create(&path)?;
        let mut write = database.begin_write()?;
        write.set_durability(redb::Durability::Immediate)?;
        write.set_two_phase_commit(true);
        schema::create_tables(&write)?;
        let metadata = EngineMetadata {
            database_id: [0x33; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        write
            .open_table(ENGINE_META)?
            .insert(&b"format"[..], &metadata[..])?;
        let expired = InvocationOutcome {
            status: OutcomeStatus::Aborted,
            invocation_id: [0x45; 16],
            principal_id: [0x54; 16],
            commit_sequence: 0,
            retention_deadline_us: 10,
        }
        .encode()?;
        write
            .open_table(OUTCOMES)?
            .insert(&[0x45; 16][..], &expired[..])?;
        write.commit()?;
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let fail_next_sync = Arc::new(AtomicBool::new(false));
    let database = Database::builder().create_with_backend(FaultInjectedBackend {
        inner: redb::backends::FileBackend::new(file)?,
        fail_next_sync: fail_next_sync.clone(),
        exit_after_sync: Arc::new(AtomicBool::new(false)),
        exit_after_write: Arc::new(AtomicBool::new(false)),
        fail_next_write: Arc::new(AtomicBool::new(false)),
        fail_next_read: Arc::new(AtomicBool::new(false)),
        fail_next_set_len: Arc::new(AtomicBool::new(false)),
        fail_next_len: Arc::new(AtomicBool::new(false)),
    })?;
    let storage =
        StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
    fail_next_sync.store(true, Ordering::SeqCst);
    assert!(prune_expired_outcomes(&database, 11, 1).is_err());
    assert!(!fail_next_sync.load(Ordering::SeqCst));
    assert!(storage.requires_reopen_after_commit_error());
    assert!(matches!(
        storage.begin_invocation([0x65; 16], [0x54; 16], i64::MAX, 1, 1024),
        Err(TransactionError::Storage(_))
    ));

    let database_key = &database as *const Database as usize;
    drop(storage);
    drop(database);
    let reopened = Database::open(&path)?;
    crate::storage_manager::clear_uncertain_database_key(database_key);
    let read = reopened.begin_read()?;
    assert!(read.open_table(OUTCOMES)?.get(&[0x45; 16][..])?.is_some());
    drop(read);
    drop(reopened);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}
