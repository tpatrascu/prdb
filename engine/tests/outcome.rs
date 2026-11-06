use prdb_engine::outcome::{
    InvocationOutcome, OutcomeError, OutcomeStatus, prune_expired_outcomes,
};
use prdb_engine::schema::{OUTCOMES, STATS, create_tables};
use redb::{Database, ReadableDatabase, ReadableTable};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn open() -> Result<(Self, Database), Box<dyn std::error::Error>> {
        let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("prdb-outcome-{}-{id}.redb", std::process::id()));
        let database = Database::create(&path)?;
        let write = database.begin_write()?;
        create_tables(&write)?;
        write.commit()?;
        Ok((Self(path), database))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn seed_outcomes(
    database: &Database,
    values: &[([u8; 16], i64)],
) -> Result<(), Box<dyn std::error::Error>> {
    let write = database.begin_write()?;
    let mut outcomes = write.open_table(OUTCOMES)?;
    for (id, retention_deadline_us) in values {
        let encoded = InvocationOutcome {
            status: OutcomeStatus::Aborted,
            invocation_id: *id,
            principal_id: [0x99; 16],
            commit_sequence: 0,
            retention_deadline_us: *retention_deadline_us,
        }
        .encode()?;
        outcomes.insert(id.as_slice(), encoded.as_slice())?;
    }
    drop(outcomes);
    write.commit()?;
    Ok(())
}

fn outcome_count(database: &Database) -> Result<usize, Box<dyn std::error::Error>> {
    let read = database.begin_read()?;
    Ok(read.open_table(OUTCOMES)?.iter()?.count())
}

fn sample() -> InvocationOutcome {
    InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x11; 16],
        principal_id: [0x22; 16],
        commit_sequence: 0x0807_0605_0403_0201,
        retention_deadline_us: 0x0807_0605_0403_0201,
    }
}

#[test]
fn committed_outcome_is_canonical_and_round_trips() {
    let outcome = sample();
    let encoded = outcome.encode().unwrap();
    assert_eq!(&encoded[..8], b"OUT1\x01\x00\x01\x01");
    assert_eq!(&encoded[8..24], &[0x11; 16]);
    assert_eq!(&encoded[24..40], &[0x22; 16]);
    assert_eq!(&encoded[40..48], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(&encoded[48..56], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(InvocationOutcome::decode(&encoded), Ok(outcome));
}

#[test]
fn committed_outcome_rejects_unknown_status_ids_and_invalid_retention() {
    let canonical = sample().encode().unwrap();
    let mut bytes = canonical;
    bytes[0] = b'X';
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidMagic)
    );
    let mut bytes = canonical;
    bytes[6] = 3;
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidStatus)
    );
    let mut bytes = canonical;
    bytes[7] = 0;
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidRetentionState)
    );
    let mut bytes = canonical;
    bytes[8..24].fill(0);
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidId)
    );
    let mut bytes = canonical;
    bytes[40..48].fill(0);
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidCommitSequence)
    );
    let mut bytes = canonical;
    bytes[48..56].fill(0);
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidRetentionDeadline)
    );
    assert_eq!(
        InvocationOutcome::decode(&canonical[..55]),
        Err(OutcomeError::InvalidLength)
    );
}

#[test]
fn aborted_outcome_uses_no_commit_sequence() {
    let aborted = InvocationOutcome {
        status: OutcomeStatus::Aborted,
        invocation_id: [0x11; 16],
        principal_id: [0x22; 16],
        commit_sequence: 0,
        retention_deadline_us: 100,
    };
    let encoded = aborted.encode().unwrap();
    assert_eq!(encoded[6], 2);
    assert_eq!(InvocationOutcome::decode(&encoded), Ok(aborted.clone()));
    let invalid = InvocationOutcome {
        commit_sequence: 1,
        ..aborted
    };
    assert_eq!(invalid.encode(), Err(OutcomeError::InvalidCommitSequence));
}

#[test]
fn expired_outcomes_are_removed_in_bounded_resumable_batches()
-> Result<(), Box<dyn std::error::Error>> {
    let (fixture, database) = Fixture::open()?;
    seed_outcomes(
        &database,
        &[
            ([1; 16], 10),
            ([2; 16], 10),
            ([3; 16], 10),
            ([4; 16], 10),
            ([5; 16], 10),
        ],
    )?;

    let first = prune_expired_outcomes(&database, 10, 2)
        .map_err(|error| format!("first bounded prune failed: {error:?}"))?;
    drop(database);
    let database = Database::open(&fixture.0)?;
    let second = prune_expired_outcomes(&database, 10, 2)
        .map_err(|error| format!("second bounded prune failed: {error:?}"))?;
    let third = prune_expired_outcomes(&database, 10, 2)
        .map_err(|error| format!("third bounded prune failed: {error:?}"))?;
    assert_eq!(
        (first.scanned, first.removed, first.cycle_complete),
        (2, 2, false)
    );
    assert_eq!(
        (second.scanned, second.removed, second.cycle_complete),
        (2, 2, false)
    );
    assert_eq!(
        (third.scanned, third.removed, third.cycle_complete),
        (1, 1, true)
    );
    assert_eq!(outcome_count(&database)?, 0);
    Ok(())
}

#[test]
fn outcome_pruning_keeps_unexpired_records_and_rejects_invalid_budgets()
-> Result<(), Box<dyn std::error::Error>> {
    let (_fixture, database) = Fixture::open()?;
    seed_outcomes(&database, &[([1; 16], 10), ([2; 16], 20), ([3; 16], 21)])?;
    let result = prune_expired_outcomes(&database, 20, 8)
        .map_err(|error| format!("bounded prune failed: {error:?}"))?;
    assert_eq!(
        (result.scanned, result.removed, result.cycle_complete),
        (3, 2, true)
    );
    assert_eq!(outcome_count(&database)?, 1);
    assert!(matches!(
        prune_expired_outcomes(&database, 0, 2),
        Err(prdb_engine::outcome::OutcomeMaintenanceError::InvalidTime)
    ));
    assert!(matches!(
        prune_expired_outcomes(&database, 20, 0),
        Err(prdb_engine::outcome::OutcomeMaintenanceError::InvalidLimit)
    ));
    assert!(matches!(
        prune_expired_outcomes(&database, 20, 1025),
        Err(prdb_engine::outcome::OutcomeMaintenanceError::InvalidLimit)
    ));
    Ok(())
}

#[test]
fn exact_prune_batch_requires_a_follow_up_to_confirm_cycle_completion()
-> Result<(), Box<dyn std::error::Error>> {
    let (_fixture, database) = Fixture::open()?;
    seed_outcomes(&database, &[([1; 16], 10), ([2; 16], 10)])?;
    let result = prune_expired_outcomes(&database, 10, 2)
        .map_err(|error| format!("exact-batch prune failed: {error:?}"))?;
    assert_eq!(
        (result.scanned, result.removed, result.cycle_complete),
        (2, 2, false)
    );
    let completed = prune_expired_outcomes(&database, 10, 2)
        .map_err(|error| format!("completion prune failed: {error:?}"))?;
    assert_eq!(
        (
            completed.scanned,
            completed.removed,
            completed.cycle_complete
        ),
        (0, 0, true)
    );
    Ok(())
}

#[test]
fn outcome_pruning_rejects_a_corrupt_persistent_cursor_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let (_fixture, database) = Fixture::open()?;
    seed_outcomes(&database, &[([1; 16], 10), ([2; 16], 10)])?;
    let write = database.begin_write()?;
    let mut stats = write.open_table(STATS)?;
    stats.insert(b"outcome_prune_cursor_v1".as_slice(), b"invalid".as_slice())?;
    drop(stats);
    write.commit()?;

    assert!(matches!(
        prune_expired_outcomes(&database, 10, 1),
        Err(prdb_engine::outcome::OutcomeMaintenanceError::InvalidCursor)
    ));
    assert_eq!(outcome_count(&database)?, 2);
    Ok(())
}

#[test]
fn outcome_pruning_rejects_a_corrupt_outcome_atomically() -> Result<(), Box<dyn std::error::Error>>
{
    let (_fixture, database) = Fixture::open()?;
    seed_outcomes(&database, &[([1; 16], 10), ([2; 16], 10)])?;
    let write = database.begin_write()?;
    let mut outcomes = write.open_table(OUTCOMES)?;
    outcomes.insert([1; 16].as_slice(), b"bad".as_slice())?;
    drop(outcomes);
    write.commit()?;

    assert!(matches!(
        prune_expired_outcomes(&database, 10, 2),
        Err(prdb_engine::outcome::OutcomeMaintenanceError::InvalidOutcome(_))
    ));
    assert_eq!(outcome_count(&database)?, 2);
    Ok(())
}
