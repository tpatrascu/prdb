//! Disposable redb API experiments, not a storage-manager interface.
use redb::{
    CompactionError, Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition,
};
use std::sync::atomic::{AtomicU64, Ordering};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("fixture");
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-redb-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("fixture.redb")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn committed_writes_survive_reopen_and_aborted_writes_do_not()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    {
        let db = Database::create(fixture.path())?;
        let mut write = db.begin_write()?;
        write.set_durability(Durability::Immediate)?;
        write.set_two_phase_commit(true);
        {
            let mut table = write.open_table(TABLE)?;
            table.insert(&b"committed"[..], &b"value"[..])?;
        }
        write.commit()?;
        let write = db.begin_write()?;
        {
            let mut table = write.open_table(TABLE)?;
            table.insert(&b"aborted"[..], &b"value"[..])?;
        }
        write.abort()?;
    }
    let db = Database::open(fixture.path())?;
    let read = db.begin_read()?;
    let table = read.open_table(TABLE)?;
    assert_eq!(table.get(&b"committed"[..])?.unwrap().value(), b"value");
    assert!(table.get(&b"aborted"[..])?.is_none());
    Ok(())
}

#[test]
fn retained_snapshot_is_stable_and_byte_keys_sort_lexicographically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let db = Database::create(fixture.path())?;
    let write = db.begin_write()?;
    {
        let mut table = write.open_table(TABLE)?;
        table.insert(&b"a"[..], &b"old"[..])?;
    }
    write.commit()?;
    let old = db.begin_read()?;
    let write = db.begin_write()?;
    {
        let mut table = write.open_table(TABLE)?;
        table.insert(&b"a"[..], &b"new"[..])?;
        table.insert(&b"a\0"[..], &b"prefix"[..])?;
        table.insert(&b"\xff"[..], &b"last"[..])?;
    }
    write.commit()?;
    assert_eq!(
        old.open_table(TABLE)?.get(&b"a"[..])?.unwrap().value(),
        b"old"
    );
    let current = db.begin_read()?;
    let table = current.open_table(TABLE)?;
    assert_eq!(table.get(&b"a"[..])?.unwrap().value(), b"new");
    let keys: Vec<Vec<u8>> = table
        .iter()?
        .map(|entry| entry.unwrap().0.value().to_vec())
        .collect();
    assert_eq!(keys, [b"a".to_vec(), b"a\0".to_vec(), vec![0xff]]);
    Ok(())
}

#[test]
fn retained_reader_allows_repeated_durable_writes_without_changing_its_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    const VALUE_BYTES: usize = 32 * 1024;
    const WRITE_COUNT: u32 = 64;

    let fixture = Fixture::new();
    let db = Database::create(fixture.path())?;
    let original_value = vec![0x11; VALUE_BYTES];
    let mut write = db.begin_write()?;
    write.set_durability(Durability::Immediate)?;
    write.set_two_phase_commit(true);
    {
        let mut table = write.open_table(TABLE)?;
        table.insert(&b"row"[..], original_value.as_slice())?;
    }
    write.commit()?;

    let retained_reader = db.begin_read()?;
    for version in 0..WRITE_COUNT {
        let value = vec![version as u8; VALUE_BYTES];
        let mut write = db.begin_write()?;
        write.set_durability(Durability::Immediate)?;
        write.set_two_phase_commit(true);
        {
            let mut table = write.open_table(TABLE)?;
            table.insert(&b"row"[..], value.as_slice())?;
        }
        write.commit()?;
    }

    assert_eq!(
        retained_reader
            .open_table(TABLE)?
            .get(&b"row"[..])?
            .ok_or("retained snapshot lost its row")?
            .value(),
        original_value
    );
    drop(retained_reader);

    let current_reader = db.begin_read()?;
    assert_eq!(
        current_reader
            .open_table(TABLE)?
            .get(&b"row"[..])?
            .ok_or("current snapshot lost its row")?
            .value(),
        vec![(WRITE_COUNT - 1) as u8; VALUE_BYTES]
    );
    Ok(())
}

#[test]
fn retained_reader_blocks_compaction_until_release_then_reclaims_deleted_pages()
-> Result<(), Box<dyn std::error::Error>> {
    const ROW_COUNT: u32 = 64;
    const DELETE_COUNT: u32 = 58;
    const VALUE_BYTES: usize = 100 * 1024;

    let fixture = Fixture::new();
    let mut db = Database::create(fixture.path())?;
    let value = vec![0x5a; VALUE_BYTES];
    let mut write = db.begin_write()?;
    write.set_durability(Durability::Immediate)?;
    write.set_two_phase_commit(true);
    {
        let mut table = write.open_table(TABLE)?;
        for key in 0..ROW_COUNT {
            table.insert(&key.to_be_bytes()[..], value.as_slice())?;
        }
    }
    write.commit()?;

    let retained_reader = db.begin_read()?;
    let mut write = db.begin_write()?;
    write.set_durability(Durability::Immediate)?;
    write.set_two_phase_commit(true);
    {
        let mut table = write.open_table(TABLE)?;
        for key in 0..DELETE_COUNT {
            table.remove(&key.to_be_bytes()[..])?;
        }
    }
    write.commit()?;
    let mut write = db.begin_write()?;
    write.set_durability(Durability::Immediate)?;
    write.set_two_phase_commit(true);
    write.commit()?;

    let old_table = retained_reader.open_table(TABLE)?;
    assert_eq!(old_table.len()?, u64::from(ROW_COUNT));
    assert_eq!(
        old_table
            .get(&0u32.to_be_bytes()[..])?
            .ok_or("retained snapshot lost a deleted row")?
            .value(),
        value
    );
    drop(old_table);
    assert!(matches!(
        db.compact(),
        Err(CompactionError::TransactionInProgress)
    ));
    drop(retained_reader);

    let file_size_before = std::fs::metadata(fixture.path())?.len();
    assert!(db.compact()?);
    let file_size_after = std::fs::metadata(fixture.path())?.len();
    assert!(
        file_size_after < file_size_before,
        "compaction did not reclaim deleted pages: {file_size_before} -> {file_size_after}"
    );

    let current_reader = db.begin_read()?;
    let current_table = current_reader.open_table(TABLE)?;
    assert_eq!(current_table.len()?, u64::from(ROW_COUNT - DELETE_COUNT));
    for key in DELETE_COUNT..ROW_COUNT {
        assert_eq!(
            current_table
                .get(&key.to_be_bytes()[..])?
                .ok_or("compaction lost a retained row")?
                .value(),
            value
        );
    }
    Ok(())
}
