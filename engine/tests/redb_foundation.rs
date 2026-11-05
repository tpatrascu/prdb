//! Disposable redb API experiments, not a storage-manager interface.
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
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
