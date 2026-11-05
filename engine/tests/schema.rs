use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::schema::{
    self, AUDIT, CATALOG, ENGINE_META, INDEXES, OUTCOMES, ROW_INDEXES, ROWS, STATS,
};
use redb::{Database, ReadableDatabase};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-schema-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("schema.redb")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn creates_and_reopens_all_versioned_engine_tables() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    {
        let database = Database::create(fixture.path())?;
        let write = database.begin_write()?;
        schema::create_tables(&write)?;
        let record = EngineMetadata {
            database_id: [0x42; 16],
            catalog_version: 1,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Initializing,
        };
        {
            let mut metadata = write.open_table(ENGINE_META)?;
            metadata.insert(&b"format"[..], &record.encode()?[..])?;
        }
        write.commit()?;
    }

    let database = Database::open(fixture.path())?;
    let read = database.begin_read()?;
    let stored_metadata = read
        .open_table(ENGINE_META)?
        .get(&b"format"[..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        EngineMetadata::decode(&stored_metadata)?,
        EngineMetadata {
            database_id: [0x42; 16],
            catalog_version: 1,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence: 0,
            lifecycle: Lifecycle::Initializing,
        }
    );
    drop(read.open_table(ROWS)?);
    drop(read.open_table(INDEXES)?);
    drop(read.open_table(ROW_INDEXES)?);
    drop(read.open_table(CATALOG)?);
    drop(read.open_table(OUTCOMES)?);
    drop(read.open_table(AUDIT)?);
    drop(read.open_table(STATS)?);
    Ok(())
}
