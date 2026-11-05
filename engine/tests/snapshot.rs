use prdb_engine::catalog::{PolicyTarget, encode_policy_target};
use prdb_engine::overlay::{Overlay, ResolvedRow, RowId};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{CATALOG, ROWS};
use prdb_engine::snapshot::RowSnapshot;
use redb::{Database, ReadableDatabase};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-snapshot-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("snapshot.redb")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn id(row: u8) -> RowId {
    RowId {
        table: [0; 16],
        row: [row; 16],
    }
}

fn row_key(row: u8) -> [u8; 32] {
    let mut key = [0; 32];
    key[16..].fill(row);
    key
}

#[test]
fn retained_redb_snapshot_is_stable_and_overlay_writes_shadow_it()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = Database::create(fixture.path())?;
    let write = database.begin_write()?;
    {
        let mut rows = write.open_table(ROWS)?;
        let old = RowRecord {
            row_version: 1,
            payload: b"old".to_vec(),
        }
        .encode()?;
        let deleted = RowRecord {
            row_version: 1,
            payload: b"deleted".to_vec(),
        }
        .encode()?;
        rows.insert(&row_key(1)[..], &old[..])?;
        rows.insert(&row_key(2)[..], &deleted[..])?;
    }
    let catalog_payload = encode_policy_target(&PolicyTarget {
        policy_id: [1; 16],
        kind: 1,
        target_id: [2; 16],
    })?;
    let catalog_key = catalog_payload[32..71].to_vec();
    write
        .open_table(CATALOG)?
        .insert(&catalog_key[..], &catalog_payload[..])?;
    let mut corrupt_key = vec![0, 10, 1];
    corrupt_key.extend_from_slice(&[3; 16]);
    write
        .open_table(CATALOG)?
        .insert(&corrupt_key[..], &b"broken"[..])?;
    write.commit()?;

    let snapshot = RowSnapshot::new(database.begin_read()?);
    assert_eq!(
        snapshot
            .get_catalog_record(&catalog_key)?
            .unwrap()
            .logical_key,
        catalog_key
    );
    assert!(snapshot.get_catalog_record(&corrupt_key).is_err());
    let mut overlay = Overlay::new(2, 16);
    overlay.put(id(1), b"new").unwrap();
    overlay.delete(id(2)).unwrap();

    let committed = RowRecord::decode(&snapshot.get(&row_key(1))?.unwrap())?;
    assert_eq!(committed.row_version, 1);
    assert_eq!(committed.payload, b"old");
    assert_eq!(
        overlay
            .read(&id(1), |_| Some(committed.payload.as_slice()))
            .unwrap(),
        Some(ResolvedRow::OverlayValue(&b"new"[..]))
    );
    let deleted_committed = RowRecord::decode(&snapshot.get(&row_key(2))?.unwrap())?;
    assert_eq!(
        overlay
            .read(&id(2), |_| Some(deleted_committed.payload.as_slice()))
            .unwrap(),
        None
    );

    let write = database.begin_write()?;
    {
        let mut rows = write.open_table(ROWS)?;
        let latest = RowRecord {
            row_version: 2,
            payload: b"latest".to_vec(),
        }
        .encode()?;
        rows.insert(&row_key(1)[..], &latest[..])?;
    }
    write.commit()?;

    let old_again = RowRecord::decode(&snapshot.get(&row_key(1))?.unwrap())?;
    assert_eq!(old_again.row_version, 1);
    assert_eq!(old_again.payload, b"old");
    let current = database.begin_read()?;
    let current_value = current
        .open_table(ROWS)?
        .get(&row_key(1)[..])?
        .unwrap()
        .value()
        .to_vec();
    let current_record = RowRecord::decode(&current_value)?;
    assert_eq!(current_record.row_version, 2);
    assert_eq!(current_record.payload, b"latest");
    Ok(())
}
