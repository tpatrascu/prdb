use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{
    self, AUDIT, CATALOG, ENGINE_META, INDEXES, OUTCOMES, ROW_INDEXES, ROWS,
};
use prdb_engine::startup::{StartupError, validate_persisted_records};
use redb::Database;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-startup-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> std::path::PathBuf {
        self.0.join("startup.redb")
    }

    fn database(&self, commit_sequence: u64) -> Database {
        let database = Database::create(self.path()).unwrap();
        let write = database.begin_write().unwrap();
        schema::create_tables(&write).unwrap();
        let metadata = EngineMetadata {
            database_id: [0x11; 16],
            catalog_version: 1,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence,
            lifecycle: Lifecycle::Ready,
        }
        .encode()
        .unwrap();
        write
            .open_table(ENGINE_META)
            .unwrap()
            .insert(&b"format"[..], &metadata[..])
            .unwrap();
        write.commit().unwrap();
        database
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn row_key(table_id: [u8; 16], row_id: [u8; 16]) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&table_id);
    key[16..].copy_from_slice(&row_id);
    key
}

fn reverse_index_key(row_key: &[u8; 32], index_key: &[u8]) -> Vec<u8> {
    let mut key = row_key.to_vec();
    key.extend_from_slice(&index_key[..index_key.len() - 16]);
    key
}

fn insert_row(database: &Database, key: &[u8; 32], version: u64) {
    let bytes = RowRecord {
        row_version: version,
        payload: b"tuple-fixture".to_vec(),
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(&key[..], &bytes[..])
        .unwrap();
    write.commit().unwrap();
}

fn index_definition(index_id: [u8; 16], table_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let schema = family_schema(9).unwrap();
    let mut key = 9u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&index_id);
    let mut columns = 1u32.to_le_bytes().to_vec();
    columns.extend_from_slice(&[0x66; 16]);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: table_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Ids,
            payload: columns,
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bool,
            payload: vec![0],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Id,
            payload: [0x77; 16].to_vec(),
        },
        CatalogField {
            tag: 8,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::U32,
            payload: 1u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    (
        key.clone(),
        encode_envelope(&schema, &key, &fields).unwrap(),
    )
}

fn put_catalog(database: &Database, key: &[u8], value: &[u8]) {
    let write = database.begin_write().unwrap();
    write
        .open_table(CATALOG)
        .unwrap()
        .insert(key, value)
        .unwrap();
    write.commit().unwrap();
}

fn principal_record(
    id: [u8; 16],
    kind: u16,
    login_enabled: bool,
    epoch: u64,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 10u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: kind.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Text,
            payload: format!("principal_{}", id[0]).into_bytes(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Bool,
            payload: vec![u8::from(login_enabled)],
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::U64,
            payload: epoch.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(10).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

fn membership_record(
    member_id: [u8; 16],
    role_id: [u8; 16],
    grantor_id: [u8; 16],
    epoch: u64,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 11u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&member_id);
    key.push(1);
    key.extend_from_slice(&role_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: grantor_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U64,
            payload: epoch.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(11).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

#[test]
fn validates_ready_image_records_and_references() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(3);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let key = row_key(table_id, row_id);
    insert_row(&database, &key, 2);

    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 2,
    }
    .encode();
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 3,
        retention_deadline_us: 9_999,
    }
    .encode()?;
    let audit = AuditEvent {
        event_id: [0x88; 16],
        actor_id: Some([0x66; 16]),
        correlation_id: [0x99; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[0xaa; 16]],
    }
    .encode()?;
    let write = database.begin_write()?;
    write
        .open_table(CATALOG)?
        .insert(&catalog_key[..], &catalog_value[..])?;
    write
        .open_table(INDEXES)?
        .insert(&index_key[..], &locator[..])?;
    write
        .open_table(ROW_INDEXES)?
        .insert(&reverse_index_key(&key, &index_key)[..], &b""[..])?;
    write
        .open_table(OUTCOMES)?
        .insert(&[0x55; 16][..], &outcome[..])?;
    write
        .open_table(AUDIT)?
        .insert(&[0x88; 16][..], &audit[..])?;
    write.commit()?;

    let report = validate_persisted_records(&database)?;
    assert_eq!(report.metadata.commit_sequence, 3);
    assert_eq!(report.rows, 1);
    assert_eq!(report.index_entries, 1);
    assert_eq!(report.catalog_records, 1);
    assert_eq!(report.outcomes, 1);
    assert_eq!(report.audit_events, 1);
    Ok(())
}

#[test]
fn rejects_row_versions_beyond_the_commit_frontier() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    insert_row(&database, &row_key([0x22; 16], [0x33; 16]), 2);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::SequenceAhead)
    ));
}

#[test]
fn rejects_zero_object_ids_in_row_keys() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    insert_row(&database, &row_key([0; 16], [0x33; 16]), 1);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidRow)
    ));
}

#[test]
fn rejects_index_without_its_catalog_definition() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let row_id = [0x33; 16];
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

#[test]
fn rejects_index_without_its_referenced_row() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &catalog_key, &catalog_value);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write
        .open_table(ROW_INDEXES)
        .unwrap()
        .insert(
            &reverse_index_key(&row_key(table_id, row_id), &index_key)[..],
            &b""[..],
        )
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

#[test]
fn rejects_a_locator_whose_version_differs_from_its_row() {
    let fixture = Fixture::new();
    let database = fixture.database(2);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let key = row_key(table_id, row_id);
    insert_row(&database, &key, 2);
    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &catalog_key, &catalog_value);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write
        .open_table(ROW_INDEXES)
        .unwrap()
        .insert(&reverse_index_key(&key, &index_key)[..], &b""[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

#[test]
fn rejects_catalog_record_stored_under_a_different_key() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = index_definition([0x44; 16], [0x22; 16]);
    let mut wrong_key = key;
    wrong_key[18] ^= 1;
    put_catalog(&database, &wrong_key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn rejects_a_committed_outcome_beyond_the_commit_frontier() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 2,
        retention_deadline_us: 9_999,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(OUTCOMES)
        .unwrap()
        .insert(&[0x55; 16][..], &outcome[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidOutcome)
    ));
}

#[test]
fn initializing_database_is_not_ready_for_normal_startup() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let metadata = EngineMetadata {
        database_id: [0x11; 16],
        catalog_version: 1,
        expression_version: 1,
        builtin_semantics_version: 1,
        commit_sequence: 0,
        lifecycle: Lifecycle::Initializing,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(ENGINE_META)
        .unwrap()
        .insert(&b"format"[..], &metadata[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::NotReady)
    ));
}

#[test]
fn rejects_outcome_key_value_mismatch() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 1,
        retention_deadline_us: 9_999,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(OUTCOMES)
        .unwrap()
        .insert(&[0x54; 16][..], &outcome[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidOutcome)
    ));
}

#[test]
fn accepts_pinned_acyclic_membership_graph() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 2, false, 1),
        principal_record([3; 16], 2, false, 1),
        membership_record([1; 16], [2; 16], [1; 16], 1),
        membership_record([2; 16], [3; 16], [1; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    let report = validate_persisted_records(&database)?;
    assert_eq!(report.catalog_records, 5);
    Ok(())
}

#[test]
fn rejects_cycles_and_invalid_membership_endpoints_at_startup() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 2, false, 1),
        principal_record([3; 16], 2, false, 1),
        membership_record([2; 16], [3; 16], [1; 16], 1),
        membership_record([3; 16], [2; 16], [1; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));

    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 1, true, 1),
        membership_record([1; 16], [2; 16], [1; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}
