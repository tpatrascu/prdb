use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::{decode_registered_envelope, encode_envelope, family_schema};
use prdb_engine::commit::{
    CatalogWrite, CommitError, IndexWrite, RowWrite, commit_catalog_and_audit,
    commit_rows_and_indexes,
};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::schema::{self, CATALOG, PRINCIPAL_NAMES};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ScalarType, ScalarValue};
use redb::ReadableDatabase;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn catalog_index_changes_reject_tables_with_existing_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = key(77);
    let existing_index_key = index_key(&row_key);
    commit_rows_and_indexes(
        &database,
        id(76),
        id(9),
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(bytea_tuple(b"existing row")),
        }],
        &[IndexWrite {
            key: existing_index_key.clone(),
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: false,
        }],
    )?;

    let (new_index_key, new_index_payload) = index_definition(id(0x88), id(16));
    let new_index = CatalogWrite {
        key: new_index_key.clone(),
        expected_value: None,
        payload: Some(new_index_payload),
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(77),
            id(9),
            9_999,
            &[new_index, object_write(id(0x88), 7)],
            &[audit_event(77)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(77), id(9), 1)?.unwrap().status,
        OutcomeStatus::Aborted
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(new_index_key.as_slice())?.is_none());
    assert!(
        catalog
            .get(id_catalog_key(2, id(0x88)).as_slice())?
            .is_none()
    );
    assert!(
        read.open_table(schema::INDEXES)?
            .get(existing_index_key.as_slice())?
            .is_some()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[77; 16][..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn storage_manager_builds_and_drops_populated_indexes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = table_row_key(id(16), id(77));
    let payload = bytea_tuple(b"existing row");
    let previous_index = typed_bytea_index_key(id(0x44), &row_key, &payload);
    commit_rows_and_indexes(
        &database,
        id(76),
        id(9),
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(payload.clone()),
        }],
        &[IndexWrite {
            key: previous_index.key,
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: false,
        }],
    )?;

    let (new_index_key, new_index_payload) = index_definition(id(0x88), id(16));
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let table_epoch_before_build = {
        let read = database.begin_read()?;
        let record = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(5, id(16)).as_slice())?
                .ok_or("table record missing before index build")?
                .value(),
        )?;
        u64::from_le_bytes(
            record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table write epoch missing before index build")?
                .payload
                .as_slice()
                .try_into()?,
        )
    };
    let stale_writer_invocation = id(0x7c);
    let stale_row_key = table_row_key(id(16), id(0x7d));
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, id(9), 9_999, 1, 64)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(16))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_payload = bytea_tuple(b"staged before index build");
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(16),
            row: id(0x7d),
        },
        &stale_payload,
    )?;
    let index_race_barrier = Arc::new(Barrier::new(2));
    let (stale_result, index_sequence) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&index_race_barrier);
        let storage = &storage;
        let index_key = new_index_key.clone();
        let ddl = scope.spawn(move || {
            barrier.wait();
            storage.commit_catalog_and_audit(
                id(77),
                id(9),
                9_999,
                &[
                    CatalogWrite {
                        key: index_key,
                        expected_value: None,
                        payload: Some(new_index_payload),
                    },
                    object_write(id(0x88), 7),
                ],
                &[audit_event(77)],
            )
        });
        index_race_barrier.wait();
        let stale_result = stale_writer.commit();
        let index_result = ddl.join().expect("index publication thread panicked");
        (
            stale_result,
            index_result.expect("index publication should commit"),
        )
    });
    let table_epoch_after_build = {
        let read = database.begin_read()?;
        let record = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(5, id(16)).as_slice())?
                .ok_or("table record missing after index build")?
                .value(),
        )?;
        u64::from_le_bytes(
            record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table write epoch missing after index build")?
                .payload
                .as_slice()
                .try_into()?,
        )
    };
    assert_eq!(table_epoch_after_build, table_epoch_before_build + 1);
    let stale_status = match stale_result {
        Ok(sequence) => {
            assert!(sequence < index_sequence);
            OutcomeStatus::Committed
        }
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        Err(error) => panic!("unexpected staged-write result in index race: {error:?}"),
    };
    assert_eq!(commit_sequence(&database)?, index_sequence);
    assert_eq!(
        row(&database, &stale_row_key)?.is_some(),
        stale_status == OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, id(9), 1)?
            .ok_or("missing stale index-build writer outcome")?
            .status,
        stale_status
    );

    let expected = typed_bytea_index_key(id(0x88), &row_key, &payload);
    let stale_expected = typed_bytea_index_key(id(0x88), &stale_row_key, &stale_payload);
    let read = database.begin_read()?;
    let mut reverse_key = row_key.to_vec();
    reverse_key.extend_from_slice(&expected.key[..expected.key.len() - 16]);
    let (definition_bytes, object_bytes) = {
        let indexes = read.open_table(schema::INDEXES)?;
        let locator = indexes.get(expected.key.as_slice())?.unwrap();
        assert_eq!(
            IndexLocator::decode_for_key(&expected.key, locator.value())?.row_version,
            1
        );
        assert!(
            read.open_table(schema::ROW_INDEXES)?
                .get(reverse_key.as_slice())?
                .is_some()
        );
        let mut stale_reverse_key = stale_row_key.to_vec();
        stale_reverse_key.extend_from_slice(&stale_expected.key[..stale_expected.key.len() - 16]);
        let stale_locator = indexes.get(stale_expected.key.as_slice())?;
        assert_eq!(
            stale_locator.is_some(),
            stale_status == OutcomeStatus::Committed,
            "index backfill must include a row committed before publication"
        );
        assert_eq!(
            read.open_table(schema::ROW_INDEXES)?
                .get(stale_reverse_key.as_slice())?
                .is_some(),
            stale_status == OutcomeStatus::Committed
        );
        if let Some(locator) = stale_locator {
            assert_eq!(
                IndexLocator::decode_for_key(&stale_expected.key, locator.value())?.row_version,
                2
            );
        }
        let catalog = read.open_table(CATALOG)?;
        let definition_bytes = catalog
            .get(new_index_key.as_slice())?
            .unwrap()
            .value()
            .to_vec();
        let object_key = id_catalog_key(2, id(0x88));
        let object_bytes = catalog
            .get(object_key.as_slice())?
            .unwrap()
            .value()
            .to_vec();
        assert!(
            read.open_table(schema::AUDIT)?
                .get(&[77; 16][..])?
                .is_some()
        );
        (definition_bytes, object_bytes)
    };
    assert_eq!(
        lookup_outcome(&database, id(77), id(9), 1)?.unwrap().status,
        OutcomeStatus::Committed
    );

    let mut object_delete = object_write(id(0x88), 7);
    object_delete.expected_value = Some(object_bytes);
    object_delete.payload = None;
    let drop_writes = [
        CatalogWrite {
            key: new_index_key.clone(),
            expected_value: Some(definition_bytes),
            payload: None,
        },
        object_delete,
        CatalogWrite {
            key: id_catalog_key(17, id(0x88)),
            expected_value: None,
            payload: Some(encode_envelope(
                &family_schema(17).unwrap(),
                &id_catalog_key(17, id(0x88)),
                &[],
            )?),
        },
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(203),
            id(9),
            9_999,
            &drop_writes,
            &[audit_event(203)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    let drop_writer_invocation = id(0x7e);
    let drop_writer_row_id = id(0x7f);
    let mut drop_writer =
        storage.begin_invocation(drop_writer_invocation, id(9), 9_999, 1, 1024)?;
    let drop_writer_schema = drop_writer.load_catalog_table_schema(id(16))?;
    drop_writer.load_catalog_indexes(&drop_writer_schema)?;
    let drop_writer_payload = bytea_tuple(b"staged before index drop");
    drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(16),
            row: drop_writer_row_id,
        },
        &drop_writer_payload,
    )?;
    let drop_writer_key = table_row_key(id(16), drop_writer_row_id);
    let drop_writer_index = typed_bytea_index_key(id(0x88), &drop_writer_key, &drop_writer_payload);
    let index_drop_invocation = id(202);
    let drop_barrier = Arc::new(Barrier::new(2));
    let (drop_result, drop_writer_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&drop_barrier);
        let storage = &storage;
        let ddl = scope.spawn(move || {
            barrier.wait();
            storage.commit_catalog_and_audit(
                index_drop_invocation,
                id(9),
                9_999,
                &drop_writes,
                &[audit_event(202)],
            )
        });
        drop_barrier.wait();
        let writer = drop_writer.commit();
        (ddl.join().expect("index drop thread panicked"), writer)
    });
    let drop_sequence = drop_result?;
    let drop_writer_status = match drop_writer_result {
        Ok(sequence) => {
            assert!(sequence < drop_sequence);
            OutcomeStatus::Committed
        }
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        Err(error) => panic!("unexpected writer result in index drop race: {error:?}"),
    };
    assert_eq!(
        drop_sequence,
        index_sequence + 1 + u64::from(drop_writer_status == OutcomeStatus::Committed)
    );
    assert_eq!(
        row(&database, &drop_writer_key)?.is_some(),
        drop_writer_status == OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, drop_writer_invocation, id(9), 1)?
            .ok_or("missing index-drop writer outcome")?
            .status,
        drop_writer_status
    );
    let table_epoch_after_drop = {
        let read = database.begin_read()?;
        let record = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(5, id(16)).as_slice())?
                .ok_or("table record missing after index drop")?
                .value(),
        )?;
        u64::from_le_bytes(
            record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table write epoch missing after index drop")?
                .payload
                .as_slice()
                .try_into()?,
        )
    };
    assert_eq!(table_epoch_after_drop, table_epoch_after_build + 1);
    assert!(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(17, id(0x88)).as_slice())?
            .is_some(),
        "DROP INDEX must permanently reserve its retired object ID"
    );
    assert!(
        read.open_table(schema::INDEXES)?
            .get(expected.key.as_slice())?
            .is_some()
    );
    assert!(
        database
            .begin_read()?
            .open_table(schema::INDEXES)?
            .get(expected.key.as_slice())?
            .is_none()
    );
    assert!(
        database
            .begin_read()?
            .open_table(schema::ROW_INDEXES)?
            .get(reverse_key.as_slice())?
            .is_none()
    );
    let mut drop_writer_reverse_key = drop_writer_key.to_vec();
    drop_writer_reverse_key
        .extend_from_slice(&drop_writer_index.key[..drop_writer_index.key.len() - 16]);
    let after_drop_read = database.begin_read()?;
    assert!(
        after_drop_read
            .open_table(schema::INDEXES)?
            .get(drop_writer_index.key.as_slice())?
            .is_none(),
        "index drop must remove memberships written immediately before it"
    );
    assert!(
        after_drop_read
            .open_table(schema::ROW_INDEXES)?
            .get(drop_writer_reverse_key.as_slice())?
            .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, index_drop_invocation, id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_rebuilds_populated_indexes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let index_admin = id(0x78);
    let new_column_object = object_write(id(0x67), 4);
    let new_column = column_write_at_ordinal(id(0x67), id(16), 1, "object_103");
    {
        let setup = database.begin_write()?;
        for record in [
            new_column_object,
            new_column,
            principal_write_with_name(index_admin, 1, 1, "index_rebuild_admin"),
            object_write_with_name(index_admin, 8, "index_rebuild_admin"),
            membership_write(index_admin, builtin_owner_id()),
            dependency_write(index_admin, builtin_owner_id(), 5),
        ] {
            if record.key.starts_with(&10u16.to_be_bytes()) {
                let decoded = decode_registered_envelope(record.payload.as_ref().unwrap())?;
                let name = decoded
                    .fields
                    .iter()
                    .find(|field| field.tag == 2)
                    .ok_or("principal name missing")?;
                setup
                    .open_table(PRINCIPAL_NAMES)?
                    .insert(name.payload.as_slice(), &record.key[3..19])?;
            }
            setup
                .open_table(CATALOG)?
                .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
        }
        setup.commit()?;
    }
    let row_key = table_row_key(id(16), id(73));
    let payload = two_bytea_tuple(b"old index key", b"new index key");
    let existing = typed_two_bytea_index_key(id(0x44), &row_key, &payload, id(0x66));
    commit_rows_and_indexes(
        &database,
        id(204),
        id(9),
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(payload.clone()),
        }],
        &[IndexWrite {
            key: existing.key.clone(),
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: false,
        }],
    )?;
    let old_snapshot = database.begin_read()?;
    let index_catalog_key = id_catalog_key(9, id(0x44));
    let old_definition = old_snapshot
        .open_table(CATALOG)?
        .get(index_catalog_key.as_slice())?
        .ok_or("missing existing index definition")?
        .value()
        .to_vec();
    let (same_index_key, unique_definition) =
        index_definition_with_options(id(0x44), id(16), &[id(0x67)], true);
    assert_eq!(same_index_key, index_catalog_key);
    let rebuilt = typed_two_bytea_index_key(id(0x44), &row_key, &payload, id(0x67));
    assert_ne!(existing.key, rebuilt.key);
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(208),
            id(9),
            9_999,
            &[CatalogWrite {
                key: index_catalog_key.clone(),
                expected_value: Some(old_definition.clone()),
                payload: Some(unique_definition.clone()),
            }],
            &[audit_event(208)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    let stale_writer_invocation = id(0x79);
    let stale_row = id(0x7a);
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, id(9), 9_999, 1, 64)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(16))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(16),
            row: stale_row,
        },
        &two_bytea_tuple(b"staged", b"before rebuild"),
    )?;
    let mut rebuild_audit = audit_event(205);
    rebuild_audit.actor_id = Some(index_admin);
    rebuild_audit.correlation_id = id(205);
    let rebuilt_result = storage
        .rebuild_index(prdb_engine::storage_manager::RebuildIndexRequest {
            invocation_id: id(205),
            authenticated_actor_id: index_admin,
            index_id: id(0x44),
            column_ids: vec![id(0x67)],
            unique: true,
            retention_deadline_us: 9_999,
            audit_event: rebuild_audit,
        })
        .map_err(|error| format!("typed index rebuild failed: {error:?}"))?;
    assert_eq!(rebuilt_result.commit_sequence, 2);
    let stale_result = stale_writer.commit();
    assert!(
        matches!(
            stale_result,
            Err(prdb_engine::transaction::TransactionError::Commit(
                CommitError::Conflict
            ))
        ),
        "writer pinned before index rebuild returned {stale_result:?}"
    );
    assert!(row(&database, &table_row_key(id(16), stale_row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, id(9), 1)?
            .ok_or("missing stale index-rebuild writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let old_catalog = old_snapshot.open_table(CATALOG)?;
    assert_eq!(
        old_catalog
            .get(index_catalog_key.as_slice())?
            .unwrap()
            .value(),
        old_definition
    );
    let old_indexes = old_snapshot.open_table(schema::INDEXES)?;
    let old_locator = old_indexes.get(existing.key.as_slice())?.unwrap();
    assert_eq!(
        IndexLocator::decode_for_key(&existing.key, old_locator.value())?.row_version,
        1
    );
    let reverse_key = {
        let mut key = row_key.to_vec();
        key.extend_from_slice(&existing.key[..existing.key.len() - 16]);
        key
    };
    assert!(
        old_snapshot
            .open_table(schema::ROW_INDEXES)?
            .get(reverse_key.as_slice())?
            .is_some()
    );
    assert!(old_indexes.get(rebuilt.key.as_slice())?.is_none());

    let current = database.begin_read()?;
    assert_eq!(
        current
            .open_table(CATALOG)?
            .get(index_catalog_key.as_slice())?
            .unwrap()
            .value(),
        unique_definition
    );
    let locator = current
        .open_table(schema::INDEXES)?
        .get(rebuilt.key.as_slice())?
        .unwrap();
    assert_eq!(
        IndexLocator::decode_for_key(&rebuilt.key, locator.value())?.row_version,
        1
    );
    assert!(
        current
            .open_table(schema::INDEXES)?
            .get(existing.key.as_slice())?
            .is_none()
    );
    let rebuilt_reverse_key = {
        let mut key = row_key.to_vec();
        key.extend_from_slice(&rebuilt.key[..rebuilt.key.len() - 16]);
        key
    };
    assert!(
        current
            .open_table(schema::ROW_INDEXES)?
            .get(reverse_key.as_slice())?
            .is_none()
    );
    assert!(
        current
            .open_table(schema::ROW_INDEXES)?
            .get(rebuilt_reverse_key.as_slice())?
            .is_some()
    );
    assert_eq!(
        lookup_outcome(&database, id(205), index_admin, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(current);
    drop(old_snapshot);
    let mut repeat_rebuild_audit = audit_event(206);
    repeat_rebuild_audit.actor_id = Some(index_admin);
    repeat_rebuild_audit.correlation_id = id(206);
    let repeat_rebuild = storage
        .rebuild_index(prdb_engine::storage_manager::RebuildIndexRequest {
            invocation_id: id(206),
            authenticated_actor_id: index_admin,
            index_id: id(0x44),
            column_ids: vec![id(0x67)],
            unique: true,
            retention_deadline_us: 9_999,
            audit_event: repeat_rebuild_audit,
        })
        .map_err(|error| format!("idempotent typed index rebuild failed: {error:?}"))?;
    assert_eq!(repeat_rebuild.commit_sequence, 3);
    assert_eq!(
        lookup_outcome(&database, id(206), index_admin, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_rejects_unique_rebuild_with_duplicate_values()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let payload = bytea_tuple(b"duplicate rebuild");
    let row_keys = [table_row_key(id(16), id(74)), table_row_key(id(16), id(75))];
    let indexes = row_keys
        .iter()
        .map(|row_key| {
            let derived = typed_bytea_index_key(id(0x44), row_key, &payload);
            IndexWrite {
                key: derived.key,
                row_key: *row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }
        })
        .collect::<Vec<_>>();
    commit_rows_and_indexes(
        &database,
        id(206),
        id(9),
        9_999,
        &row_keys
            .iter()
            .map(|row_key| RowWrite {
                key: *row_key,
                expected_version: None,
                payload: Some(payload.clone()),
            })
            .collect::<Vec<_>>(),
        &indexes,
    )?;

    let index_catalog_key = id_catalog_key(9, id(0x44));
    let old_definition = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(index_catalog_key.as_slice())?
        .unwrap()
        .value()
        .to_vec();
    let (_, unique_definition) = index_definition_with_options(id(0x44), id(16), &[id(0x66)], true);
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(207),
            id(9),
            9_999,
            &[CatalogWrite {
                key: index_catalog_key.clone(),
                expected_value: Some(old_definition.clone()),
                payload: Some(unique_definition),
            }],
            &[audit_event(207)],
        ),
        Err(CommitError::UniqueViolation)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(207), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert_eq!(
        read.open_table(CATALOG)?
            .get(index_catalog_key.as_slice())?
            .unwrap()
            .value(),
        old_definition
    );
    for index_write in &indexes {
        assert!(
            read.open_table(schema::INDEXES)?
                .get(index_write.key.as_slice())?
                .is_some()
        );
    }
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[207; 16][..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn storage_manager_checks_index_ddl_authority_for_current_actor()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let authorized_user = id(0x51);
    let denied_user = id(0x52);
    let admin_user = id(0x53);
    let table_owner = id(0x54);
    let mut records = vec![
        principal_write_with_name(authorized_user, 1, 1, "indexer"),
        object_write_with_name(authorized_user, 8, "indexer"),
        principal_write_with_name(denied_user, 1, 1, "no_index"),
        object_write_with_name(denied_user, 8, "no_index"),
        principal_write_with_name(admin_user, 1, 1, "database_admin_member"),
        object_write_with_name(admin_user, 8, "database_admin_member"),
        principal_write_with_name(table_owner, 1, 1, "table_owner"),
        object_write_with_name(table_owner, 8, "table_owner"),
        object_write_with_owner(id(16), 3, table_owner),
        grant_write(authorized_user, id(16), 9, 1, id(16), builtin_owner_id()),
        dependency_write(authorized_user, id(16), 5),
        membership_write(admin_user, builtin_owner_id()),
        dependency_write(admin_user, builtin_owner_id(), 5),
    ];
    {
        let setup = database.begin_write()?;
        for record in records.drain(..) {
            if record.key.starts_with(&10u16.to_be_bytes()) {
                let decoded = decode_registered_envelope(record.payload.as_ref().unwrap())?;
                let name = decoded
                    .fields
                    .iter()
                    .find(|field| field.tag == 2)
                    .ok_or("principal name missing")?;
                setup
                    .open_table(PRINCIPAL_NAMES)?
                    .insert(name.payload.as_slice(), &record.key[3..19])?;
            }
            setup
                .open_table(CATALOG)?
                .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
        }
        setup.commit()?;
    }

    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let (allowed_index_key, allowed_index_definition) = index_definition(id(0x8b), id(16));
    let mut allowed_audit = audit_event(210);
    allowed_audit.actor_id = Some(authorized_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(210),
            authorized_user,
            9_999,
            &[
                CatalogWrite {
                    key: allowed_index_key.clone(),
                    expected_value: None,
                    payload: Some(allowed_index_definition),
                },
                object_write(id(0x8b), 7),
            ],
            &[allowed_audit],
        )?,
        1
    );

    let (admin_index_key, admin_index_definition) = index_definition(id(0x8d), id(16));
    let mut admin_audit = audit_event(212);
    admin_audit.actor_id = Some(admin_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(212),
            admin_user,
            9_999,
            &[
                CatalogWrite {
                    key: admin_index_key.clone(),
                    expected_value: None,
                    payload: Some(admin_index_definition),
                },
                object_write(id(0x8d), 7),
            ],
            &[admin_audit],
        )?,
        2
    );

    let (owned_index_key, owned_index_definition) = index_definition(id(0x8e), id(16));
    let mut owner_audit = audit_event(214);
    owner_audit.actor_id = Some(table_owner);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(214),
            table_owner,
            9_999,
            &[
                CatalogWrite {
                    key: owned_index_key.clone(),
                    expected_value: None,
                    payload: Some(owned_index_definition),
                },
                object_write(id(0x8e), 7),
            ],
            &[owner_audit],
        )?,
        3
    );

    let allowed_index_definition_key = id_catalog_key(9, id(0x8b));
    let allowed_index_object_key = id_catalog_key(2, id(0x8b));
    let (old_index_definition, old_index_object) = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        (
            catalog
                .get(allowed_index_definition_key.as_slice())?
                .unwrap()
                .value()
                .to_vec(),
            catalog
                .get(allowed_index_object_key.as_slice())?
                .unwrap()
                .value()
                .to_vec(),
        )
    };
    let (_, unique_index_definition) =
        index_definition_with_options(id(0x8b), id(16), &[id(0x66)], true);
    let owner_transfer = object_write_with_owner(id(0x8b), 7, denied_user);
    let mut transfer_audit = audit_event(213);
    transfer_audit.actor_id = Some(authorized_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(213),
            authorized_user,
            9_999,
            &[
                CatalogWrite {
                    key: allowed_index_definition_key.clone(),
                    expected_value: Some(old_index_definition.clone()),
                    payload: Some(unique_index_definition),
                },
                CatalogWrite {
                    key: allowed_index_object_key,
                    expected_value: Some(old_index_object),
                    payload: owner_transfer.payload,
                },
            ],
            &[transfer_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let (denied_index_key, denied_index_definition) = index_definition(id(0x8c), id(16));
    let mut denied_audit = audit_event(211);
    denied_audit.actor_id = Some(denied_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(211),
            denied_user,
            9_999,
            &[
                CatalogWrite {
                    key: denied_index_key.clone(),
                    expected_value: None,
                    payload: Some(denied_index_definition),
                },
                object_write(id(0x8c), 7),
            ],
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 3);
    assert_eq!(
        lookup_outcome(&database, id(210), authorized_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(212), admin_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(214), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(213), authorized_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, id(211), denied_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let (narrow_guard_index_key, narrow_guard_index_definition) =
        index_definition(id(0x8f), id(16));
    let mut narrow_guard_audit = audit_event(215);
    narrow_guard_audit.actor_id = Some(authorized_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(215),
            authorized_user,
            9_999,
            &[
                CatalogWrite {
                    key: narrow_guard_index_key.clone(),
                    expected_value: None,
                    payload: Some(narrow_guard_index_definition),
                },
                object_write(id(0x8f), 7),
                dependency_write(id(0x8b), id(16), 1),
            ],
            &[narrow_guard_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        lookup_outcome(&database, id(215), authorized_user, 1)?
            .ok_or("missing rejected unrelated index ownership-edge outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(allowed_index_key.as_slice())?
            .is_some()
    );
    assert!(
        read.open_table(CATALOG)?
            .get(denied_index_key.as_slice())?
            .is_none()
    );
    assert!(
        read.open_table(CATALOG)?
            .get(narrow_guard_index_key.as_slice())?
            .is_none()
    );
    assert!(
        read.open_table(CATALOG)?
            .get(admin_index_key.as_slice())?
            .is_some()
    );
    assert!(
        read.open_table(CATALOG)?
            .get(owned_index_key.as_slice())?
            .is_some()
    );
    assert_eq!(
        read.open_table(CATALOG)?
            .get(allowed_index_definition_key.as_slice())?
            .unwrap()
            .value(),
        old_index_definition
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[211; 16][..])?
            .is_none()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[213; 16][..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn column_default_ddl_revalidates_after_table_owner_transfer()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AlterColumnDefaultRequest, CreateTableRequest, TableColumnDefault, TableColumnDefinition,
        TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let administrator = id(0xd0);
    let owner = id(0xd1);
    let successor = id(0xd2);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xd3, admin_role, owner);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xd3),
        admin_role,
        9_999,
        &[
            principal_write_with_name(administrator, 1, 1, "default_ddl_admin"),
            object_write_with_name(administrator, 8, "default_ddl_admin"),
            membership_write(administrator, admin_role),
            dependency_write(administrator, admin_role, 5),
            principal_write_with_name(owner, 1, 1, "default_ddl_owner"),
            object_write_with_name(owner, 8, "default_ddl_owner"),
            principal_write_with_name(successor, 1, 1, "default_ddl_successor"),
            object_write_with_name(successor, 8, "default_ddl_successor"),
        ],
        &[setup_audit],
    )?;
    let mut schema_audit = identity_audit_event(0xd4, administrator, owner);
    schema_audit.correlation_id = id(0xd4);
    let schema = storage.create_schema(
        id(0xd4),
        administrator,
        "default_ddl_schema",
        owner,
        9_999,
        schema_audit,
    )?;
    let mut table_audit = identity_audit_event(0xd5, owner, schema.schema_id);
    table_audit.correlation_id = id(0xd5);
    let table = storage.create_table(CreateTableRequest {
        invocation_id: id(0xd5),
        authenticated_actor_id: owner,
        schema_id: schema.schema_id,
        name: "default_ddl_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: table_audit,
    })?;

    let alter_invocation = id(0xd6);
    let transfer_invocation = id(0xd7);
    let before_race = commit_sequence(&database)?;
    let table_id = table.table_id;
    let column_id = table.column_ids[0];
    let barrier = Arc::new(Barrier::new(3));
    let (alter_result, transfer_result) = std::thread::scope(|scope| {
        let alter_barrier = Arc::clone(&barrier);
        let alter_storage = &storage;
        let alter = scope.spawn(move || {
            let mut audit = identity_audit_event(0xd8, owner, table.table_id);
            audit.correlation_id = alter_invocation;
            audit.action = AuditAction::Ddl;
            alter_barrier.wait();
            alter_storage.alter_column_default(AlterColumnDefaultRequest {
                invocation_id: alter_invocation,
                authenticated_actor_id: owner,
                table_id: table.table_id,
                column_id,
                default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(42)))),
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        let transfer_barrier = Arc::clone(&barrier);
        let transfer_storage = &storage;
        let transfer = scope.spawn(move || {
            let mut audit = identity_audit_event(0xd9, administrator, table.table_id);
            audit.correlation_id = transfer_invocation;
            audit.action = AuditAction::Ddl;
            transfer_barrier.wait();
            transfer_storage.set_table_owner(
                transfer_invocation,
                administrator,
                table.table_id,
                successor,
                9_999,
                audit,
            )
        });
        barrier.wait();
        (
            alter.join().expect("column default ALTER thread panicked"),
            transfer
                .join()
                .expect("table owner transfer thread panicked"),
        )
    });
    let alter_sequence = match alter_result {
        Ok(sequence) => Some(sequence),
        Err(
            TableLifecycleError::Unauthorized | TableLifecycleError::Commit(CommitError::Conflict),
        ) => None,
        Err(error) => return Err(format!("unexpected default/owner race: {error:?}").into()),
    };
    let (transfer_sequence, first_transfer_committed, retry_invocation) = match transfer_result {
        Ok(sequence) => (sequence, true, None),
        Err(TableLifecycleError::Commit(CommitError::Conflict)) => {
            let retry_invocation = id(0xda);
            let mut retry_audit = identity_audit_event(0xdb, administrator, table.table_id);
            retry_audit.correlation_id = retry_invocation;
            retry_audit.action = AuditAction::Ddl;
            (
                storage.set_table_owner(
                    retry_invocation,
                    administrator,
                    table.table_id,
                    successor,
                    9_999,
                    retry_audit,
                )?,
                false,
                Some(retry_invocation),
            )
        }
        Err(error) => return Err(format!("unexpected owner-transfer result: {error:?}").into()),
    };

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let table_object = decode_registered_envelope(
        catalog
            .get(id_catalog_key(2, table.table_id).as_slice())?
            .ok_or("table object missing after owner transfer")?
            .value(),
    )?;
    assert_eq!(
        table_object
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .ok_or("table owner missing after owner transfer")?
            .payload
            .as_slice(),
        successor
    );
    let column_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(6, column_id).as_slice())?
            .ok_or("column missing after owner transfer")?
            .value(),
    )?;
    let default_id = column_record
        .fields
        .iter()
        .find(|field| field.tag == 7)
        .map(|field| <[u8; 16]>::try_from(field.payload.as_slice()))
        .transpose()?;
    assert_eq!(default_id.is_some(), alter_sequence.is_some());
    if let Some(default_id) = default_id {
        let default_key = id_catalog_key(7, default_id);
        let default_record = decode_registered_envelope(
            catalog
                .get(default_key.as_slice())?
                .ok_or("default expression missing after owner transfer")?
                .value(),
        )?;
        let default_record =
            prdb_engine::expression::decode_catalog_expression_record(&default_record)?;
        assert_eq!(
            default_record.expression,
            prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Integer(42)))
        );
    }
    drop(catalog);
    drop(read);
    if let Some(sequence) = alter_sequence {
        assert!(sequence < transfer_sequence);
        assert_eq!(
            lookup_outcome(&database, alter_invocation, owner, 1)?
                .ok_or("missing successful default ALTER outcome")?
                .status,
            OutcomeStatus::Committed
        );
    } else {
        assert_eq!(
            lookup_outcome(&database, alter_invocation, owner, 1)?
                .ok_or("missing rejected default ALTER outcome")?
                .status,
            OutcomeStatus::Aborted
        );
    }
    assert_eq!(
        lookup_outcome(&database, transfer_invocation, administrator, 1)?
            .ok_or("missing owner-transfer outcome")?
            .status,
        if first_transfer_committed {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    if let Some(retry_invocation) = retry_invocation {
        assert_eq!(
            lookup_outcome(&database, retry_invocation, administrator, 1)?
                .ok_or("missing retried owner-transfer outcome")?
                .status,
            OutcomeStatus::Committed
        );
    }
    assert_eq!(
        commit_sequence(&database)?,
        before_race + 1 + u64::from(alter_sequence.is_some())
    );

    let before_denied_alter = commit_sequence(&database)?;
    let denied_invocation = id(0xdc);
    let mut denied_audit = identity_audit_event(0xdd, owner, table.table_id);
    denied_audit.correlation_id = denied_invocation;
    denied_audit.action = AuditAction::Ddl;
    assert_eq!(
        storage.alter_column_default(AlterColumnDefaultRequest {
            invocation_id: denied_invocation,
            authenticated_actor_id: owner,
            table_id,
            column_id,
            default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(43)))),
            retention_deadline_us: 9_999,
            audit_event: denied_audit,
        }),
        Err(TableLifecycleError::Unauthorized)
    );
    assert_eq!(commit_sequence(&database)?, before_denied_alter);
    assert_eq!(
        lookup_outcome(&database, denied_invocation, owner, 1)?
            .ok_or("missing rejected former-owner ALTER outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn index_ddl_racing_table_owner_transfer_rechecks_authority_at_commit()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, SchemaLifecycleError, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let owner = id(0xb1);
    let administrator = id(0xb2);
    let successor = id(0xb3);
    let mut setup_audit = identity_audit_event(0xb4, admin_role, owner);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xb4),
        admin_role,
        9_999,
        &[
            principal_write_with_name(owner, 1, 1, "index_race_owner"),
            object_write_with_name(owner, 8, "index_race_owner"),
            principal_write_with_name(administrator, 1, 1, "index_race_admin"),
            object_write_with_name(administrator, 8, "index_race_admin"),
            membership_write(administrator, admin_role),
            dependency_write(administrator, admin_role, 5),
            principal_write_with_name(successor, 1, 1, "index_race_successor"),
            object_write_with_name(successor, 8, "index_race_successor"),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0xb5);
    let mut schema_audit = identity_audit_event(0xb6, administrator, owner);
    schema_audit.correlation_id = schema_invocation;
    let schema = storage
        .create_schema(
            schema_invocation,
            administrator,
            "index_race_schema",
            owner,
            9_999,
            schema_audit,
        )
        .map_err(|error: SchemaLifecycleError| format!("schema setup failed: {error:?}"))?;
    let table_invocation = id(0xb7);
    let mut table_audit = identity_audit_event(0xb8, owner, schema.schema_id);
    table_audit.correlation_id = table_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: owner,
        schema_id: schema.schema_id,
        name: "indexed_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Bytea,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: table_audit,
    })?;

    let index_id = id(0xb9);
    let (index_key, index_payload) =
        index_definition_with_columns(index_id, table.table_id, &table.column_ids);
    let index_key_for_commit = index_key.clone();
    let index_invocation = id(0xba);
    let transfer_invocation = id(0xbb);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (index_result, transfer_result) = std::thread::scope(|scope| {
        let index_barrier = barrier.clone();
        let index_storage = &storage;
        let index = scope.spawn(move || {
            let mut audit = identity_audit_event(0xbc, owner, table.table_id);
            audit.correlation_id = index_invocation;
            index_barrier.wait();
            index_storage.commit_authorized_index_ddl(
                index_invocation,
                owner,
                9_999,
                &[
                    CatalogWrite {
                        key: index_key_for_commit,
                        expected_value: None,
                        payload: Some(index_payload),
                    },
                    object_write_with_owner(index_id, 7, owner),
                ],
                &[audit],
            )
        });
        let transfer_barrier = barrier.clone();
        let transfer_storage = &storage;
        let transfer = scope.spawn(move || {
            let mut audit = identity_audit_event(0xbd, administrator, table.table_id);
            audit.correlation_id = transfer_invocation;
            transfer_barrier.wait();
            transfer_storage.set_table_owner(
                transfer_invocation,
                administrator,
                table.table_id,
                successor,
                9_999,
                audit,
            )
        });
        barrier.wait();
        (
            index.join().expect("index DDL thread panicked"),
            transfer.join().expect("owner transfer thread panicked"),
        )
    });

    let (index_status, transfer_status) = match (index_result, transfer_result) {
        (Ok(_), Ok(_)) => (OutcomeStatus::Committed, OutcomeStatus::Committed),
        (Ok(_), Err(TableLifecycleError::Commit(CommitError::Conflict))) => {
            (OutcomeStatus::Committed, OutcomeStatus::Aborted)
        }
        (Err(CommitError::DdlAuthorizationDenied), Ok(_)) => {
            (OutcomeStatus::Aborted, OutcomeStatus::Committed)
        }
        (index, transfer) => {
            panic!("unexpected index/owner-transfer race: index={index:?}, transfer={transfer:?}")
        }
    };
    assert_eq!(
        commit_sequence(&database)?,
        before_race
            + u64::from(index_status == OutcomeStatus::Committed)
            + u64::from(transfer_status == OutcomeStatus::Committed)
    );
    assert_eq!(
        lookup_outcome(&database, index_invocation, owner, 1)?
            .ok_or("missing raced index outcome")?
            .status,
        index_status
    );
    assert_eq!(
        lookup_outcome(&database, transfer_invocation, administrator, 1)?
            .ok_or("missing raced owner-transfer outcome")?
            .status,
        transfer_status
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert_eq!(
        catalog.get(index_key.as_slice())?.is_some(),
        index_status == OutcomeStatus::Committed
    );
    let table_object = decode_registered_envelope(
        catalog
            .get(id_catalog_key(2, table.table_id).as_slice())?
            .ok_or("table object missing after owner race")?
            .value(),
    )?;
    let final_owner = table_object
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("table owner missing after owner race")?
        .payload
        .as_slice();
    assert_eq!(
        final_owner,
        if transfer_status == OutcomeStatus::Committed {
            successor.as_slice()
        } else {
            owner.as_slice()
        }
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn policy_ddl_racing_table_owner_transfer_rechecks_authority_at_commit()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, SchemaLifecycleError, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let owner = id(0xc1);
    let administrator = id(0xc2);
    let successor = id(0xc3);
    let mut setup_audit = identity_audit_event(0xc4, admin_role, owner);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xc4),
        admin_role,
        9_999,
        &[
            principal_write_with_name(owner, 1, 1, "policy_race_owner"),
            object_write_with_name(owner, 8, "policy_race_owner"),
            principal_write_with_name(administrator, 1, 1, "policy_race_admin"),
            object_write_with_name(administrator, 8, "policy_race_admin"),
            membership_write(administrator, admin_role),
            dependency_write(administrator, admin_role, 5),
            principal_write_with_name(successor, 1, 1, "policy_race_successor"),
            object_write_with_name(successor, 8, "policy_race_successor"),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0xc5);
    let mut schema_audit = identity_audit_event(0xc6, administrator, owner);
    schema_audit.correlation_id = schema_invocation;
    let schema = storage
        .create_schema(
            schema_invocation,
            administrator,
            "policy_race_schema",
            owner,
            9_999,
            schema_audit,
        )
        .map_err(|error: SchemaLifecycleError| format!("schema setup failed: {error:?}"))?;
    let table_invocation = id(0xc7);
    let mut table_audit = identity_audit_event(0xc8, owner, schema.schema_id);
    table_audit.correlation_id = table_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: owner,
        schema_id: schema.schema_id,
        name: "policy_race_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Bytea,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: table_audit,
    })?;

    let policy_invocation = id(0xc9);
    let transfer_invocation = id(0xca);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (policy_result, transfer_result) = std::thread::scope(|scope| {
        let policy_barrier = barrier.clone();
        let policy_storage = &storage;
        let policy = scope.spawn(move || {
            let mut audit = identity_audit_event(0xcb, owner, table.table_id);
            audit.correlation_id = policy_invocation;
            policy_barrier.wait();
            policy_storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
                invocation_id: policy_invocation,
                authenticated_actor_id: owner,
                table_id: table.table_id,
                name: "owner_policy_race".to_owned(),
                kind: prdb_engine::storage_manager::PolicyKind::Row,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Allow,
                recipients: vec![prdb_engine::authorization::Subject::Principal(owner)],
                target_column_ids: Vec::new(),
                using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(true),
                ))),
                with_check_expression: None,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        let transfer_barrier = barrier.clone();
        let transfer_storage = &storage;
        let transfer = scope.spawn(move || {
            let mut audit = identity_audit_event(0xcc, administrator, table.table_id);
            audit.correlation_id = transfer_invocation;
            transfer_barrier.wait();
            transfer_storage.set_table_owner(
                transfer_invocation,
                administrator,
                table.table_id,
                successor,
                9_999,
                audit,
            )
        });
        barrier.wait();
        (
            policy.join().expect("policy DDL thread panicked"),
            transfer.join().expect("owner-transfer thread panicked"),
        )
    });

    let (policy_status, policy_id, transfer_status) = match (policy_result, transfer_result) {
        (Ok(created), Ok(_)) => (
            OutcomeStatus::Committed,
            Some(created.policy_id),
            OutcomeStatus::Committed,
        ),
        (Ok(created), Err(TableLifecycleError::Commit(CommitError::Conflict))) => (
            OutcomeStatus::Committed,
            Some(created.policy_id),
            OutcomeStatus::Aborted,
        ),
        (
            Err(
                TableLifecycleError::Unauthorized
                | TableLifecycleError::Commit(CommitError::Conflict),
            ),
            Ok(_),
        ) => (OutcomeStatus::Aborted, None, OutcomeStatus::Committed),
        (policy, transfer) => {
            panic!(
                "unexpected policy/owner-transfer race: policy={policy:?}, transfer={transfer:?}"
            )
        }
    };
    assert_eq!(
        commit_sequence(&database)?,
        before_race
            + u64::from(policy_status == OutcomeStatus::Committed)
            + u64::from(transfer_status == OutcomeStatus::Committed)
    );
    assert_eq!(
        lookup_outcome(&database, policy_invocation, owner, 1)?
            .ok_or("missing raced policy outcome")?
            .status,
        policy_status
    );
    assert_eq!(
        lookup_outcome(&database, transfer_invocation, administrator, 1)?
            .ok_or("missing raced owner-transfer outcome")?
            .status,
        transfer_status
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    if let Some(policy_id) = policy_id {
        assert!(
            catalog
                .get(id_catalog_key(13, policy_id).as_slice())?
                .is_some()
        );
    }
    let table_object = decode_registered_envelope(
        catalog
            .get(id_catalog_key(2, table.table_id).as_slice())?
            .ok_or("table object missing after policy race")?
            .value(),
    )?;
    let final_owner = table_object
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("table owner missing after policy race")?
        .payload
        .as_slice();
    assert_eq!(
        final_owner,
        if transfer_status == OutcomeStatus::Committed {
            successor.as_slice()
        } else {
            owner.as_slice()
        }
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}
