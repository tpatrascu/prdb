use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::lookup_index_id_by_name;
use prdb_engine::commit::{CatalogWrite, CommitError, commit_catalog_and_audit};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ROWS};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ScalarValue, decode_tuple, encode_tuple};
use redb::{ReadableDatabase, ReadableTable};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn foreign_key_creation_racing_a_staged_child_write_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::CreateForeignKeyRequest;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0x9a);
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_child_write_race_admin"),
        object_write_with_name(actor, 8, "fk_child_write_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0x9b, admin_role, id(0x9c));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x9c), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let parent_seed_invocation = id(0xa1);
    let mut parent_seed =
        storage.begin_invocation(parent_seed_invocation, actor, 9_999, 1, 1024)?;
    let target_schema = parent_seed.load_catalog_table_schema(id(81))?;
    parent_seed.load_catalog_indexes(&target_schema)?;
    parent_seed.load_catalog_check_constraints(&target_schema)?;
    let referenced_value = ScalarValue::Bytea(b"existing parent".to_vec());
    let parent_payload = encode_tuple(
        &target_schema.columns,
        target_schema.schema_version,
        &[Some(referenced_value.clone())],
    )?;
    let parent_row_id = id(0xa2);
    parent_seed.stage_put(
        prdb_engine::overlay::RowId {
            table: id(81),
            row: parent_row_id,
        },
        &parent_payload,
    )?;
    parent_seed.commit()?;

    let writer_invocation = id(0x9d);
    let mut writer = storage.begin_invocation(writer_invocation, actor, 9_999, 1, 1024)?;
    let source_schema = writer.load_catalog_table_schema(id(82))?;
    writer.load_catalog_indexes(&source_schema)?;
    writer.load_catalog_check_constraints(&source_schema)?;
    let child_payload = encode_tuple(
        &source_schema.columns,
        source_schema.schema_version,
        &[Some(referenced_value)],
    )?;
    writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0x9e),
        },
        &child_payload,
    )?;

    let fk_invocation = id(0x9f);
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, fk_result) = std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer_task = scope.spawn(move || {
            writer_barrier.wait();
            writer.commit()
        });
        let ddl_barrier = barrier.clone();
        let ddl_storage = &storage;
        let ddl_task = scope.spawn(move || {
            let mut audit = identity_audit_event(0xa0, actor, id(82));
            audit.correlation_id = fk_invocation;
            ddl_barrier.wait();
            ddl_storage.create_foreign_key(CreateForeignKeyRequest {
                invocation_id: fk_invocation,
                authenticated_actor_id: actor,
                source_table_id: id(82),
                source_column_ids: vec![id(84)],
                target_constraint_id: id(85),
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        barrier.wait();
        (writer_task.join().unwrap(), ddl_task.join().unwrap())
    });

    let writer_status = match writer_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        other => panic!("unexpected child writer result: {other:?}"),
    };
    let created = fk_result.map_err(|error| format!("FK creation race failed: {error:?}"))?;
    assert_eq!(
        lookup_outcome(&database, writer_invocation, actor, 1)?
            .ok_or("missing staged child writer outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, fk_invocation, actor, 1)?
            .ok_or("missing FK creation outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        commit_sequence(&database)?,
        if writer_status == OutcomeStatus::Committed {
            4
        } else {
            3
        },
        "a stale child writer must not advance the commit sequence"
    );
    assert_eq!(created.commit_sequence, commit_sequence(&database)?);
    assert_eq!(
        row(&database, &table_row_key(id(82), id(0x9e)),)?.is_some(),
        writer_status == OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(8, created.foreign_key_id).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(
                dependency_write(created.foreign_key_id, id(85), 3)
                    .key
                    .as_slice()
            )?
            .is_some()
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn foreign_key_drop_racing_a_staged_child_write_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0x9a);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_drop_child_write_race_admin"),
        object_write_with_name(actor, 8, "fk_drop_child_write_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0x9b, admin_role, id(0x9c));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x9c), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let referenced_value = ScalarValue::Bytea(b"existing parent".to_vec());
    let mut parent_seed = storage.begin_invocation(id(0xa1), actor, 9_999, 1, 1024)?;
    let target_schema = parent_seed.load_catalog_table_schema(id(81))?;
    parent_seed.load_catalog_indexes(&target_schema)?;
    parent_seed.load_catalog_check_constraints(&target_schema)?;
    let parent_payload = encode_tuple(
        &target_schema.columns,
        target_schema.schema_version,
        &[Some(referenced_value.clone())],
    )?;
    parent_seed.stage_put(
        prdb_engine::overlay::RowId {
            table: id(81),
            row: id(0xa2),
        },
        &parent_payload,
    )?;
    parent_seed.commit()?;

    let writer_invocation = id(0x9d);
    let mut writer = storage.begin_invocation(writer_invocation, actor, 9_999, 1, 1024)?;
    let source_schema = writer.load_catalog_table_schema(id(82))?;
    writer.load_catalog_indexes(&source_schema)?;
    writer.load_catalog_check_constraints(&source_schema)?;
    let child_payload = encode_tuple(
        &source_schema.columns,
        source_schema.schema_version,
        &[Some(referenced_value)],
    )?;
    writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0x9e),
        },
        &child_payload,
    )?;

    let drop_invocation = id(0x9f);
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, drop_result) = std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer_task = scope.spawn(move || {
            writer_barrier.wait();
            writer.commit()
        });
        let ddl_barrier = barrier.clone();
        let ddl_storage = &storage;
        let ddl_task = scope.spawn(move || {
            let mut audit = identity_audit_event(0xa0, actor, id(86));
            audit.correlation_id = drop_invocation;
            ddl_barrier.wait();
            ddl_storage.drop_foreign_key(drop_invocation, actor, id(86), 9_999, audit)
        });
        barrier.wait();
        (writer_task.join().unwrap(), ddl_task.join().unwrap())
    });

    let writer_status = match writer_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        other => panic!("unexpected child writer result: {other:?}"),
    };
    let drop_sequence = drop_result.map_err(|error| format!("FK drop race failed: {error:?}"))?;
    assert_eq!(
        lookup_outcome(&database, writer_invocation, actor, 1)?
            .ok_or("missing staged child writer outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing FK drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        commit_sequence(&database)?,
        if writer_status == OutcomeStatus::Committed {
            4
        } else {
            3
        },
        "a stale child writer must not advance the commit sequence"
    );
    assert_eq!(drop_sequence, commit_sequence(&database)?);
    assert_eq!(
        row(&database, &table_row_key(id(82), id(0x9e)),)?.is_some(),
        writer_status == OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(id_catalog_key(8, id(86)).as_slice())?.is_none());
    assert!(catalog.get(id_catalog_key(2, id(86)).as_slice())?.is_none());
    assert!(
        catalog
            .get(dependency_write(id(86), id(85), 3).key.as_slice())?
            .is_none()
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_atomically_creates_table_with_foreign_key_to_existing_key()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateIndexRequest, CreateTableForeignKeyDefinition, CreateTableRequest,
        RebuildIndexRequest, RenameIndexRequest, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xea);
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_create_table_admin"),
        object_write_with_name(actor, 8, "fk_create_table_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0xeb, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xeb), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut probe = storage.begin_invocation(id(0xec), actor, 9_999, 1, 1024)?;
    let target_schema = probe.load_catalog_table_schema(id(81))?;
    let target_type = target_schema.columns[0].scalar_type;
    drop(probe);

    let invocation_id = id(0xed);
    let mut audit = identity_audit_event(0xee, actor, id(80));
    audit.correlation_id = invocation_id;
    let created_result = storage.create_table_with_foreign_keys(
        CreateTableRequest {
            invocation_id,
            authenticated_actor_id: actor,
            schema_id: id(80),
            name: "new_child".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "parent_id".to_owned(),
                scalar_type: target_type,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: 9_999,
            audit_event: audit,
        },
        None,
        &[],
        &[],
        &[CreateTableForeignKeyDefinition {
            source_columns: vec!["parent_id".to_owned()],
            target_constraint_id: id(85),
            target_columns: None,
        }],
    );
    let created = created_result
        .map_err(|error| format!("atomic CREATE TABLE FOREIGN KEY failed: {error:?}"))?;
    assert_eq!(created.commit_sequence, 2);
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing CREATE TABLE FK outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let self_reference_invocation = id(0xf0);
    let mut self_reference_audit = identity_audit_event(0xf1, actor, id(80));
    self_reference_audit.correlation_id = self_reference_invocation;
    let self_referencing = storage
        .create_table_with_foreign_keys(
            CreateTableRequest {
                invocation_id: self_reference_invocation,
                authenticated_actor_id: actor,
                schema_id: id(80),
                name: "self_referencing".to_owned(),
                columns: vec![
                    TableColumnDefinition {
                        name: "id".to_owned(),
                        scalar_type: target_type,
                        nullable: false,
                        generated_uuid_identity: false,
                        default: None,
                    },
                    TableColumnDefinition {
                        name: "parent_id".to_owned(),
                        scalar_type: target_type,
                        nullable: true,
                        generated_uuid_identity: false,
                        default: None,
                    },
                ],
                retention_deadline_us: 9_999,
                audit_event: self_reference_audit,
            },
            Some(&["id".to_owned()]),
            &[],
            &[],
            &[CreateTableForeignKeyDefinition {
                source_columns: vec!["parent_id".to_owned()],
                target_constraint_id: [0; 16],
                target_columns: Some(vec!["id".to_owned()]),
            }],
        )
        .map_err(|error| format!("atomic self-referencing CREATE TABLE failed: {error:?}"))?;
    assert_eq!(self_referencing.commit_sequence, 3);
    let create_index_invocation = id(0xf2);
    let mut create_index_audit = identity_audit_event(0xf3, actor, id(80));
    create_index_audit.correlation_id = create_index_invocation;
    let created_index = storage.create_index(CreateIndexRequest {
        invocation_id: create_index_invocation,
        authenticated_actor_id: actor,
        table_id: created.table_id,
        name: "post_self_fk_index".to_owned(),
        column_ids: vec![created.column_ids[0]],
        unique: false,
        retention_deadline_us: 9_999,
        audit_event: create_index_audit,
    })?;
    let rename_index_invocation = id(0xf6);
    let mut rename_index_audit = identity_audit_event(0xf7, actor, id(80));
    rename_index_audit.correlation_id = rename_index_invocation;
    let renamed_index = storage.rename_index(RenameIndexRequest {
        invocation_id: rename_index_invocation,
        authenticated_actor_id: actor,
        index_id: created_index.index_id,
        name: "renamed_post_self_fk_index".to_owned(),
        retention_deadline_us: 9_999,
        audit_event: rename_index_audit,
    });
    assert!(
        renamed_index.is_ok(),
        "ordinary index rename failed: {renamed_index:?}"
    );
    let renamed_index = renamed_index.unwrap();
    assert_eq!(renamed_index.index_id, created_index.index_id);
    assert_eq!(
        renamed_index.commit_sequence,
        created_index.commit_sequence + 1
    );
    let renamed_index_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_index_id_by_name(&renamed_index_snapshot, id(80), "post_self_fk_index")?,
        None
    );
    assert_eq!(
        lookup_index_id_by_name(
            &renamed_index_snapshot,
            id(80),
            "renamed_post_self_fk_index"
        )?,
        Some(created_index.index_id)
    );
    let rebuild_index_invocation = id(0xf4);
    let mut rebuild_index_audit = identity_audit_event(0xf5, actor, id(80));
    rebuild_index_audit.correlation_id = rebuild_index_invocation;
    let rebuilt_index = storage.rebuild_index(RebuildIndexRequest {
        invocation_id: rebuild_index_invocation,
        authenticated_actor_id: actor,
        index_id: created_index.index_id,
        column_ids: vec![created.column_ids[0]],
        unique: false,
        retention_deadline_us: 9_999,
        audit_event: rebuild_index_audit,
    });
    assert!(
        rebuilt_index.is_ok(),
        "unrelated index rebuild after self-referencing CREATE TABLE failed: {rebuilt_index:?}"
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let new_fk = catalog
        .iter()?
        .filter_map(|entry| entry.ok())
        .find_map(|(key, value)| {
            let key = key.value();
            if key.len() == 19 && key[..2] == 8u16.to_be_bytes() && key[2] == 1 {
                let record = decode_registered_envelope(value.value()).ok()?;
                let source_table = record.fields.iter().find(|field| field.tag == 1)?;
                (source_table.payload.as_slice() == created.table_id)
                    .then(|| <[u8; 16]>::try_from(&key[3..19]).ok())?
            } else {
                None
            }
        })
        .ok_or("new table FOREIGN KEY record missing")?;
    assert!(
        catalog
            .get(dependency_write(new_fk, id(85), 3).key.as_slice())?
            .is_some()
    );
    let target_table = decode_registered_envelope(
        catalog
            .get(id_catalog_key(5, id(81)).as_slice())?
            .ok_or("target table missing")?
            .value(),
    )?;
    let target_epoch = u64::from_le_bytes(
        target_table
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("target table epoch missing")?
            .payload
            .as_slice()
            .try_into()?,
    );
    assert_eq!(target_epoch, 2);
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn foreign_key_creation_racing_orphan_child_insert_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateForeignKeyRequest, TableLifecycleError};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe9);
    let fk_edge = dependency_write(id(86), id(85), 3).key;
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != fk_edge
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_insert_race_admin"),
        object_write_with_name(actor, 8, "fk_insert_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0xea, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xea), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let insert_invocation = id(0xeb);
    let insert_row = id(0xec);
    let mut insert = storage.begin_invocation(insert_invocation, actor, 9_999, 1, 1024)?;
    let source_schema = insert.load_catalog_table_schema(id(82))?;
    insert.load_catalog_indexes(&source_schema)?;
    let orphan = encode_tuple(
        &source_schema.columns,
        source_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"missing parent".to_vec()))],
    )?;
    insert.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: insert_row,
        },
        &orphan,
    )?;

    let fk_invocation = id(0xed);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (insert_result, fk_result) = std::thread::scope(|scope| {
        let insert_barrier = barrier.clone();
        let insert_thread = scope.spawn(move || {
            insert_barrier.wait();
            insert.commit()
        });
        let fk_barrier = barrier.clone();
        let fk_storage = &storage;
        let fk_thread = scope.spawn(move || {
            let mut audit = identity_audit_event(0xee, actor, id(82));
            audit.correlation_id = fk_invocation;
            fk_barrier.wait();
            fk_storage.create_foreign_key(CreateForeignKeyRequest {
                invocation_id: fk_invocation,
                authenticated_actor_id: actor,
                source_table_id: id(82),
                source_column_ids: vec![id(84)],
                target_constraint_id: id(85),
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        barrier.wait();
        (
            insert_thread
                .join()
                .expect("FK race insert thread panicked"),
            fk_thread.join().expect("FK race DDL thread panicked"),
        )
    });

    let (insert_status, fk_status, created_fk) = match (insert_result, fk_result) {
        (Ok(_), Err(TableLifecycleError::Commit(CommitError::ForeignKeyViolation))) => {
            (OutcomeStatus::Committed, OutcomeStatus::Aborted, None)
        }
        (
            Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)),
            Ok(created),
        ) => (
            OutcomeStatus::Aborted,
            OutcomeStatus::Committed,
            Some(created.foreign_key_id),
        ),
        (insert, fk) => panic!("unexpected FK-create/orphan-insert race: {insert:?} / {fk:?}"),
    };
    assert_eq!(commit_sequence(&database)?, before_race + 1);
    assert_eq!(
        lookup_outcome(&database, insert_invocation, actor, 1)?
            .ok_or("missing FK-race insert outcome")?
            .status,
        insert_status
    );
    assert_eq!(
        lookup_outcome(&database, fk_invocation, actor, 1)?
            .ok_or("missing FK-race DDL outcome")?
            .status,
        fk_status
    );
    assert_eq!(
        row(&database, &table_row_key(id(82), insert_row))?.is_some(),
        insert_status == OutcomeStatus::Committed
    );
    if let Some(foreign_key_id) = created_fk {
        let read = database.begin_read()?;
        assert!(
            read.open_table(CATALOG)?
                .get(id_catalog_key(8, foreign_key_id).as_slice())?
                .is_some()
        );
        drop(read);
        let drop_invocation = id(0xef);
        let mut drop_audit = identity_audit_event(0xf0, actor, foreign_key_id);
        drop_audit.correlation_id = drop_invocation;
        storage.drop_foreign_key(drop_invocation, actor, foreign_key_id, 9_999, drop_audit)?;
    }
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn foreign_key_creation_racing_row_policy_creation_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateForeignKeyRequest, TableLifecycleError};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xf1);
    let foreign_key_id = id_catalog_key(8, id(86));
    let foreign_key_object_id = id_catalog_key(2, id(86));
    let foreign_key_edge = dependency_write(id(86), id(85), 3).key;
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != foreign_key_id
                && write.key != foreign_key_object_id
                && write.key != foreign_key_edge
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_policy_race_admin"),
        object_write_with_name(actor, 8, "fk_policy_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0xf2, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xf2), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let foreign_key_invocation = id(0xf3);
    let policy_invocation = id(0xf4);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (foreign_key_result, policy_result) = std::thread::scope(|scope| {
        let foreign_key_barrier = barrier.clone();
        let foreign_key_storage = &storage;
        let foreign_key_thread = scope.spawn(move || {
            let mut audit = identity_audit_event(0xf5, actor, id(82));
            audit.correlation_id = foreign_key_invocation;
            foreign_key_barrier.wait();
            foreign_key_storage.create_foreign_key(CreateForeignKeyRequest {
                invocation_id: foreign_key_invocation,
                authenticated_actor_id: actor,
                source_table_id: id(82),
                source_column_ids: vec![id(84)],
                target_constraint_id: id(85),
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        let policy_barrier = barrier.clone();
        let policy_storage = &storage;
        let policy_thread = scope.spawn(move || {
            let mut audit = identity_audit_event(0xf6, actor, id(82));
            audit.correlation_id = policy_invocation;
            policy_barrier.wait();
            policy_storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
                invocation_id: policy_invocation,
                authenticated_actor_id: actor,
                table_id: id(82),
                name: "fk_policy_race_row_policy".to_owned(),
                kind: prdb_engine::storage_manager::PolicyKind::Row,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Allow,
                recipients: vec![prdb_engine::authorization::Subject::Principal(actor)],
                target_column_ids: Vec::new(),
                using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(true),
                ))),
                with_check_expression: None,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        barrier.wait();
        (
            foreign_key_thread
                .join()
                .expect("foreign-key race thread panicked"),
            policy_thread.join().expect("policy race thread panicked"),
        )
    });

    let (foreign_key_status, created_foreign_key_id) = match foreign_key_result {
        Ok(created) => (OutcomeStatus::Committed, Some(created.foreign_key_id)),
        Err(TableLifecycleError::Commit(CommitError::Conflict)) => (OutcomeStatus::Aborted, None),
        Err(error) => return Err(format!("unexpected FK/policy race result: {error:?}").into()),
    };
    let (policy_status, created_policy) = match policy_result {
        Ok(created) => (OutcomeStatus::Committed, Some(created)),
        Err(TableLifecycleError::Commit(CommitError::Conflict)) => (OutcomeStatus::Aborted, None),
        Err(error) => return Err(format!("unexpected FK/policy race result: {error:?}").into()),
    };
    assert!(
        foreign_key_status == OutcomeStatus::Committed || policy_status == OutcomeStatus::Committed,
        "at least one fenced DDL operation must publish"
    );
    let committed_count = u64::from(foreign_key_status == OutcomeStatus::Committed)
        + u64::from(policy_status == OutcomeStatus::Committed);
    assert_eq!(commit_sequence(&database)?, before_race + committed_count);
    for (invocation_id, status) in [
        (foreign_key_invocation, foreign_key_status),
        (policy_invocation, policy_status),
    ] {
        assert_eq!(
            lookup_outcome(&database, invocation_id, actor, 1)?
                .ok_or("missing FK/policy race outcome")?
                .status,
            status
        );
    }

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    if let Some(foreign_key_id) = created_foreign_key_id {
        for key in [
            id_catalog_key(8, foreign_key_id),
            id_catalog_key(2, foreign_key_id),
            dependency_write(foreign_key_id, id(85), 3).key,
        ] {
            assert!(
                catalog.get(key.as_slice())?.is_some(),
                "committed foreign key is missing {key:?}"
            );
        }
    } else {
        assert!(catalog.get(foreign_key_id.as_slice())?.is_none());
        assert!(catalog.get(foreign_key_object_id.as_slice())?.is_none());
        assert!(catalog.get(foreign_key_edge.as_slice())?.is_none());
    }
    let mut source_policy_count = 0usize;
    let policy_prefix = 13u16.to_be_bytes();
    for entry in catalog.range(policy_prefix.as_slice()..)? {
        let (key, value) = entry?;
        let key = key.value();
        if !key.starts_with(&policy_prefix) {
            break;
        }
        let policy_record = decode_registered_envelope(value.value())?;
        if policy_record
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .ok_or("policy table ID is missing")?
            .payload
            == id(82)
        {
            source_policy_count += 1;
        }
    }
    assert_eq!(
        source_policy_count,
        usize::from(policy_status == OutcomeStatus::Committed),
        "a failed policy DDL race must not leave a partial definition"
    );
    if let Some(policy) = created_policy {
        assert!(
            catalog
                .get(id_catalog_key(13, policy.policy_id).as_slice())?
                .is_some()
        );
        assert!(
            catalog
                .get(id_catalog_key(2, policy.policy_id).as_slice())?
                .is_some()
        );
        let expression_id = policy
            .using_expression_id
            .ok_or("committed row policy is missing its expression")?;
        assert!(
            catalog
                .get(id_catalog_key(7, expression_id).as_slice())?
                .is_some()
        );
    }
    let source_table = catalog
        .get(id_catalog_key(5, id(82)).as_slice())?
        .ok_or("source table disappeared during FK/policy race")?
        .value()
        .to_vec();
    let source_epoch = u64::from_le_bytes(
        decode_registered_envelope(&source_table)?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("source table write epoch is missing")?
            .payload
            .as_slice()
            .try_into()?,
    );
    assert_eq!(source_epoch, 1 + committed_count);
    let target_table = catalog
        .get(id_catalog_key(5, id(81)).as_slice())?
        .ok_or("referenced table disappeared during FK/policy race")?
        .value()
        .to_vec();
    let target_epoch = u64::from_le_bytes(
        decode_registered_envelope(&target_table)?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("referenced table write epoch is missing")?
            .payload
            .as_slice()
            .try_into()?,
    );
    assert_eq!(
        target_epoch,
        1 + u64::from(foreign_key_status == OutcomeStatus::Committed)
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_creates_named_unique_index_and_backfills_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xd0);
    let foreign_key_edge = dependency_write(id(86), id(85), 3).key;
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != foreign_key_edge
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "named_index_admin"),
        object_write_with_name(actor, 8, "named_index_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xd1, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xd1), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut seed = storage.begin_invocation(id(0xd2), actor, 9_999, 2, 2048)?;
    let schema = seed.load_catalog_table_schema(id(82))?;
    seed.load_catalog_indexes(&schema)?;
    for (row_id, value) in [(id(0xd3), b"one".as_slice()), (id(0xd4), b"two".as_slice())] {
        let tuple = encode_tuple(
            &schema.columns,
            schema.schema_version,
            &[Some(ScalarValue::Bytea(value.to_vec()))],
        )?;
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: id(82),
                row: row_id,
            },
            &tuple,
        )?;
    }
    assert_eq!(seed.commit()?, 2);

    let stale_invocation = id(0xd5);
    let stale_row = id(0xd6);
    let mut stale_writer = storage.begin_invocation(stale_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(82))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"three".to_vec()))],
    )?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: stale_row,
        },
        &stale_tuple,
    )?;

    let duplicate_name_invocation = id(0xc0);
    let mut duplicate_name_audit = identity_audit_event(0xc1, actor, id(82));
    duplicate_name_audit.correlation_id = duplicate_name_invocation;
    let duplicate_name = storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
        invocation_id: duplicate_name_invocation,
        authenticated_actor_id: actor,
        table_id: id(82),
        name: "object_82".to_owned(),
        column_ids: vec![id(84)],
        unique: false,
        retention_deadline_us: 9_999,
        audit_event: duplicate_name_audit,
    });
    assert!(matches!(
        duplicate_name,
        Err(prdb_engine::storage_manager::IndexLifecycleError::Commit(
            CommitError::InvalidCatalog
        ))
    ));
    assert_eq!(commit_sequence(&database)?, 2);
    let pre_index_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_index_id_by_name(&pre_index_snapshot, id(80), "object_82")?,
        None
    );

    let invocation_id = id(0xd7);
    let mut audit = identity_audit_event(0xd8, actor, id(82));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_index(prdb_engine::storage_manager::CreateIndexRequest {
            invocation_id,
            authenticated_actor_id: actor,
            table_id: id(82),
            name: "source_value_unique".to_owned(),
            column_ids: vec![id(84)],
            unique: true,
            retention_deadline_us: 9_999,
            audit_event: audit,
        })
        .map_err(|error| format!("named unique index creation failed: {error:?}"))?;
    assert_ne!(created.index_id, [0; 16]);
    assert_eq!(created.commit_sequence, 3);
    let index_name_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_index_id_by_name(&index_name_snapshot, id(80), "source_value_unique")?,
        Some(created.index_id)
    );
    assert_eq!(
        prdb_engine::catalog_index::scan_index_ids_for_table(&index_name_snapshot, id(82), 1)?,
        vec![created.index_id]
    );
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), stale_row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_invocation, actor, 1)?
            .ok_or("missing stale named-index writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing named-index creation outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let index_object = decode_registered_envelope(
        catalog
            .get(id_catalog_key(2, created.index_id).as_slice())?
            .ok_or("missing index object")?
            .value(),
    )?;
    let owner_id: [u8; 16] = index_object
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("index object has no owner")?
        .payload
        .as_slice()
        .try_into()?;
    assert!(
        prdb_engine::catalog_index::scan_object_ids_for_owner(&read, owner_id, 4096)?
            .contains(&created.index_id)
    );
    assert!(
        index_object
            .fields
            .iter()
            .any(|field| { field.tag == 3 && field.payload.as_slice() == b"source_value_unique" })
    );
    assert!(
        index_object
            .fields
            .iter()
            .any(|field| field.tag == 2 && field.payload.as_slice() == id(80))
    );
    assert!(
        catalog
            .get(dependency_write(created.index_id, id(80), 1).key.as_slice())?
            .is_some()
    );
    let index_record = catalog
        .get(id_catalog_key(9, created.index_id).as_slice())?
        .ok_or("missing index definition")?;
    assert!(
        decode_registered_envelope(index_record.value())?
            .fields
            .iter()
            .any(|field| field.tag == 4 && field.payload.as_slice() == [1])
    );
    assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 2);
    assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 2);
    let row_table = read.open_table(ROWS)?;
    let table_prefix = id(82);
    let mut reference_scan = Vec::new();
    for entry in row_table.range::<&[u8]>(table_prefix.as_slice()..)? {
        let (row_key, row_value) = entry?;
        let row_key = row_key.value();
        if !row_key.starts_with(&table_prefix) {
            break;
        }
        if row_key.len() != 32 {
            return Err("malformed row key in reference scan".into());
        }
        let row_id: [u8; 16] = row_key[16..].try_into()?;
        let row_record = RowRecord::decode(row_value.value())?;
        let tuple = decode_tuple(&schema.columns, schema.schema_version, &row_record.payload)?;
        let Some(Some(ScalarValue::Bytea(value))) = tuple.values.first().cloned() else {
            return Err("reference row lacks its indexed BYTEA value".into());
        };
        reference_scan.push((value, row_id));
    }
    reference_scan.sort();

    let pinned = prdb_engine::snapshot::RowSnapshot::new(database.begin_read()?);
    let first_index_page = pinned.scan_index_prefix(&created.index_id, None, 1)?;
    assert_eq!(first_index_page.candidates.len(), 1);
    let second_index_page = pinned.scan_index_prefix(
        &created.index_id,
        first_index_page.continuation.as_deref(),
        1,
    )?;
    assert_eq!(second_index_page.candidates.len(), 1);
    assert!(second_index_page.continuation.is_none());
    let candidates = first_index_page
        .candidates
        .into_iter()
        .chain(second_index_page.candidates)
        .collect::<Vec<_>>();
    let mut persisted_index_scan = Vec::new();
    for candidate in candidates {
        let row_key = table_row_key(id(82), candidate.locator.row_id);
        let row_record = RowRecord::decode(
            row_table
                .get(row_key.as_slice())?
                .ok_or("index points to a missing row")?
                .value(),
        )?;
        assert_eq!(candidate.locator.row_version, row_record.row_version);
        let derived = prdb_engine::index_key::derive_index_key(
            created.index_id,
            candidate.locator.row_id,
            schema.schema_version,
            &schema.column_ids,
            &schema.columns,
            &row_record.payload,
            &[id(84)],
        )?;
        assert_eq!(derived.key, candidate.key);
        let tuple = decode_tuple(&schema.columns, schema.schema_version, &row_record.payload)?;
        let Some(Some(ScalarValue::Bytea(value))) = tuple.values.first().cloned() else {
            return Err("indexed row lacks its indexed BYTEA value".into());
        };
        persisted_index_scan.push((value, candidate.locator.row_id));
    }
    assert_eq!(persisted_index_scan, reference_scan);
    drop(index_record);
    drop(catalog);
    drop(read);

    let mut old_value_prefix = created.index_id.to_vec();
    old_value_prefix.extend(prdb_engine::index_key::encode_sql_tuple(
        &schema.columns,
        &[Some(ScalarValue::Bytea(b"one".to_vec()))],
    )?);
    let old_snapshot = prdb_engine::snapshot::RowSnapshot::new(database.begin_read()?);
    let old_membership = old_snapshot.scan_index_prefix(&old_value_prefix, None, 8)?;
    assert_eq!(old_membership.candidates.len(), 1);
    assert_eq!(old_membership.candidates[0].locator.row_id, id(0xd3));

    let mut update = storage.begin_invocation(id(0xe7), actor, 9_999, 1, 1024)?;
    let update_schema = update.load_catalog_table_schema(id(82))?;
    update.load_catalog_indexes(&update_schema)?;
    let updated_tuple = encode_tuple(
        &update_schema.columns,
        update_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"changed".to_vec()))],
    )?;
    update.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xd3),
        },
        &updated_tuple,
    )?;
    assert_eq!(update.commit()?, 4);

    let current_snapshot = prdb_engine::snapshot::RowSnapshot::new(database.begin_read()?);
    assert_eq!(
        old_snapshot
            .scan_index_prefix(&old_value_prefix, None, 8)?
            .candidates[0]
            .locator,
        old_membership.candidates[0].locator
    );
    let current_old_membership = current_snapshot.scan_index_prefix(&old_value_prefix, None, 8)?;
    assert!(current_old_membership.candidates.is_empty());
    let mut new_value_prefix = created.index_id.to_vec();
    new_value_prefix.extend(prdb_engine::index_key::encode_sql_tuple(
        &schema.columns,
        &[Some(ScalarValue::Bytea(b"changed".to_vec()))],
    )?);
    let current_new_membership = current_snapshot.scan_index_prefix(&new_value_prefix, None, 8)?;
    assert_eq!(current_new_membership.candidates.len(), 1);
    assert_eq!(
        current_new_membership.candidates[0].locator.row_id,
        id(0xd3)
    );
    assert_eq!(current_new_membership.candidates[0].locator.row_version, 4);

    let drop_invocation = id(0xd9);
    let mut drop_audit = identity_audit_event(0xda, actor, created.index_id);
    drop_audit.correlation_id = drop_invocation;
    let dropped = storage
        .drop_index(prdb_engine::storage_manager::DropIndexRequest {
            invocation_id: drop_invocation,
            authenticated_actor_id: actor,
            index_id: created.index_id,
            retention_deadline_us: 9_999,
            audit_event: drop_audit,
        })
        .map_err(|error| format!("named unique index drop failed: {error:?}"))?;
    assert_eq!(dropped.index_id, created.index_id);
    assert_eq!(dropped.commit_sequence, 5);
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing named-index drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(2, created.index_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(17, created.index_id).as_slice())?
            .is_some(),
        "DROP INDEX must permanently reserve its retired object ID"
    );
    assert_eq!(
        lookup_index_id_by_name(&index_name_snapshot, id(80), "source_value_unique")?,
        Some(created.index_id)
    );
    let retained_index_catalog = index_name_snapshot.open_table(CATALOG)?;
    assert!(
        retained_index_catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_some()
    );
    assert!(
        retained_index_catalog
            .get(id_catalog_key(2, created.index_id).as_slice())?
            .is_some()
    );
    let retired_index_definition = retained_index_catalog
        .get(id_catalog_key(9, created.index_id).as_slice())?
        .ok_or("retained index definition is missing")?
        .value()
        .to_vec();
    let retired_index_object = retained_index_catalog
        .get(id_catalog_key(2, created.index_id).as_slice())?
        .ok_or("retained index object is missing")?
        .value()
        .to_vec();
    drop(retained_index_catalog);
    let reuse_invocation = id(0xfa);
    let mut reuse_audit = identity_audit_event(0xfb, actor, created.index_id);
    reuse_audit.correlation_id = reuse_invocation;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            reuse_invocation,
            actor,
            9_999,
            &[
                CatalogWrite {
                    key: id_catalog_key(9, created.index_id),
                    expected_value: None,
                    payload: Some(retired_index_definition),
                },
                CatalogWrite {
                    key: id_catalog_key(2, created.index_id),
                    expected_value: None,
                    payload: Some(retired_index_object),
                },
            ],
            &[reuse_audit],
        ),
        Err(CommitError::InvalidCatalog),
        "a dropped index ID must not be reusable by a later catalog commit"
    );
    assert_eq!(
        index_name_snapshot
            .open_table(schema::INDEXES)?
            .iter()?
            .count(),
        2
    );
    assert_eq!(
        index_name_snapshot
            .open_table(schema::ROW_INDEXES)?
            .iter()?
            .count(),
        2
    );
    assert!(
        catalog
            .get(dependency_write(created.index_id, id(80), 1).key.as_slice())?
            .is_none()
    );
    assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 0);
    assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 0);
    assert_eq!(
        lookup_index_id_by_name(&read, id(80), "source_value_unique")?,
        None
    );
    drop(catalog);
    drop(read);

    let stale_policy_writer_invocation = id(0xdb);
    let stale_policy_row = id(0xdc);
    let mut stale_policy_writer =
        storage.begin_invocation(stale_policy_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_policy_schema = stale_policy_writer.load_catalog_table_schema(id(82))?;
    stale_policy_writer.load_catalog_indexes(&stale_policy_schema)?;
    let stale_policy_tuple = encode_tuple(
        &stale_policy_schema.columns,
        stale_policy_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"staged before policy creation".to_vec(),
        ))],
    )?;
    stale_policy_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: stale_policy_row,
        },
        &stale_policy_tuple,
    )?;
    let policy_invocation = id(0xdd);
    let mut policy_audit = identity_audit_event(0xde, actor, id(82));
    policy_audit.correlation_id = policy_invocation;
    let policy = storage
        .create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
            invocation_id: policy_invocation,
            authenticated_actor_id: actor,
            table_id: id(82),
            name: "fresh_row_policy".to_owned(),
            kind: prdb_engine::storage_manager::PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients: vec![prdb_engine::authorization::Subject::Principal(actor)],
            target_column_ids: Vec::new(),
            using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                ScalarValue::Boolean(true),
            ))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: policy_audit,
        })
        .map_err(|error| format!("row policy creation failed: {error:?}"))?;
    assert_eq!(policy.commit_sequence, 6);
    assert!(matches!(
        stale_policy_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), stale_policy_row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_policy_writer_invocation, actor, 1)?
            .ok_or("missing stale policy writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, policy_invocation, actor, 1)?
            .ok_or("missing policy creation outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let racing_writer_invocation = id(0xdf);
    let racing_row = id(0xe0);
    let mut racing_writer =
        storage.begin_invocation(racing_writer_invocation, actor, 9_999, 1, 1024)?;
    let racing_schema = racing_writer.load_catalog_table_schema(id(82))?;
    racing_writer.load_catalog_indexes(&racing_schema)?;
    let racing_tuple = encode_tuple(
        &racing_schema.columns,
        racing_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"raced policy creation".to_vec()))],
    )?;
    racing_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: racing_row,
        },
        &racing_tuple,
    )?;
    let racing_policy_invocation = id(0xe1);
    let before_policy_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, policy_result) = std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            racing_writer.commit()
        });
        let policy_barrier = barrier.clone();
        let policy_storage = &storage;
        let policy = scope.spawn(move || {
            let mut audit = identity_audit_event(0xe2, actor, id(82));
            audit.correlation_id = racing_policy_invocation;
            policy_barrier.wait();
            policy_storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
                invocation_id: racing_policy_invocation,
                authenticated_actor_id: actor,
                table_id: id(82),
                name: "racing_row_policy".to_owned(),
                kind: prdb_engine::storage_manager::PolicyKind::Row,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Allow,
                recipients: vec![prdb_engine::authorization::Subject::Principal(actor)],
                target_column_ids: Vec::new(),
                using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(true),
                ))),
                with_check_expression: None,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        barrier.wait();
        (
            writer.join().expect("racing writer panicked"),
            policy.join().expect("racing policy DDL panicked"),
        )
    });
    let created_racing_policy =
        policy_result.map_err(|error| format!("racing row policy creation failed: {error:?}"))?;
    let writer_sequence = match writer_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    let writer_status = if writer_sequence.is_some() {
        OutcomeStatus::Committed
    } else {
        OutcomeStatus::Aborted
    };
    assert_eq!(
        row(&database, &table_row_key(id(82), racing_row))?.is_some(),
        writer_sequence.is_some()
    );
    assert_eq!(
        lookup_outcome(&database, racing_writer_invocation, actor, 1)?
            .ok_or("missing racing writer outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, racing_policy_invocation, actor, 1)?
            .ok_or("missing racing policy outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        created_racing_policy.commit_sequence,
        before_policy_race + 1 + u64::from(writer_sequence.is_some())
    );
    assert_eq!(
        commit_sequence(&database)?,
        created_racing_policy.commit_sequence
    );
    let stale_policy_drop_invocation = id(0xf7);
    let stale_policy_drop_row = id(0xf9);
    let mut stale_policy_drop_writer =
        storage.begin_invocation(stale_policy_drop_invocation, actor, 9_999, 1, 1024)?;
    let stale_policy_drop_schema = stale_policy_drop_writer.load_catalog_table_schema(id(82))?;
    stale_policy_drop_writer.load_catalog_indexes(&stale_policy_drop_schema)?;
    let stale_policy_drop_tuple = encode_tuple(
        &stale_policy_drop_schema.columns,
        stale_policy_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"staged before policy drop".to_vec(),
        ))],
    )?;
    stale_policy_drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: stale_policy_drop_row,
        },
        &stale_policy_drop_tuple,
    )?;
    let policy_drop_invocation = id(0xf8);
    let dropped_policy_id = created_racing_policy.policy_id;
    let old_policy_snapshot = database.begin_read()?;
    let policy_drop_barrier = Arc::new(Barrier::new(3));
    let storage_ref = &storage;
    let (writer_result, policy_drop_result) = std::thread::scope(|scope| {
        let writer_barrier = Arc::clone(&policy_drop_barrier);
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            stale_policy_drop_writer.commit()
        });
        let drop_barrier = Arc::clone(&policy_drop_barrier);
        let dropper = scope.spawn(move || {
            let mut audit = identity_audit_event(0xfa, actor, dropped_policy_id);
            audit.correlation_id = policy_drop_invocation;
            drop_barrier.wait();
            storage_ref.drop_policy(prdb_engine::storage_manager::DropPolicyRequest {
                invocation_id: policy_drop_invocation,
                authenticated_actor_id: actor,
                policy_id: dropped_policy_id,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        policy_drop_barrier.wait();
        (
            writer.join().expect("staged policy-drop writer panicked"),
            dropper.join().expect("policy drop thread panicked"),
        )
    });
    let policy_drop_sequence =
        policy_drop_result.map_err(|error| format!("row policy drop failed: {error:?}"))?;
    let policy_drop_writer_sequence = match writer_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    assert_eq!(
        policy_drop_sequence,
        created_racing_policy.commit_sequence
            + 1
            + u64::from(policy_drop_writer_sequence.is_some())
    );
    assert_eq!(
        row(&database, &table_row_key(id(82), stale_policy_drop_row))?.is_some(),
        policy_drop_writer_sequence.is_some()
    );
    assert_eq!(
        lookup_outcome(&database, stale_policy_drop_invocation, actor, 1)?
            .ok_or("missing policy-drop writer outcome")?
            .status,
        if policy_drop_writer_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        lookup_outcome(&database, policy_drop_invocation, actor, 1)?
            .ok_or("missing policy-drop DDL outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(commit_sequence(&database)?, policy_drop_sequence);
    let old_policy_catalog = old_policy_snapshot.open_table(CATALOG)?;
    let dropped_policy_expression_id = created_racing_policy
        .using_expression_id
        .ok_or("created row policy has no USING expression")?;
    assert!(
        old_policy_catalog
            .get(id_catalog_key(13, dropped_policy_id).as_slice())?
            .is_some()
    );
    assert!(
        old_policy_catalog
            .get(id_catalog_key(7, dropped_policy_expression_id).as_slice())?
            .is_some()
    );
    assert!(
        old_policy_catalog
            .get(
                dependency_write(dropped_policy_id, dropped_policy_expression_id, 2)
                    .key
                    .as_slice()
            )?
            .is_some()
    );
    drop(old_policy_catalog);
    let current_policy_snapshot = database.begin_read()?;
    let current_policy_catalog = current_policy_snapshot.open_table(CATALOG)?;
    assert!(
        current_policy_catalog
            .get(id_catalog_key(13, dropped_policy_id).as_slice())?
            .is_none()
    );
    assert!(
        current_policy_catalog
            .get(id_catalog_key(7, dropped_policy_expression_id).as_slice())?
            .is_none()
    );
    assert!(
        current_policy_catalog
            .get(
                dependency_write(dropped_policy_id, dropped_policy_expression_id, 2)
                    .key
                    .as_slice()
            )?
            .is_none()
    );
    drop(current_policy_catalog);
    drop(current_policy_snapshot);

    let policy_recreate_invocation = id(0xe5);
    let mut policy_recreate_audit = identity_audit_event(0xe6, actor, id(82));
    policy_recreate_audit.correlation_id = policy_recreate_invocation;
    let recreated_policy = storage
        .create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
            invocation_id: policy_recreate_invocation,
            authenticated_actor_id: actor,
            table_id: id(82),
            name: "racing_row_policy".to_owned(),
            kind: prdb_engine::storage_manager::PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients: vec![prdb_engine::authorization::Subject::Principal(actor)],
            target_column_ids: Vec::new(),
            using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                ScalarValue::Boolean(true),
            ))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: policy_recreate_audit,
        })
        .map_err(|error| format!("same-name row policy recreation failed: {error:?}"))?;
    assert_ne!(recreated_policy.policy_id, dropped_policy_id);
    let recreated_policy_expression_id = recreated_policy
        .using_expression_id
        .ok_or("recreated row policy has no USING expression")?;
    let old_after_recreate = old_policy_snapshot.open_table(CATALOG)?;
    assert!(
        old_after_recreate
            .get(id_catalog_key(13, dropped_policy_id).as_slice())?
            .is_some()
    );
    assert!(
        old_after_recreate
            .get(id_catalog_key(7, dropped_policy_expression_id).as_slice())?
            .is_some()
    );
    drop(old_after_recreate);
    let recreated_policy_snapshot = database.begin_read()?;
    let recreated_policy_catalog = recreated_policy_snapshot.open_table(CATALOG)?;
    assert!(
        recreated_policy_catalog
            .get(id_catalog_key(13, dropped_policy_id).as_slice())?
            .is_none()
    );
    assert!(
        recreated_policy_catalog
            .get(id_catalog_key(7, dropped_policy_expression_id).as_slice())?
            .is_none()
    );
    assert!(
        recreated_policy_catalog
            .get(id_catalog_key(13, recreated_policy.policy_id).as_slice())?
            .is_some()
    );
    assert!(
        recreated_policy_catalog
            .get(id_catalog_key(7, recreated_policy_expression_id).as_slice())?
            .is_some()
    );
    assert!(
        recreated_policy_catalog
            .get(
                dependency_write(
                    recreated_policy.policy_id,
                    recreated_policy_expression_id,
                    2
                )
                .key
                .as_slice()
            )?
            .is_some()
    );
    let select_policies = prdb_engine::catalog_index::lookup_policies_for_table_operation(
        &recreated_policy_snapshot,
        id(82),
        1,
    )?;
    assert!(
        select_policies
            .iter()
            .any(|record| record.logical_key[3..] == recreated_policy.policy_id)
    );
    assert!(
        select_policies
            .iter()
            .all(|record| record.logical_key[3..] != dropped_policy_id)
    );
    drop(recreated_policy_catalog);
    drop(recreated_policy_snapshot);
    drop(old_policy_snapshot);
    let recreate_invocation = id(0xe3);
    let mut recreate_audit = identity_audit_event(0xe4, actor, id(82));
    recreate_audit.correlation_id = recreate_invocation;
    let recreated_index =
        storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
            invocation_id: recreate_invocation,
            authenticated_actor_id: actor,
            table_id: id(82),
            name: "source_value_unique".to_owned(),
            column_ids: vec![id(84)],
            unique: true,
            retention_deadline_us: 9_999,
            audit_event: recreate_audit,
        })?;
    assert_ne!(recreated_index.index_id, created.index_id);
    let recreated_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_index_id_by_name(&recreated_snapshot, id(80), "source_value_unique")?,
        Some(recreated_index.index_id)
    );
    assert_eq!(
        lookup_index_id_by_name(&index_name_snapshot, id(80), "source_value_unique")?,
        Some(created.index_id)
    );
    let recreated_catalog = recreated_snapshot.open_table(CATALOG)?;
    assert!(
        recreated_catalog
            .get(id_catalog_key(2, created.index_id).as_slice())?
            .is_none()
    );
    assert!(
        recreated_catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_none()
    );
    assert!(
        recreated_catalog
            .get(id_catalog_key(2, recreated_index.index_id).as_slice())?
            .is_some()
    );
    assert!(
        recreated_catalog
            .get(id_catalog_key(9, recreated_index.index_id).as_slice())?
            .is_some()
    );
    assert!(
        recreated_catalog
            .get(
                dependency_write(recreated_index.index_id, id(80), 1)
                    .key
                    .as_slice()
            )?
            .is_some()
    );
    assert_eq!(
        prdb_engine::catalog_index::scan_index_ids_for_table(&recreated_snapshot, id(82), 8)?,
        vec![recreated_index.index_id]
    );
    drop(recreated_catalog);
    drop(index_name_snapshot);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}
