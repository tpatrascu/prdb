use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::commit::{
    CatalogWrite, CommitError, IndexWrite, RowWrite, commit_catalog_and_audit,
    commit_catalog_and_audit_with_fences, commit_rows, commit_rows_and_indexes,
};
use prdb_engine::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::schema::{self, CATALOG, ROWS};
use prdb_engine::storage_manager::{StorageManager, StorageManagerConfig};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{ReadableDatabase, ReadableTable};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_creates_unique_constraint_and_backfills_existing_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe9);
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
        principal_write_with_name(actor, 1, 1, "unique_create_admin"),
        object_write_with_name(actor, 8, "unique_create_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xea, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xea), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut insert = storage.begin_invocation(id(0xeb), actor, 9_999, 2, 2048)?;
    let schema = insert.load_catalog_table_schema(id(82))?;
    insert.load_catalog_indexes(&schema)?;
    for (row_id, value) in [
        (id(0xec), b"first".as_slice()),
        (id(0xed), b"second".as_slice()),
    ] {
        let tuple = encode_tuple(
            &schema.columns,
            schema.schema_version,
            &[Some(ScalarValue::Bytea(value.to_vec()))],
        )?;
        insert.stage_put(
            prdb_engine::overlay::RowId {
                table: id(82),
                row: row_id,
            },
            &tuple,
        )?;
    }
    assert_eq!(insert.commit()?, 2);

    let mut stale_writer = storage.begin_invocation(id(0xee), actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(82))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"third".to_vec()))],
    )?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xef),
        },
        &stale_tuple,
    )?;

    let invocation_id = id(0xf0);
    let mut audit = identity_audit_event(0xf1, actor, id(82));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_named_unique_constraint(
            prdb_engine::storage_manager::CreateUniqueConstraintRequest {
                invocation_id,
                authenticated_actor_id: actor,
                table_id: id(82),
                column_ids: vec![id(84)],
                retention_deadline_us: 9_999,
                audit_event: audit,
            },
            "unique_object_84_key".to_owned(),
        )
        .map_err(|error| format!("UNIQUE constraint creation failed: {error:?}"))?;
    assert_eq!(created.commit_sequence, 3);
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, id(0xee), actor, 1)?
            .ok_or("missing stale UNIQUE writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(8, created.constraint_id).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_some()
    );
    let constraint_object = catalog
        .get(id_catalog_key(2, created.constraint_id).as_slice())?
        .ok_or("missing UNIQUE constraint object")?;
    assert_eq!(
        decode_registered_envelope(constraint_object.value())?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("missing UNIQUE constraint object name")?
            .payload,
        b"unique_object_84_key"
    );
    drop(constraint_object);
    assert!(
        catalog
            .get(id_catalog_key(2, created.index_id).as_slice())?
            .is_some()
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    let mut stale_drop_writer = storage.begin_invocation(id(0xe6), actor, 9_999, 1, 1024)?;
    let stale_drop_schema = stale_drop_writer.load_catalog_table_schema(id(82))?;
    stale_drop_writer.load_catalog_indexes(&stale_drop_schema)?;
    let stale_drop_tuple = encode_tuple(
        &stale_drop_schema.columns,
        stale_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"stale after UNIQUE drop".to_vec(),
        ))],
    )?;
    stale_drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xe7),
        },
        &stale_drop_tuple,
    )?;
    let drop_invocation_id = id(0xe2);
    let old_read = database.begin_read()?;
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, drop_result) = std::thread::scope(|scope| {
        let writer_barrier = Arc::clone(&barrier);
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            stale_drop_writer.commit()
        });
        let drop_barrier = Arc::clone(&barrier);
        let dropper = scope.spawn(move || {
            let mut drop_audit = identity_audit_event(0xe3, actor, created.constraint_id);
            drop_audit.correlation_id = drop_invocation_id;
            drop_barrier.wait();
            storage.drop_unique_constraint(
                drop_invocation_id,
                actor,
                created.constraint_id,
                9_999,
                drop_audit,
            )
        });
        barrier.wait();
        (
            writer.join().expect("racing UNIQUE writer panicked"),
            dropper.join().expect("racing UNIQUE drop panicked"),
        )
    });
    let drop_sequence = drop_result.map_err(|error| format!("UNIQUE drop failed: {error:?}"))?;
    let writer_sequence = match writer_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    assert_eq!(drop_sequence, 4 + u64::from(writer_sequence.is_some()));
    assert_eq!(
        prdb_engine::catalog_index::scan_constraint_ids_for_table(&old_read, id(82), 1)?,
        vec![created.constraint_id]
    );
    let old_catalog = old_read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, created.constraint_id),
        id_catalog_key(9, created.index_id),
        id_catalog_key(2, created.constraint_id),
        id_catalog_key(2, created.index_id),
        dependency_write(created.constraint_id, created.index_id, 4).key,
    ] {
        assert!(old_catalog.get(key.as_slice())?.is_some());
    }
    drop(old_catalog);
    assert_eq!(
        row(&database, &table_row_key(id(82), id(0xe7)))?.is_some(),
        writer_sequence.is_some()
    );
    assert_eq!(
        lookup_outcome(&database, id(0xe6), actor, 1)?
            .ok_or("missing stale UNIQUE-drop writer outcome")?
            .status,
        if writer_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation_id, actor, 1)?
            .ok_or("missing UNIQUE-drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(commit_sequence(&database)?, drop_sequence);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, id(82), 1)?.is_empty()
    );
    assert!(
        catalog
            .get(id_catalog_key(8, created.constraint_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_none()
    );
    drop(catalog);
    drop(read);
    drop(old_read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_creates_primary_key_with_owned_unique_index()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xf2);
    let foreign_key_edge = dependency_write(id(86), id(85), 3).key;
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(6, id(84))
                && write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != foreign_key_edge
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        column_write_not_null(id(84), id(82), "object_84"),
        principal_write_with_name(actor, 1, 1, "primary_create_admin"),
        object_write_with_name(actor, 8, "primary_create_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xf3, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xf3), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut stale_writer = storage.begin_invocation(id(0xd8), actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(82))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"stale before PRIMARY KEY add".to_vec(),
        ))],
    )?;
    let stale_row = prdb_engine::overlay::RowId {
        table: id(82),
        row: id(0xd9),
    };
    stale_writer.stage_put(stale_row, &stale_tuple)?;

    let invocation_id = id(0xf4);
    let mut audit = identity_audit_event(0xf5, actor, id(82));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_named_primary_key(
            prdb_engine::storage_manager::CreatePrimaryKeyRequest {
                invocation_id,
                authenticated_actor_id: actor,
                table_id: id(82),
                column_ids: vec![id(84)],
                retention_deadline_us: 9_999,
                audit_event: audit,
            },
            "primary_object_84_pk".to_owned(),
        )
        .map_err(|error| format!("PRIMARY KEY creation failed: {error:?}"))?;
    assert_eq!(created.commit_sequence, 2);
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), stale_row.row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, id(0xd8), actor, 1)?
            .ok_or("missing stale PRIMARY KEY-add writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let old_read = database.begin_read()?;
    let catalog = old_read.open_table(CATALOG)?;
    let constraint = catalog
        .get(id_catalog_key(8, created.constraint_id).as_slice())?
        .ok_or("missing created PRIMARY KEY")?;
    let decoded = decode_registered_envelope(constraint.value())?;
    let kind = decoded
        .fields
        .iter()
        .find(|field| field.tag == 2)
        .ok_or("missing PRIMARY KEY kind")?
        .payload
        .as_slice()
        .try_into()?;
    assert_eq!(u16::from_le_bytes(kind), 2);
    let constraint_object = catalog
        .get(id_catalog_key(2, created.constraint_id).as_slice())?
        .ok_or("missing PRIMARY KEY object")?;
    assert_eq!(
        decode_registered_envelope(constraint_object.value())?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("missing PRIMARY KEY object name")?
            .payload,
        b"primary_object_84_pk"
    );
    drop(constraint_object);
    assert!(
        catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(
                dependency_write(created.constraint_id, created.index_id, 4)
                    .key
                    .as_slice()
            )?
            .is_some()
    );
    drop(constraint);
    drop(catalog);
    prdb_engine::startup::validate_persisted_records(&database)?;
    let mut stale_drop_writer = storage.begin_invocation(id(0xda), actor, 9_999, 1, 1024)?;
    let stale_drop_schema = stale_drop_writer.load_catalog_table_schema(id(82))?;
    stale_drop_writer.load_catalog_indexes(&stale_drop_schema)?;
    let stale_drop_tuple = encode_tuple(
        &stale_drop_schema.columns,
        stale_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"stale after PRIMARY KEY drop".to_vec(),
        ))],
    )?;
    stale_drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xdb),
        },
        &stale_drop_tuple,
    )?;
    let drop_invocation_id = id(0xe4);
    let drop_barrier = Arc::new(Barrier::new(3));
    let storage_ref = &storage;
    let (writer_result, drop_result) = std::thread::scope(|scope| {
        let writer_barrier = Arc::clone(&drop_barrier);
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            stale_drop_writer.commit()
        });
        let drop_barrier_for_thread = Arc::clone(&drop_barrier);
        let dropper = scope.spawn(move || {
            let mut drop_audit = identity_audit_event(0xe5, actor, created.constraint_id);
            drop_audit.correlation_id = drop_invocation_id;
            drop_barrier_for_thread.wait();
            storage_ref.drop_primary_key(
                drop_invocation_id,
                actor,
                created.constraint_id,
                9_999,
                drop_audit,
            )
        });
        drop_barrier.wait();
        (
            writer.join().expect("racing PRIMARY KEY writer panicked"),
            dropper.join().expect("racing PRIMARY KEY drop panicked"),
        )
    });
    let drop_sequence =
        drop_result.map_err(|error| format!("PRIMARY KEY drop failed: {error:?}"))?;
    let writer_sequence = match writer_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    assert_eq!(drop_sequence, 3 + u64::from(writer_sequence.is_some()));
    let old_catalog = old_read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, created.constraint_id),
        id_catalog_key(9, created.index_id),
        id_catalog_key(2, created.constraint_id),
        id_catalog_key(2, created.index_id),
        dependency_write(created.constraint_id, created.index_id, 4).key,
    ] {
        assert!(
            old_catalog.get(key.as_slice())?.is_some(),
            "pre-drop snapshot lost PRIMARY KEY record {key:?}"
        );
    }
    drop(old_catalog);
    assert_eq!(
        row(&database, &table_row_key(id(82), id(0xdb)))?.is_some(),
        writer_sequence.is_some()
    );
    assert_eq!(
        lookup_outcome(&database, id(0xda), actor, 1)?
            .ok_or("missing stale PRIMARY KEY-drop writer outcome")?
            .status,
        if writer_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation_id, actor, 1)?
            .ok_or("missing PRIMARY KEY-drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(commit_sequence(&database)?, drop_sequence);
    drop(old_read);
    let current_read = database.begin_read()?;
    let current_catalog = current_read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, created.constraint_id),
        id_catalog_key(9, created.index_id),
        id_catalog_key(2, created.constraint_id),
        id_catalog_key(2, created.index_id),
        dependency_write(created.constraint_id, created.index_id, 4).key,
    ] {
        assert!(
            current_catalog.get(key.as_slice())?.is_none(),
            "new snapshot retained dropped PRIMARY KEY record {key:?}"
        );
    }
    let retired_index = current_catalog
        .get(id_catalog_key(17, created.index_id).as_slice())?
        .ok_or("missing retired PRIMARY KEY index identifier")?;
    assert!(
        decode_registered_envelope(retired_index.value())?
            .fields
            .is_empty()
    );
    drop(retired_index);
    let retired_constraint = current_catalog
        .get(id_catalog_key(17, created.constraint_id).as_slice())?
        .ok_or("missing retired PRIMARY KEY constraint identifier")?;
    assert!(
        decode_registered_envelope(retired_constraint.value())?
            .fields
            .is_empty()
    );
    drop(retired_constraint);
    drop(current_catalog);
    drop(current_read);

    let atomic_create_invocation = id(0xc1);
    let mut atomic_create_audit = identity_audit_event(0xc2, actor, id(0xc3));
    atomic_create_audit.correlation_id = atomic_create_invocation;
    let created_table = storage
        .create_table_with_constraints(
            prdb_engine::storage_manager::CreateTableRequest {
                invocation_id: atomic_create_invocation,
                authenticated_actor_id: actor,
                schema_id: id(80),
                name: "atomic_primary".to_owned(),
                columns: vec![
                    prdb_engine::storage_manager::TableColumnDefinition {
                        name: "tenant".to_owned(),
                        scalar_type: ScalarType::Uuid,
                        nullable: false,
                        generated_uuid_identity: false,
                        default: None,
                    },
                    prdb_engine::storage_manager::TableColumnDefinition {
                        name: "event_id".to_owned(),
                        scalar_type: ScalarType::Integer,
                        nullable: false,
                        generated_uuid_identity: false,
                        default: None,
                    },
                    prdb_engine::storage_manager::TableColumnDefinition {
                        name: "label".to_owned(),
                        scalar_type: ScalarType::Text,
                        nullable: true,
                        generated_uuid_identity: false,
                        default: None,
                    },
                ],
                retention_deadline_us: 9_999,
                audit_event: atomic_create_audit,
            },
            Some(&["tenant".to_owned(), "event_id".to_owned()]),
            &[vec!["label".to_owned()]],
        )
        .map_err(|error| {
            format!("atomic table with PRIMARY KEY and UNIQUE create failed: {error:?}")
        })?;
    assert_eq!(created_table.commit_sequence, drop_sequence + 1);
    assert_eq!(created_table.column_ids.len(), 3);
    let atomic_reader = database.begin_read()?;
    let atomic_constraint_ids = prdb_engine::catalog_index::scan_constraint_ids_for_table(
        &atomic_reader,
        created_table.table_id,
        2,
    )?;
    assert_eq!(atomic_constraint_ids.len(), 2);
    let atomic_catalog = atomic_reader.open_table(CATALOG)?;
    let mut constraint_kinds = Vec::new();
    for constraint_id in &atomic_constraint_ids {
        let atomic_constraint = atomic_catalog
            .get(id_catalog_key(8, *constraint_id).as_slice())?
            .ok_or("atomic key constraint record missing")?;
        let atomic_constraint = decode_registered_envelope(atomic_constraint.value())?;
        let kind = atomic_constraint
            .fields
            .iter()
            .find(|field| field.tag == 2)
            .ok_or("atomic key constraint kind missing")?;
        constraint_kinds.push(u16::from_le_bytes(kind.payload.as_slice().try_into()?));
    }
    constraint_kinds.sort_unstable();
    assert_eq!(constraint_kinds, [2, 3]);
    drop(atomic_catalog);
    drop(atomic_reader);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_refuses_to_drop_primary_key_referenced_by_foreign_key()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe8);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "referenced_key_drop_admin"),
        object_write_with_name(actor, 8, "referenced_key_drop_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xe9, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xe9), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let invocation_id = id(0xea);
    let mut audit = identity_audit_event(0xeb, actor, id(85));
    audit.correlation_id = invocation_id;
    assert_eq!(
        storage.drop_primary_key(invocation_id, actor, id(85), 9_999, audit),
        Err(TableLifecycleError::Commit(CommitError::InvalidCatalog))
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing rejected referenced-key drop outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, id(85)),
        id_catalog_key(9, id(87)),
        dependency_write(id(86), id(85), 3).key,
    ] {
        assert!(catalog.get(key.as_slice())?.is_some(), "missing {key:?}");
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_creates_and_drops_check_constraint()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xf6);
    let foreign_key_edge = dependency_write(id(86), id(85), 3).key;
    let setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != foreign_key_edge
        })
        .chain([
            principal_write_with_name(actor, 1, 1, "check_create_admin"),
            object_write_with_name(actor, 8, "check_create_admin"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ])
        .collect::<Vec<_>>();
    let mut setup_audit = identity_audit_event(0xf7, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xf7), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut insert = storage.begin_invocation(id(0xf8), actor, 9_999, 1, 1024)?;
    let schema = insert.load_catalog_table_schema(id(82))?;
    insert.load_catalog_indexes(&schema)?;
    let tuple = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(ScalarValue::Bytea(b"check row".to_vec()))],
    )?;
    insert.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xf9),
        },
        &tuple,
    )?;
    assert_eq!(insert.commit()?, 2);

    let invocation_id = id(0xfa);
    let mut invalid_audit = identity_audit_event(0xfb, actor, id(82));
    invalid_audit.correlation_id = invocation_id;
    assert!(matches!(
        storage.create_check_constraint(
            prdb_engine::storage_manager::CreateCheckConstraintRequest {
                invocation_id,
                authenticated_actor_id: actor,
                table_id: id(82),
                name: None,
                expression: prdb_engine::authorization::Expr::CurrentPrincipalId,
                dependency_column_ids: vec![],
                retention_deadline_us: 9_999,
                audit_event: invalid_audit,
            }
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::InvalidRequest)
    ));

    let invalid_row_check_id = id(0xfc);
    let mut invalid_row_check_audit = identity_audit_event(0xfd, actor, id(82));
    invalid_row_check_audit.correlation_id = invalid_row_check_id;
    assert!(matches!(
        storage.create_check_constraint(
            prdb_engine::storage_manager::CreateCheckConstraintRequest {
                invocation_id: invalid_row_check_id,
                authenticated_actor_id: actor,
                table_id: id(82),
                name: None,
                expression: prdb_engine::authorization::Expr::IsNull(Box::new(
                    prdb_engine::authorization::Expr::Field(0),
                )),
                dependency_column_ids: vec![id(84)],
                retention_deadline_us: 9_999,
                audit_event: invalid_row_check_audit,
            }
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::CheckViolation
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, invalid_row_check_id, actor, 1)?
            .ok_or("missing rejected CHECK creation outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut stale_add_writer = storage.begin_invocation(id(0xdc), actor, 9_999, 1, 1024)?;
    let stale_add_schema = stale_add_writer.load_catalog_table_schema(id(82))?;
    stale_add_writer.load_catalog_indexes(&stale_add_schema)?;
    let stale_add_tuple = encode_tuple(
        &stale_add_schema.columns,
        stale_add_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"stale before CHECK add".to_vec()))],
    )?;
    let stale_add_row = prdb_engine::overlay::RowId {
        table: id(82),
        row: id(0xdd),
    };
    stale_add_writer.stage_put(stale_add_row, &stale_add_tuple)?;

    let invocation_id = id(0xfe);
    let mut create_audit = identity_audit_event(0xff, actor, id(82));
    create_audit.correlation_id = invocation_id;
    let check_race_barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let (create_result, staged_write_result) = std::thread::scope(|scope| {
        let barrier = std::sync::Arc::clone(&check_race_barrier);
        let storage = &storage;
        let create = scope.spawn(move || {
            barrier.wait();
            storage.create_check_constraint(
                prdb_engine::storage_manager::CreateCheckConstraintRequest {
                    invocation_id,
                    authenticated_actor_id: actor,
                    table_id: id(82),
                    name: Some("always_true".to_owned()),
                    expression: prdb_engine::authorization::Expr::Literal(Some(
                        ScalarValue::Boolean(true),
                    )),
                    dependency_column_ids: vec![],
                    retention_deadline_us: 9_999,
                    audit_event: create_audit,
                },
            )
        });
        check_race_barrier.wait();
        let staged_write_result = stale_add_writer.commit();
        let create_result = create.join().expect("CHECK creation thread panicked");
        (create_result, staged_write_result)
    });
    let created =
        create_result.map_err(|error| format!("CHECK constraint creation failed: {error:?}"))?;
    let staged_write_status = match staged_write_result {
        Ok(sequence) => {
            assert!(sequence < created.commit_sequence);
            OutcomeStatus::Committed
        }
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        Err(error) => panic!("unexpected staged-write result in CHECK creation race: {error:?}"),
    };
    assert_eq!(
        created.commit_sequence,
        if staged_write_status == OutcomeStatus::Committed {
            4
        } else {
            3
        }
    );
    assert_eq!(
        row(&database, &table_row_key(id(82), stale_add_row.row))?.is_some(),
        staged_write_status == OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0xdc), actor, 1)?
            .ok_or("missing CHECK-creation race writer outcome")?
            .status,
        staged_write_status
    );
    let duplicate_id = id(0xf0);
    let mut duplicate_audit = identity_audit_event(0xf1, actor, id(82));
    duplicate_audit.correlation_id = duplicate_id;
    assert!(matches!(
        storage.create_check_constraint(
            prdb_engine::storage_manager::CreateCheckConstraintRequest {
                invocation_id: duplicate_id,
                authenticated_actor_id: actor,
                table_id: id(82),
                name: Some("always_true".to_owned()),
                expression: prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(
                    true
                ))),
                dependency_column_ids: vec![],
                retention_deadline_us: 9_999,
                audit_event: duplicate_audit,
            }
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::DuplicateConstraintName
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, duplicate_id, actor, 1)?
            .ok_or("missing duplicate named CHECK rejection outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let unauthorized_id = id(0xf2);
    let unauthorized_actor = id(0xf3);
    let mut unauthorized_audit = identity_audit_event(0xf4, unauthorized_actor, id(82));
    unauthorized_audit.correlation_id = unauthorized_id;
    assert!(matches!(
        storage.create_check_constraint(
            prdb_engine::storage_manager::CreateCheckConstraintRequest {
                invocation_id: unauthorized_id,
                authenticated_actor_id: unauthorized_actor,
                table_id: id(82),
                name: Some("always_true".to_owned()),
                expression: prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(
                    true
                ))),
                dependency_column_ids: vec![],
                retention_deadline_us: 9_999,
                audit_event: unauthorized_audit,
            }
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(&database, unauthorized_id, unauthorized_actor, 1)?
            .ok_or("missing unauthorized named CHECK outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let named_constraint_read = database.begin_read()?;
    let named_constraint_catalog = named_constraint_read.open_table(CATALOG)?;
    let named_constraint_object = named_constraint_catalog
        .get(id_catalog_key(2, created.constraint_id).as_slice())?
        .ok_or("named CHECK object missing")?;
    assert_eq!(
        decode_registered_envelope(named_constraint_object.value())?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("named CHECK object name missing")?
            .payload,
        b"always_true"
    );
    let constraint_snapshot = database.begin_read()?;
    assert_eq!(
        prdb_engine::catalog_index::scan_constraint_ids_for_table(&constraint_snapshot, id(82), 1)?,
        vec![created.constraint_id]
    );
    prdb_engine::startup::validate_persisted_records(&database)?;

    let mut stale_drop_writer = storage.begin_invocation(id(0xe3), actor, 9_999, 1, 1024)?;
    let stale_drop_schema = stale_drop_writer.load_catalog_table_schema(id(82))?;
    stale_drop_writer.load_catalog_indexes(&stale_drop_schema)?;
    stale_drop_writer.load_catalog_check_constraints(&stale_drop_schema)?;
    let stale_drop_tuple = encode_tuple(
        &stale_drop_schema.columns,
        stale_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"stale after CHECK drop".to_vec()))],
    )?;
    let stale_drop_row = prdb_engine::overlay::RowId {
        table: id(82),
        row: id(0xe4),
    };
    stale_drop_writer.stage_put(stale_drop_row, &stale_drop_tuple)?;

    let drop_invocation_id = id(0xe1);
    let mut drop_audit = identity_audit_event(0xe2, actor, created.constraint_id);
    drop_audit.correlation_id = drop_invocation_id;
    let drop_race_barrier = Arc::new(Barrier::new(2));
    let (drop_result, stale_drop_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&drop_race_barrier);
        let drop_storage = &storage;
        let drop = scope.spawn(move || {
            barrier.wait();
            drop_storage.drop_check_constraint(
                drop_invocation_id,
                actor,
                created.constraint_id,
                9_999,
                drop_audit,
            )
        });
        drop_race_barrier.wait();
        let staged_write = stale_drop_writer.commit();
        (
            drop.join().expect("CHECK drop thread panicked"),
            staged_write,
        )
    });
    let drop_sequence = drop_result?;
    let stale_drop_status = match stale_drop_result {
        Ok(sequence) => {
            assert!(sequence < drop_sequence);
            OutcomeStatus::Committed
        }
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        Err(error) => panic!("unexpected staged-write result in CHECK drop race: {error:?}"),
    };
    assert_eq!(
        drop_sequence,
        created.commit_sequence + 1 + u64::from(stale_drop_status == OutcomeStatus::Committed)
    );
    assert_eq!(
        prdb_engine::catalog_index::scan_constraint_ids_for_table(&constraint_snapshot, id(82), 1)?,
        vec![created.constraint_id]
    );
    let pinned_catalog = constraint_snapshot.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, created.constraint_id),
        id_catalog_key(7, created.expression_id),
        dependency_write(created.constraint_id, created.expression_id, 2).key,
    ] {
        assert!(pinned_catalog.get(key.as_slice())?.is_some());
    }
    drop(pinned_catalog);
    let dropped_constraint_snapshot = database.begin_read()?;
    assert!(
        prdb_engine::catalog_index::scan_constraint_ids_for_table(
            &dropped_constraint_snapshot,
            id(82),
            1
        )?
        .is_empty()
    );
    let current_catalog = dropped_constraint_snapshot.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, created.constraint_id),
        id_catalog_key(7, created.expression_id),
        dependency_write(created.constraint_id, created.expression_id, 2).key,
    ] {
        assert!(current_catalog.get(key.as_slice())?.is_none());
    }
    drop(current_catalog);
    assert_eq!(
        row(&database, &table_row_key(id(82), stale_drop_row.row))?.is_some(),
        stale_drop_status == OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0xe3), actor, 1)?
            .ok_or("missing stale CHECK-drop writer outcome")?
            .status,
        stale_drop_status
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(8, created.constraint_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(7, created.expression_id).as_slice())?
            .is_none()
    );
    drop(catalog);
    drop(read);

    let mut remove_seed = storage.begin_invocation(id(0xd0), actor, 9_999, 1, 1024)?;
    let remove_seed_schema = remove_seed.load_catalog_table_schema(id(82))?;
    remove_seed.load_catalog_indexes(&remove_seed_schema)?;
    remove_seed.stage_delete(prdb_engine::overlay::RowId {
        table: id(82),
        row: id(0xf9),
    })?;
    remove_seed.commit()?;

    let insert_invocation = id(0xd1);
    let insert_row_id = id(0xd2);
    let mut racing_insert = storage.begin_invocation(insert_invocation, actor, 9_999, 1, 1024)?;
    let racing_schema = racing_insert.load_catalog_table_schema(id(82))?;
    racing_insert.load_catalog_indexes(&racing_schema)?;
    let violating_tuple = encode_tuple(
        &racing_schema.columns,
        racing_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"not null".to_vec()))],
    )?;
    racing_insert.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: insert_row_id,
        },
        &violating_tuple,
    )?;

    let check_invocation = id(0xd3);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (insert_result, check_result) = std::thread::scope(|scope| {
        let insert_barrier = barrier.clone();
        let insert = scope.spawn(move || {
            insert_barrier.wait();
            racing_insert.commit()
        });
        let check_barrier = barrier.clone();
        let check_storage = &storage;
        let check = scope.spawn(move || {
            let mut audit = identity_audit_event(0xd4, actor, id(82));
            audit.correlation_id = check_invocation;
            check_barrier.wait();
            check_storage.create_check_constraint(
                prdb_engine::storage_manager::CreateCheckConstraintRequest {
                    invocation_id: check_invocation,
                    authenticated_actor_id: actor,
                    table_id: id(82),
                    name: None,
                    expression: prdb_engine::authorization::Expr::IsNull(Box::new(
                        prdb_engine::authorization::Expr::Field(0),
                    )),
                    dependency_column_ids: vec![id(84)],
                    retention_deadline_us: 9_999,
                    audit_event: audit,
                },
            )
        });
        barrier.wait();
        (
            insert.join().expect("CHECK race insert thread panicked"),
            check.join().expect("CHECK race DDL thread panicked"),
        )
    });
    match (insert_result, check_result) {
        (
            Ok(_),
            Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
                CommitError::CheckViolation,
            )),
        ) => {
            assert_eq!(
                lookup_outcome(&database, insert_invocation, actor, 1)?
                    .ok_or("missing winning CHECK-race insert outcome")?
                    .status,
                OutcomeStatus::Committed
            );
            assert_eq!(
                lookup_outcome(&database, check_invocation, actor, 1)?
                    .ok_or("missing aborted CHECK-race DDL outcome")?
                    .status,
                OutcomeStatus::Aborted
            );
            assert!(row(&database, &table_row_key(id(82), insert_row_id))?.is_some());
            assert_eq!(commit_sequence(&database)?, before_race + 1);
        }
        (
            Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)),
            Ok(created_check),
        ) => {
            assert_eq!(
                lookup_outcome(&database, insert_invocation, actor, 1)?
                    .ok_or("missing aborted CHECK-race insert outcome")?
                    .status,
                OutcomeStatus::Aborted
            );
            assert_eq!(
                lookup_outcome(&database, check_invocation, actor, 1)?
                    .ok_or("missing winning CHECK-race DDL outcome")?
                    .status,
                OutcomeStatus::Committed
            );
            assert!(row(&database, &table_row_key(id(82), insert_row_id))?.is_none());
            assert_eq!(commit_sequence(&database)?, before_race + 1);
            let drop_invocation_id = id(0xd5);
            let mut drop_audit = identity_audit_event(0xd6, actor, created_check.constraint_id);
            drop_audit.correlation_id = drop_invocation_id;
            storage.drop_check_constraint(
                drop_invocation_id,
                actor,
                created_check.constraint_id,
                9_999,
                drop_audit,
            )?;
        }
        (insert_result, check_result) => panic!(
            "unexpected CHECK creation race outcomes: insert={insert_result:?}, check={check_result:?}"
        ),
    }
    let runtime_now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let runtime_page = storage.list_runtime_invocations(actor, runtime_now_us, 1, None)?;
    assert!(
        runtime_page.invocation_conflicts_total
            >= u64::from(stale_drop_status == OutcomeStatus::Aborted)
    );
    assert!(
        runtime_page
            .invocation_redb_write_acquisition_buckets
            .iter()
            .sum::<u64>()
            >= 1
    );
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_drop_referencing_table_removes_owned_foreign_key()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut foreign_key_writes = foreign_key_catalog_writes(true, true);
    foreign_key_writes.extend([
        constraint_write(id(88), id(82), 5, &[id(84)], Some(id(85)), &[id(83)], None),
        object_write(id(88), 6),
        dependency_write(id(88), id(85), 3),
    ]);
    let mut catalog_audit = identity_audit_event(0xa1, admin_role, id(0x80));
    catalog_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xa1),
        admin_role,
        9_999,
        &foreign_key_writes,
        &[catalog_audit],
    )?;

    let actor = id(0xa2);
    let mut setup_audit = identity_audit_event(0xa3, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xa3),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_referencing_table_admin"),
            object_write_with_name(actor, 8, "drop_referencing_table_admin"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let target_table_key = id_catalog_key(5, id(81));
    let target_epoch_before = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let bytes = catalog
            .get(target_table_key.as_slice())?
            .ok_or("missing target table before drop")?
            .value()
            .to_vec();
        let record = decode_registered_envelope(&bytes)?;
        u64::from_le_bytes(
            record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("missing target write epoch")?
                .payload
                .as_slice()
                .try_into()?,
        )
    };
    let mut stale_target_writer = storage.begin_invocation(id(0xa6), admin_role, 9_999, 1, 1024)?;
    let stale_target_schema = stale_target_writer.load_catalog_table_schema(id(81))?;
    stale_target_writer.load_catalog_indexes(&stale_target_schema)?;
    let stale_target_tuple = encode_tuple(
        &stale_target_schema.columns,
        stale_target_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"staged before FK owner drop".to_vec(),
        ))],
    )?;
    let stale_target_row = id(0xa7);
    stale_target_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(81),
            row: stale_target_row,
        },
        &stale_target_tuple,
    )?;

    let invocation_id = id(0xa4);
    let mut drop_audit = identity_audit_event(0xa5, actor, id(82));
    drop_audit.correlation_id = invocation_id;
    let dropped = storage.drop_table(invocation_id, actor, id(82), 9_999, drop_audit);
    assert!(
        dropped.is_ok(),
        "dropping the FK owner table returned {dropped:?}"
    );
    assert_eq!(dropped.unwrap().table_id, id(82));
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing referencing-table drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(matches!(
        stale_target_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, id(0xa6), admin_role, 1)?
            .ok_or("missing stale target-writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(2, id(82)),
        id_catalog_key(5, id(82)),
        id_catalog_key(2, id(84)),
        id_catalog_key(6, id(84)),
        id_catalog_key(2, id(86)),
        id_catalog_key(8, id(86)),
        id_catalog_key(2, id(88)),
        id_catalog_key(8, id(88)),
        dependency_write(id(86), id(85), 3).key,
        dependency_write(id(88), id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "record remained: {key:?}"
        );
    }
    for key in [
        id_catalog_key(2, id(81)),
        id_catalog_key(5, id(81)),
        id_catalog_key(8, id(85)),
        id_catalog_key(9, id(87)),
        dependency_write(id(85), id(87), 4).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_some(),
            "target record missing: {key:?}"
        );
    }
    let target_bytes = catalog
        .get(target_table_key.as_slice())?
        .ok_or("target table missing after drop")?
        .value()
        .to_vec();
    let target_epoch_after = u64::from_le_bytes(
        decode_registered_envelope(&target_bytes)?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("missing target write epoch after drop")?
            .payload
            .as_slice()
            .try_into()?,
    );
    assert_eq!(target_epoch_after, target_epoch_before + 1);
    assert!(
        read.open_table(ROWS)?
            .get(table_row_key(id(81), stale_target_row).as_slice())?
            .is_none()
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_drop_table_removes_self_referential_foreign_key()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let schema_id = id(80);
    let table_id = id(82);
    let column_id = id(84);
    let primary_id = id(85);
    let foreign_key_id = id(86);
    let index_id = id(87);
    let catalog_writes = [
        schema_write(schema_id),
        table_write(table_id, schema_id),
        column_write_not_null(column_id, table_id, "object_84"),
        object_write(schema_id, 1),
        object_write(table_id, 3),
        object_write(column_id, 4),
    ];
    let mut catalog_audit = identity_audit_event(0x91, admin_role, table_id);
    catalog_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(
            id(0x91),
            admin_role,
            9_999,
            &catalog_writes,
            &[catalog_audit],
        )
        .map_err(|error| format!("self-FK catalog setup failed: {error:?}"))?;

    let actor = id(0x92);
    let mut setup_audit = identity_audit_event(0x93, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(
            id(0x93),
            admin_role,
            9_999,
            &[
                principal_write_with_name(actor, 1, 1, "self_fk_drop_admin"),
                object_write_with_name(actor, 8, "self_fk_drop_admin"),
                membership_write(actor, admin_role),
                dependency_write(actor, admin_role, 5),
            ],
            &[setup_audit],
        )
        .map_err(|error| format!("self-FK actor setup failed: {error:?}"))?;

    let primary_constraint = constraint_write(
        primary_id,
        table_id,
        2,
        &[column_id],
        None,
        &[],
        Some(index_id),
    );
    let (primary_index_key, primary_index_payload) =
        owned_unique_index_definition(index_id, table_id, &[column_id], primary_id);
    let table_key = id_catalog_key(5, table_id);
    let current_table = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(table_key.as_slice())?
        .ok_or("self-FK table missing before primary key add")?
        .value()
        .to_vec();
    let mut primary_table_epoch = table_write_with_versions(table_id, schema_id, 1, 2);
    primary_table_epoch.expected_value = Some(current_table);
    let primary_invocation = id(0x96);
    let mut primary_audit = identity_audit_event(0x97, actor, primary_id);
    primary_audit.correlation_id = primary_invocation;
    storage
        .commit_authorized_constraint_ddl(
            primary_invocation,
            actor,
            9_999,
            &[
                primary_constraint,
                object_write(primary_id, 6),
                CatalogWrite {
                    key: primary_index_key,
                    expected_value: None,
                    payload: Some(primary_index_payload),
                },
                object_write(index_id, 7),
                dependency_write(primary_id, index_id, 4),
                primary_table_epoch,
            ],
            &[primary_audit],
        )
        .map_err(|error| format!("self-FK primary setup failed: {error:?}"))?;

    let foreign_key = constraint_write(
        foreign_key_id,
        table_id,
        5,
        &[column_id],
        Some(primary_id),
        &[column_id],
        None,
    );
    let current_table = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(table_key.as_slice())?
        .ok_or("self-FK table missing before foreign key add")?
        .value()
        .to_vec();
    let mut foreign_key_table_epoch = table_write_with_versions(table_id, schema_id, 1, 3);
    foreign_key_table_epoch.expected_value = Some(current_table);
    let foreign_key_invocation = id(0x98);
    let mut foreign_key_audit = identity_audit_event(0x99, admin_role, foreign_key_id);
    foreign_key_audit.action = AuditAction::AuthorityChange;
    foreign_key_audit.correlation_id = foreign_key_invocation;
    storage
        .commit_catalog_and_audit(
            foreign_key_invocation,
            admin_role,
            9_999,
            &[
                foreign_key,
                object_write(foreign_key_id, 6),
                dependency_write(foreign_key_id, primary_id, 3),
                foreign_key_table_epoch,
            ],
            &[foreign_key_audit],
        )
        .map_err(|error| format!("self-FK foreign-key setup failed: {error:?}"))?;

    let self_row_id = id(0x9c);
    let mut insert = storage.begin_invocation(id(0x9d), actor, 9_999, 1, 1024)?;
    let table_schema = insert.load_catalog_table_schema(table_id)?;
    insert.load_catalog_indexes(&table_schema)?;
    let self_tuple = encode_tuple(
        &table_schema.columns,
        table_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"self".to_vec()))],
    )?;
    insert.stage_put(
        prdb_engine::overlay::RowId {
            table: table_id,
            row: self_row_id,
        },
        &self_tuple,
    )?;
    insert.commit()?;
    let self_row_key = table_row_key(table_id, self_row_id);
    {
        let read = database.begin_read()?;
        assert!(
            read.open_table(ROWS)?.get(&self_row_key[..])?.is_some(),
            "self-referencing row was not committed"
        );
        assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 1);
        assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 1);
    }

    let invocation_id = id(0x9a);
    let mut drop_audit = identity_audit_event(0x9b, actor, table_id);
    drop_audit.correlation_id = invocation_id;
    let dropped = storage.drop_table(invocation_id, actor, table_id, 9_999, drop_audit);
    assert!(
        dropped.is_ok(),
        "dropping the self-FK table returned {dropped:?}"
    );
    assert_eq!(dropped.unwrap().table_id, table_id);
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing self-FK table-drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(2, table_id),
        id_catalog_key(5, table_id),
        id_catalog_key(2, column_id),
        id_catalog_key(6, column_id),
        id_catalog_key(2, primary_id),
        id_catalog_key(8, primary_id),
        id_catalog_key(2, foreign_key_id),
        id_catalog_key(8, foreign_key_id),
        id_catalog_key(2, index_id),
        id_catalog_key(9, index_id),
        dependency_write(foreign_key_id, primary_id, 3).key,
        dependency_write(primary_id, index_id, 4).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "self-FK table-drop record remained: {key:?}"
        );
    }
    assert!(read.open_table(ROWS)?.get(&self_row_key[..])?.is_none());
    assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 0);
    assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 0);
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn catalog_commit_requires_foreign_key_target_dependency_edges()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(88),
            id(9),
            9_999,
            &foreign_key_catalog_writes(false, false),
            &[audit_event(88)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    let mut mismatched_types = foreign_key_catalog_writes(true, true);
    let mismatched_source_column =
        column_write_with_type(id(84), id(82), "object_84", builtin_id(109));
    let source_column_key = mismatched_source_column.key.clone();
    let source_column_write = mismatched_types
        .iter_mut()
        .find(|write| write.key == source_column_key)
        .unwrap();
    *source_column_write = mismatched_source_column;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(114),
            id(9),
            9_999,
            &mismatched_types,
            &[audit_event(114)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(89),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, true),
            &[audit_event(89)],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn adding_foreign_key_rejects_preexisting_orphan_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_key = table_row_key(id(82), [1; 16]);
    commit_rows(
        &database,
        id(115),
        id(9),
        9_999,
        &[RowWrite {
            key: source_key,
            expected_version: None,
            payload: Some(bytea_tuple(b"missing-parent")),
        }],
    )?;

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(116),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, true),
            &[audit_event(116)],
        ),
        Err(CommitError::ForeignKeyViolation)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert!(row(&database, &source_key)?.is_some());
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::CATALOG)?
            .get(&id_catalog_key(8, id(86))[..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn adding_foreign_key_accepts_preexisting_matching_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let ddl_fences = DdlFenceManager::new();
    let ddl_coordinator = DdlFenceCoordinator::new(ddl_fences.clone(), Duration::from_secs(1));
    let mut initial_catalog = foreign_key_catalog_writes(false, true);
    initial_catalog.retain(|write| {
        write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
    });
    assert_eq!(
        commit_catalog_and_audit_with_fences(
            &database,
            &ddl_coordinator,
            id(117),
            id(9),
            9_999,
            &initial_catalog,
            &[audit_event(117)],
        )?,
        1
    );

    let source_key = table_row_key(id(82), [1; 16]);
    let target_key = table_row_key(id(81), [2; 16]);
    let source_payload = bytea_tuple(b"existing-parent");
    let target_payload = bytea_tuple(b"existing-parent");
    let target_schema = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: false,
    }];
    let target_index = prdb_engine::index_key::derive_index_key(
        id(87),
        target_key[16..].try_into().unwrap(),
        1,
        &[id(83)],
        &target_schema,
        &target_payload,
        &[id(83)],
    )?;
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(118),
            id(9),
            9_999,
            &[
                RowWrite {
                    key: target_key,
                    expected_version: None,
                    payload: Some(target_payload),
                },
                RowWrite {
                    key: source_key,
                    expected_version: None,
                    payload: Some(source_payload),
                },
            ],
            &[IndexWrite {
                key: target_index.key,
                row_key: target_key,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: target_index.has_null,
            }],
        )?,
        2
    );

    let source_table_key = id_catalog_key(5, id(82));
    let previous_source_table = initial_catalog
        .iter()
        .find(|write| write.key == source_table_key)
        .unwrap()
        .payload
        .clone();
    let mut source_table = table_write_with_versions(id(82), id(80), 1, 2);
    source_table.expected_value = previous_source_table;
    let new_foreign_key_without_target_fence = [
        constraint_write(id(86), id(82), 5, &[id(84)], Some(id(85)), &[id(83)], None),
        object_write(id(86), 6),
        dependency_write(id(86), id(85), 3),
        source_table.clone(),
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(119),
            id(9),
            9_999,
            &new_foreign_key_without_target_fence,
            &[audit_event(119)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 2);

    let target_table_key = id_catalog_key(5, id(81));
    let previous_target_table = initial_catalog
        .iter()
        .find(|write| write.key == target_table_key)
        .unwrap()
        .payload
        .clone();
    let mut target_table = table_write_with_versions(id(81), id(80), 1, 2);
    target_table.expected_value = previous_target_table;
    let new_foreign_key = [
        new_foreign_key_without_target_fence[0].clone(),
        new_foreign_key_without_target_fence[1].clone(),
        new_foreign_key_without_target_fence[2].clone(),
        source_table,
        target_table,
    ];
    let target_writer = ddl_fences
        .acquire_shared(&[id(81)], Duration::ZERO)
        .unwrap();
    let no_wait_coordinator = DdlFenceCoordinator::new(ddl_fences.clone(), Duration::ZERO);
    assert_eq!(
        commit_catalog_and_audit_with_fences(
            &database,
            &no_wait_coordinator,
            id(120),
            id(9),
            9_999,
            &new_foreign_key,
            &[audit_event(120)],
        ),
        Err(CommitError::DdlFenceUnavailable)
    );
    drop(target_writer);
    assert_eq!(
        commit_catalog_and_audit_with_fences(
            &database,
            &ddl_coordinator,
            id(121),
            id(9),
            9_999,
            &new_foreign_key,
            &[audit_event(121)],
        )?,
        3
    );
    assert_eq!(commit_sequence(&database)?, 3);
    Ok(())
}

pub(super) fn storage_manager_routes_invocations_and_catalog_commits_through_shared_fences()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 128,
            max_active_analytical_invocations: 8,
            max_invocation_age: Duration::from_secs(15 * 60),
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_authorized_index_range_candidates: 128,
            max_analytical_authorized_index_range_candidates: None,
            max_authorized_index_range_bytes: 16 * 1024 * 1024,
            max_analytical_authorized_index_range_bytes: None,
            max_analytical_invocation_scan_work: None,
            max_planner_estimate_requests:
                prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_analytical_planner_estimate_requests: None,
            max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
            max_analytical_planner_estimate_candidates: None,
            max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS
                as usize,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        },
    )?;
    assert_eq!(
        storage.commit_catalog_and_audit(id(190), id(9), 9_999, &[], &[audit_event(190)],)?,
        1
    );

    let mut invocation = storage.begin_invocation(id(191), id(9), 9_999, 2, 64)?;
    let row_id = prdb_engine::overlay::RowId {
        table: id(10),
        row: id(11),
    };
    invocation.stage_put(row_id, b"coordinated")?;
    assert_eq!(invocation.commit()?, 2);
    assert_eq!(
        row(&database, &table_row_key(id(10), id(11)))?
            .unwrap()
            .payload,
        b"coordinated"
    );
    Ok(())
}
