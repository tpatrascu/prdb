use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::{
    lookup_column_id_by_name, lookup_principal_id_by_name, lookup_schema_id_by_name,
};
use prdb_engine::commit::{CatalogWrite, CommitError, commit_catalog_and_audit};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ScalarType, ScalarValue, decode_tuple};
use redb::ReadableDatabase;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn column_default_change_racing_column_drop_preserves_catalog_integrity()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AlterColumnDefaultRequest, CreateTableRequest, TableColumnDefault, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0xa1);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xa2, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xa2),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "column_drop_race_owner"),
            object_write_with_name(actor, 8, "column_drop_race_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0xa3);
    let mut schema_audit = identity_audit_event(0xa4, actor, id(0xa5));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_drop_race_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let table_invocation = id(0xa6);
    let mut table_audit = identity_audit_event(0xa7, actor, id(0xa8));
    table_audit.correlation_id = table_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "default_column_drop_race".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "retained".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "dropped".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: 9_999,
        audit_event: table_audit,
    })?;

    let alter_invocation = id(0xa9);
    let mut alter_audit = identity_audit_event(0xaa, actor, table.column_ids[1]);
    alter_audit.correlation_id = alter_invocation;
    let drop_invocation = id(0xab);
    let mut drop_audit = identity_audit_event(0xac, actor, table.column_ids[1]);
    drop_audit.correlation_id = drop_invocation;
    let table_id = table.table_id;
    let target_column_id = table.column_ids[1];
    let barrier = Arc::new(Barrier::new(2));
    let (alter_result, drop_result) = std::thread::scope(|scope| {
        let alter_barrier = Arc::clone(&barrier);
        let alter_storage = &storage;
        let alter = scope.spawn(move || {
            alter_barrier.wait();
            alter_storage.alter_column_default(AlterColumnDefaultRequest {
                invocation_id: alter_invocation,
                authenticated_actor_id: actor,
                table_id,
                column_id: target_column_id,
                default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(37)))),
                retention_deadline_us: 9_999,
                audit_event: alter_audit,
            })
        });
        barrier.wait();
        let drop = storage.drop_column(
            drop_invocation,
            actor,
            table_id,
            target_column_id,
            9_999,
            drop_audit,
        );
        (
            alter.join().expect("column-default ALTER thread panicked"),
            drop,
        )
    });

    let alter_committed = alter_result.is_ok();
    if let Err(error) = &alter_result {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::TableLifecycleError::Commit(
                    CommitError::Conflict | CommitError::InvalidCatalog
                )
            ),
            "unexpected ALTER result while dropping its column: {error:?}"
        );
    }
    let drop_committed = drop_result.is_ok();
    if let Err(error) = &drop_result {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::TableLifecycleError::Commit(
                    CommitError::Conflict | CommitError::InvalidCatalog
                )
            ),
            "unexpected DROP COLUMN result while changing its default: {error:?}"
        );
    }
    assert!(alter_committed || drop_committed);

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let column_key = id_catalog_key(6, target_column_id);
    assert_eq!(
        catalog.get(column_key.as_slice())?.is_some(),
        !drop_committed,
        "the target column remains exactly when DROP COLUMN did not commit"
    );
    if drop_committed {
        assert!(catalog.get(column_key.as_slice())?.is_none());
        assert!(
            prdb_engine::catalog_index::scan_column_ids_for_table(&read, table_id, 1)?
                .iter()
                .all(|column_id| *column_id != target_column_id)
        );
        assert!(
            prdb_engine::catalog_index::lookup_dependencies_for_dependent(
                &read,
                target_column_id,
            )?
            .is_empty(),
            "dropped column must not retain expression dependencies"
        );
        let expression_prefix = 7u16.to_be_bytes();
        let mut remaining_expressions = 0;
        for entry in catalog.range(expression_prefix.as_slice()..)? {
            let (key, _) = entry?;
            if !key.value().starts_with(&expression_prefix) {
                break;
            }
            remaining_expressions += 1;
        }
        assert_eq!(
            remaining_expressions, 0,
            "dropping the only defaulted column must remove its expression record"
        );
    } else {
        let column = catalog
            .get(column_key.as_slice())?
            .ok_or("surviving column record is missing")?;
        let record = decode_registered_envelope(column.value())?;
        assert_eq!(
            record.fields.iter().any(|field| field.tag == 7),
            alter_committed,
            "the default expression is published exactly when its ALTER commits"
        );
    }
    prdb_engine::startup::validate_persisted_records(&database)?;
    for (invocation_id, committed) in [
        (alter_invocation, alter_committed),
        (drop_invocation, drop_committed),
    ] {
        let outcome = lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing default/drop race outcome")?;
        assert_eq!(
            outcome.status,
            if committed {
                OutcomeStatus::Committed
            } else {
                OutcomeStatus::Aborted
            }
        );
    }

    let table_create_invocation = id(0xb0);
    let mut table_create_audit = identity_audit_event(0xb1, actor, id(0xb2));
    table_create_audit.correlation_id = table_create_invocation;
    let raced_table = storage.create_table(CreateTableRequest {
        invocation_id: table_create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "default_table_drop_race".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: table_create_audit,
    })?;
    let raced_alter_invocation = id(0xb3);
    let mut raced_alter_audit = identity_audit_event(0xb4, actor, raced_table.column_ids[0]);
    raced_alter_audit.correlation_id = raced_alter_invocation;
    let raced_drop_invocation = id(0xb5);
    let mut raced_drop_audit = identity_audit_event(0xb6, actor, raced_table.table_id);
    raced_drop_audit.correlation_id = raced_drop_invocation;
    let raced_barrier = Arc::new(Barrier::new(2));
    let (raced_alter_result, raced_drop_result) = std::thread::scope(|scope| {
        let alter_barrier = Arc::clone(&raced_barrier);
        let alter_storage = &storage;
        let raced_table_id = raced_table.table_id;
        let raced_column_id = raced_table.column_ids[0];
        let alter = scope.spawn(move || {
            alter_barrier.wait();
            alter_storage.alter_column_default(AlterColumnDefaultRequest {
                invocation_id: raced_alter_invocation,
                authenticated_actor_id: actor,
                table_id: raced_table_id,
                column_id: raced_column_id,
                default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(91)))),
                retention_deadline_us: 9_999,
                audit_event: raced_alter_audit,
            })
        });
        raced_barrier.wait();
        let drop = storage.drop_table(
            raced_drop_invocation,
            actor,
            raced_table.table_id,
            9_999,
            raced_drop_audit,
        );
        (
            alter.join().expect("table-default ALTER thread panicked"),
            drop,
        )
    });
    let raced_alter_committed = raced_alter_result.is_ok();
    if let Err(error) = &raced_alter_result {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::TableLifecycleError::Commit(
                    CommitError::Conflict | CommitError::InvalidCatalog
                )
            ),
            "unexpected ALTER result while dropping its table: {error:?}"
        );
    }
    let raced_drop_committed = raced_drop_result.is_ok();
    if let Err(error) = &raced_drop_result {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::TableLifecycleError::Commit(
                    CommitError::Conflict | CommitError::InvalidCatalog
                )
            ),
            "unexpected DROP TABLE result while changing its default: {error:?}"
        );
    }
    assert!(raced_alter_committed || raced_drop_committed);
    let final_read = database.begin_read()?;
    let final_catalog = final_read.open_table(CATALOG)?;
    let raced_column_key = id_catalog_key(6, raced_table.column_ids[0]);
    assert_eq!(
        final_catalog.get(raced_column_key.as_slice())?.is_some(),
        !raced_drop_committed,
        "DROP TABLE removes the defaulted column exactly when it commits"
    );
    if raced_drop_committed {
        assert!(final_catalog.get(raced_column_key.as_slice())?.is_none());
    } else {
        let column = final_catalog
            .get(raced_column_key.as_slice())?
            .ok_or("table-drop race lost its surviving column")?;
        let record = decode_registered_envelope(column.value())?;
        assert_eq!(
            record.fields.iter().any(|field| field.tag == 7),
            raced_alter_committed,
            "default is present exactly when its ALTER committed"
        );
    }
    drop(final_read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    for (invocation_id, committed) in [
        (raced_alter_invocation, raced_alter_committed),
        (raced_drop_invocation, raced_drop_committed),
    ] {
        let outcome = lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing default/table-drop race outcome")?;
        assert_eq!(
            outcome.status,
            if committed {
                OutcomeStatus::Committed
            } else {
                OutcomeStatus::Aborted
            }
        );
    }

    let nullable_create_invocation = id(0xc0);
    let mut nullable_create_audit = identity_audit_event(0xc1, actor, id(0xc2));
    nullable_create_audit.correlation_id = nullable_create_invocation;
    let nullable_table = storage.create_table(CreateTableRequest {
        invocation_id: nullable_create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "nullable_column_drop_race".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "retained".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "nullable_target".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: 9_999,
        audit_event: nullable_create_audit,
    })?;
    let nullable_alter_invocation = id(0xc3);
    let mut nullable_alter_audit = identity_audit_event(0xc4, actor, nullable_table.column_ids[1]);
    nullable_alter_audit.correlation_id = nullable_alter_invocation;
    let nullable_drop_invocation = id(0xc5);
    let mut nullable_drop_audit = identity_audit_event(0xc6, actor, nullable_table.column_ids[1]);
    nullable_drop_audit.correlation_id = nullable_drop_invocation;
    let nullable_table_id = nullable_table.table_id;
    let nullable_target_id = nullable_table.column_ids[1];
    let nullable_barrier = Arc::new(Barrier::new(2));
    let (nullable_alter_result, nullable_drop_result) = std::thread::scope(|scope| {
        let alter_barrier = Arc::clone(&nullable_barrier);
        let alter_storage = &storage;
        let alter = scope.spawn(move || {
            alter_barrier.wait();
            alter_storage.set_column_nullable(
                nullable_alter_invocation,
                actor,
                nullable_table_id,
                nullable_target_id,
                false,
                9_999,
                nullable_alter_audit,
            )
        });
        nullable_barrier.wait();
        let drop = storage.drop_column(
            nullable_drop_invocation,
            actor,
            nullable_table_id,
            nullable_target_id,
            9_999,
            nullable_drop_audit,
        );
        (
            alter.join().expect("nullability ALTER thread panicked"),
            drop,
        )
    });
    let nullable_alter_committed = nullable_alter_result.is_ok();
    if let Err(error) = &nullable_alter_result {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::TableLifecycleError::InvalidRequest
                    | prdb_engine::storage_manager::TableLifecycleError::Commit(
                        CommitError::Conflict | CommitError::InvalidCatalog
                    )
            ),
            "unexpected SET NOT NULL result while dropping its column: {error:?}"
        );
    }
    let nullable_drop_committed = nullable_drop_result.is_ok();
    if let Err(error) = &nullable_drop_result {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::TableLifecycleError::InvalidRequest
                    | prdb_engine::storage_manager::TableLifecycleError::Commit(
                        CommitError::Conflict
                            | CommitError::InvalidCatalog
                            | CommitError::ColumnHasDependencies
                    )
            ),
            "unexpected DROP COLUMN result while changing nullability: {error:?}"
        );
    }
    assert!(nullable_alter_committed || nullable_drop_committed);

    let nullable_read = database.begin_read()?;
    let nullable_catalog = nullable_read.open_table(CATALOG)?;
    let nullable_column_key = id_catalog_key(6, nullable_target_id);
    let nullable_column = nullable_catalog.get(nullable_column_key.as_slice())?;
    assert_eq!(
        nullable_column.is_some(),
        !nullable_drop_committed,
        "the nullable target remains exactly when DROP COLUMN did not commit"
    );
    if nullable_drop_committed {
        assert!(
            prdb_engine::catalog_index::scan_column_ids_for_table(
                &nullable_read,
                nullable_table_id,
                1,
            )?
            .iter()
            .all(|column_id| *column_id != nullable_target_id)
        );
        assert!(
            prdb_engine::catalog_index::lookup_dependencies_for_dependent(
                &nullable_read,
                nullable_target_id,
            )?
            .is_empty(),
            "dropped nullable column must not retain constraint dependencies"
        );
    }
    if let Some(column) = nullable_column {
        let record = decode_registered_envelope(column.value())?;
        let nullable_field = record
            .fields
            .iter()
            .find(|field| field.tag == 6)
            .ok_or("surviving column is missing nullability")?;
        assert_eq!(
            nullable_field.payload,
            vec![u8::from(!nullable_alter_committed)],
            "SET NOT NULL is visible exactly when its ALTER commits"
        );
    }
    drop(nullable_read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    for (invocation_id, committed, has_commit_outcome) in [
        (
            nullable_alter_invocation,
            nullable_alter_committed,
            matches!(
                nullable_alter_result,
                Err(prdb_engine::storage_manager::TableLifecycleError::Commit(_))
            ),
        ),
        (
            nullable_drop_invocation,
            nullable_drop_committed,
            matches!(
                nullable_drop_result,
                Err(prdb_engine::storage_manager::TableLifecycleError::Commit(_))
            ),
        ),
    ] {
        let outcome = lookup_outcome(&database, invocation_id, actor, 1)?;
        if committed || has_commit_outcome {
            let outcome = outcome.ok_or("missing nullability/drop race outcome")?;
            assert_eq!(
                outcome.status,
                if committed {
                    OutcomeStatus::Committed
                } else {
                    OutcomeStatus::Aborted
                }
            );
        } else {
            assert!(
                outcome.is_none(),
                "preflight-rejected DDL must not publish an invocation outcome"
            );
        }
    }
    Ok(())
}

pub(super) fn column_default_and_not_null_alterations_linearize_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AlterColumnDefaultRequest, CreateTableRequest, TableColumnDefault, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0xe1);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xe2, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xe2),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "alter_race_owner"),
            object_write_with_name(actor, 8, "alter_race_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0xe3);
    let mut schema_audit = identity_audit_event(0xe4, actor, id(0xe5));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "alter_race_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let table_invocation = id(0xe6);
    let mut table_audit = identity_audit_event(0xe7, actor, id(0xe8));
    table_audit.correlation_id = table_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "alter_race_table".to_owned(),
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

    let default_invocation = id(0xe9);
    let mut default_audit = identity_audit_event(0xea, actor, table.column_ids[0]);
    default_audit.correlation_id = default_invocation;
    let not_null_invocation = id(0xeb);
    let mut not_null_audit = identity_audit_event(0xec, actor, table.column_ids[0]);
    not_null_audit.correlation_id = not_null_invocation;
    let barrier = Arc::new(Barrier::new(2));
    let (default_result, not_null_result) = std::thread::scope(|scope| {
        let default_barrier = Arc::clone(&barrier);
        let default_storage = &storage;
        let table_id = table.table_id;
        let column_id = table.column_ids[0];
        let default = scope.spawn(move || {
            default_barrier.wait();
            default_storage.alter_column_default(AlterColumnDefaultRequest {
                invocation_id: default_invocation,
                authenticated_actor_id: actor,
                table_id,
                column_id,
                default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(43)))),
                retention_deadline_us: 9_999,
                audit_event: default_audit,
            })
        });
        barrier.wait();
        let not_null = storage.set_column_nullable(
            not_null_invocation,
            actor,
            table.table_id,
            table.column_ids[0],
            false,
            9_999,
            not_null_audit,
        );
        (
            default.join().expect("default ALTER thread panicked"),
            not_null,
        )
    });
    let default_committed = default_result.is_ok();
    if let Err(error) = &default_result {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::TableLifecycleError::InvalidRequest
                    | prdb_engine::storage_manager::TableLifecycleError::Commit(
                        CommitError::Conflict | CommitError::InvalidCatalog
                    )
            ),
            "unexpected SET DEFAULT race result: {error:?}"
        );
    }
    let not_null_committed = not_null_result.is_ok();
    if let Err(error) = &not_null_result {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::TableLifecycleError::InvalidRequest
                    | prdb_engine::storage_manager::TableLifecycleError::Commit(
                        CommitError::Conflict | CommitError::InvalidCatalog
                    )
            ),
            "unexpected SET NOT NULL race result: {error:?}"
        );
    }
    assert!(default_committed || not_null_committed);

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let column = catalog
        .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
        .ok_or("ALTER race lost its column record")?;
    let record = decode_registered_envelope(column.value())?;
    let nullable = record
        .fields
        .iter()
        .find(|field| field.tag == 6)
        .ok_or("column record has no nullability field")?;
    assert_eq!(
        nullable.payload,
        vec![u8::from(!not_null_committed)],
        "NOT NULL is visible exactly when its ALTER commits"
    );
    assert_eq!(
        record.fields.iter().any(|field| field.tag == 7),
        default_committed,
        "DEFAULT is visible exactly when its ALTER commits"
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    for (invocation_id, committed, has_commit_result) in [
        (
            default_invocation,
            default_committed,
            matches!(
                default_result,
                Err(prdb_engine::storage_manager::TableLifecycleError::Commit(_))
            ),
        ),
        (
            not_null_invocation,
            not_null_committed,
            matches!(
                not_null_result,
                Err(prdb_engine::storage_manager::TableLifecycleError::Commit(_))
            ),
        ),
    ] {
        let outcome = lookup_outcome(&database, invocation_id, actor, 1)?;
        if committed || has_commit_result {
            assert_eq!(
                outcome.ok_or("missing ALTER race outcome")?.status,
                if committed {
                    OutcomeStatus::Committed
                } else {
                    OutcomeStatus::Aborted
                }
            );
        } else {
            assert!(outcome.is_none(), "preflight rejection has no outcome");
        }
    }
    Ok(())
}

pub(super) fn storage_manager_adds_column_and_backfills_existing_rows()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AddTableColumnRequest, CreateTableRequest, TableColumnDefault, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let mut database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x5d);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "add_column_owner"),
        object_write_with_name(actor, 8, "add_column_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x5e, admin_role, id(0x5f));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x5f), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x60);
    let mut schema_audit = identity_audit_event(0x61, actor, id(0x62));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "add_column_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x63);
    let mut create_audit = identity_audit_event(0x64, actor, id(0x65));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "add_column_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "number".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let row_id = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x66),
    };
    let mut insert = storage.begin_invocation(id(0x67), actor, 9_999, 2, 1024)?;
    let old_schema = insert.load_catalog_table_schema(table.table_id)?;
    insert.load_catalog_indexes(&old_schema)?;
    let old_tuple = prdb_engine::tuple::encode_tuple(
        &old_schema.columns,
        old_schema.schema_version,
        &[Some(ScalarValue::Integer(7))],
    )?;
    insert.stage_put(row_id, &old_tuple)?;
    insert.commit()?;

    let index_id = id(0x6b);
    let (index_key, index_payload) =
        index_definition_with_columns(index_id, table.table_id, &[table.column_ids[0]]);
    let index_invocation = id(0x6c);
    let mut index_audit = identity_audit_event(0x6d, actor, index_id);
    index_audit.correlation_id = index_invocation;
    storage.commit_authorized_index_ddl(
        index_invocation,
        actor,
        9_999,
        &[
            CatalogWrite {
                key: index_key.clone(),
                expected_value: None,
                payload: Some(index_payload),
            },
            object_write(index_id, 7),
        ],
        &[index_audit],
    )?;

    let stale_writer_invocation = id(0x71);
    let stale_row_id = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x72),
    };
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 2, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = prdb_engine::tuple::encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Integer(99))],
    )?;
    stale_writer.stage_put(stale_row_id, &stale_tuple)?;
    let mut old_cache_reader = storage.begin_invocation(id(0xc0), id(0xc1), 9_999, 1, 1024)?;
    let old_cached_schema = old_cache_reader.load_catalog_table_schema(table.table_id)?;
    assert_eq!(old_cached_schema.schema_version, 1);
    assert_eq!(old_cached_schema.column_ids, table.column_ids);
    let retained_snapshot = database.begin_read()?;

    let add_invocation = id(0x68);
    let mut add_audit = identity_audit_event(0x69, actor, id(0x6a));
    add_audit.correlation_id = add_invocation;
    let add_table_id = table.table_id;
    let add_barrier = Arc::new(Barrier::new(3));
    let storage_ref = &storage;
    let (stale_write_result, added_result) = std::thread::scope(|scope| {
        let writer_barrier = Arc::clone(&add_barrier);
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            stale_writer.commit()
        });
        let ddl_barrier = Arc::clone(&add_barrier);
        let ddl = scope.spawn(move || {
            ddl_barrier.wait();
            storage_ref.add_table_column(AddTableColumnRequest {
                invocation_id: add_invocation,
                authenticated_actor_id: actor,
                table_id: add_table_id,
                column: TableColumnDefinition {
                    name: "label".to_owned(),
                    scalar_type: ScalarType::Text,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: Some(TableColumnDefault::Literal(Some(ScalarValue::Text(
                        "ready".to_owned(),
                    )))),
                },
                retention_deadline_us: 9_999,
                audit_event: add_audit,
            })
        });
        add_barrier.wait();
        (
            writer.join().expect("ADD COLUMN writer panicked"),
            ddl.join().expect("ADD COLUMN DDL panicked"),
        )
    });
    assert!(
        added_result.is_ok(),
        "adding column failed: {added_result:?}"
    );
    let added = added_result?;
    assert_eq!(added.schema_version, 2);
    let added_schema_columns = [
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
    ];
    let stale_write_sequence = match stale_write_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    if let Some(sequence) = stale_write_sequence {
        assert!(sequence < added.commit_sequence);
    }
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing staged ADD COLUMN writer outcome")?
            .status,
        if stale_write_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        row(&database, &table_row_key(table.table_id, stale_row_id.row))?.is_some(),
        stale_write_sequence.is_some()
    );
    assert_eq!(commit_sequence(&database)?, added.commit_sequence);

    let mut new_cache_reader = storage.begin_invocation(id(0xc2), id(0xc3), 9_999, 1, 1024)?;
    let new_cached_schema = new_cache_reader.load_catalog_table_schema(table.table_id)?;
    assert_eq!(new_cached_schema.schema_version, 2);
    assert_eq!(
        new_cached_schema.column_ids,
        [table.column_ids[0], added.column_id]
    );
    assert_eq!(
        old_cache_reader
            .load_catalog_table_schema(table.table_id)?
            .column_ids,
        old_cached_schema.column_ids,
        "the old invocation must keep its own pinned schema cache"
    );
    old_cache_reader.abort()?;
    new_cache_reader.abort()?;
    drop(old_cache_reader);
    drop(new_cache_reader);

    let retained_default_snapshot = database.begin_read()?;
    for generation in 0..64u8 {
        let churn_invocation = id(0x80 + generation);
        let mut churn = storage.begin_invocation(churn_invocation, actor, 9_999, 2, 1024)?;
        let churn_schema = churn.load_catalog_table_schema(table.table_id)?;
        churn.load_catalog_indexes(&churn_schema)?;
        let churn_tuple = prdb_engine::tuple::encode_tuple(
            &churn_schema.columns,
            churn_schema.schema_version,
            &[
                Some(ScalarValue::Integer(7)),
                Some(ScalarValue::Text(format!("generation-{generation}"))),
            ],
        )?;
        churn.stage_put(row_id, &churn_tuple)?;
        churn.commit()?;
    }
    assert_eq!(
        lookup_column_id_by_name(&retained_snapshot, table.table_id, "label")?,
        None
    );
    let retained_rows = prdb_engine::snapshot::RowSnapshot::new(retained_snapshot);
    let retained_row = RowRecord::decode(
        &retained_rows
            .get(&table_row_key(table.table_id, row_id.row))?
            .ok_or("retained reader lost its pre-DDL row")?,
    )?;
    let retained_tuple = decode_tuple(
        &old_schema.columns,
        old_schema.schema_version,
        &retained_row.payload,
    )?;
    assert_eq!(retained_tuple.values, vec![Some(ScalarValue::Integer(7))]);
    let retained_default_rows = prdb_engine::snapshot::RowSnapshot::new(retained_default_snapshot);
    let retained_default_row = RowRecord::decode(
        &retained_default_rows
            .get(&table_row_key(table.table_id, row_id.row))?
            .ok_or("post-DDL retained reader lost its backfilled row")?,
    )?;
    let retained_default_tuple = decode_tuple(
        &[
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        ],
        added.schema_version,
        &retained_default_row.payload,
    )?;
    assert_eq!(
        retained_default_tuple.values,
        vec![
            Some(ScalarValue::Integer(7)),
            Some(ScalarValue::Text("ready".to_owned())),
        ]
    );
    if stale_write_sequence.is_some() {
        let row_after_add = row(&database, &table_row_key(table.table_id, stale_row_id.row))?
            .ok_or("ADD COLUMN race lost the writer-first row")?;
        let tuple = prdb_engine::tuple::decode_tuple(
            &added_schema_columns,
            added.schema_version,
            &row_after_add.payload,
        )?;
        assert_eq!(
            tuple.values,
            [
                Some(ScalarValue::Integer(99)),
                Some(ScalarValue::Text("ready".to_owned()))
            ]
        );
    } else {
        assert!(row(&database, &table_row_key(table.table_id, stale_row_id.row))?.is_none());
    }
    let stored = row(&database, &table_row_key(table.table_id, row_id.row))?
        .ok_or("backfilled row is missing")?;
    let tuple = prdb_engine::tuple::decode_tuple(
        &[
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        ],
        added.schema_version,
        &stored.payload,
    )?;
    assert_eq!(
        tuple.values,
        vec![
            Some(ScalarValue::Integer(7)),
            Some(ScalarValue::Text("generation-63".to_owned())),
        ]
    );
    let added_schema_ids = [table.column_ids[0], added.column_id];
    let indexed_key = prdb_engine::index_key::derive_index_key(
        index_id,
        row_id.row,
        added.schema_version,
        &added_schema_ids,
        &added_schema_columns,
        &stored.payload,
        &[table.column_ids[0]],
    )?;
    let read = database.begin_read()?;
    let index_record = read
        .open_table(CATALOG)?
        .get(index_key.as_slice())?
        .ok_or("index catalog record disappeared")?;
    assert_eq!(
        u64::from_le_bytes(
            decode_registered_envelope(index_record.value())?
                .fields
                .iter()
                .find(|field| field.tag == 10)
                .ok_or("index schema version is missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        added.schema_version
    );
    let locator = read
        .open_table(schema::INDEXES)?
        .get(indexed_key.key.as_slice())?
        .ok_or("rewritten index entry is missing")?;
    assert_eq!(
        IndexLocator::decode_for_key(&indexed_key.key, locator.value())?.row_version,
        stored.row_version
    );
    drop(index_record);
    drop(locator);
    drop(read);

    let nullable_add_invocation = id(0x6e);
    let mut nullable_add_audit = identity_audit_event(0x6f, actor, id(0x70));
    nullable_add_audit.correlation_id = nullable_add_invocation;
    let nullable_added = storage.add_table_column(AddTableColumnRequest {
        invocation_id: nullable_add_invocation,
        authenticated_actor_id: actor,
        table_id: table.table_id,
        column: TableColumnDefinition {
            name: "comment".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        },
        retention_deadline_us: 9_999,
        audit_event: nullable_add_audit,
    })?;
    assert_eq!(nullable_added.schema_version, 3);
    let stored = row(&database, &table_row_key(table.table_id, row_id.row))?
        .ok_or("row backfilled with NULL is missing")?;
    let nullable_tuple = prdb_engine::tuple::decode_tuple(
        &[
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: true,
            },
        ],
        nullable_added.schema_version,
        &stored.payload,
    )?;
    assert_eq!(
        nullable_tuple.values,
        vec![
            Some(ScalarValue::Integer(7)),
            Some(ScalarValue::Text("generation-63".to_owned())),
            None,
        ]
    );
    let nullable_schema_ids = [
        table.column_ids[0],
        added.column_id,
        nullable_added.column_id,
    ];
    let nullable_schema_columns = [
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
    ];
    let nullable_indexed_key = prdb_engine::index_key::derive_index_key(
        index_id,
        row_id.row,
        nullable_added.schema_version,
        &nullable_schema_ids,
        &nullable_schema_columns,
        &stored.payload,
        &[table.column_ids[0]],
    )?;
    let read = database.begin_read()?;
    let index_record = read
        .open_table(CATALOG)?
        .get(index_key.as_slice())?
        .ok_or("index catalog record disappeared after NULL backfill")?;
    assert_eq!(
        u64::from_le_bytes(
            decode_registered_envelope(index_record.value())?
                .fields
                .iter()
                .find(|field| field.tag == 10)
                .ok_or("index schema version is missing after NULL backfill")?
                .payload
                .as_slice()
                .try_into()?
        ),
        nullable_added.schema_version
    );
    let locator = read
        .open_table(schema::INDEXES)?
        .get(nullable_indexed_key.key.as_slice())?
        .ok_or("rewritten index entry is missing after NULL backfill")?;
    assert_eq!(
        IndexLocator::decode_for_key(&nullable_indexed_key.key, locator.value())?.row_version,
        stored.row_version
    );
    drop(index_record);
    drop(locator);
    drop(read);

    drop(storage);
    assert!(matches!(
        database.compact(),
        Err(redb::CompactionError::TransactionInProgress)
    ));
    drop(retained_rows);
    drop(retained_default_rows);
    assert!(
        database.compact()?,
        "catalog row churn should produce reclaimable pages"
    );
    prdb_engine::startup::validate_persisted_records(&database)?;
    let compacted = row(&database, &table_row_key(table.table_id, row_id.row))?
        .ok_or("compaction lost the current backfilled row")?;
    let compacted_tuple = decode_tuple(
        &nullable_schema_columns,
        nullable_added.schema_version,
        &compacted.payload,
    )?;
    assert_eq!(
        compacted_tuple.values,
        vec![
            Some(ScalarValue::Integer(7)),
            Some(ScalarValue::Text("generation-63".to_owned())),
            None,
        ]
    );
    Ok(())
}

pub(super) fn catalog_commit_rejects_unresolved_schema_search_path_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(0x75),
            id(9),
            9_999,
            &[
                schema_write_with_search_path(id(0x81), &[[0x82; 16]]),
                object_write(id(0x81), 1),
            ],
            &[audit_event(0x75)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn principal_name_index_tracks_renames_per_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let principal_id = id(0x83);
    let name = "indexed_principal";
    let mut principal = principal_write_with_name(principal_id, 1, 1, name);
    let mut object = object_write_with_name(principal_id, 8, name);
    let initial_principal = principal.payload.clone().unwrap();
    let initial_object = object.payload.clone().unwrap();
    let mut event = audit_event(0x76);
    event.actor_id = Some(builtin_owner_id());
    commit_catalog_and_audit(
        &database,
        id(0x76),
        builtin_owner_id(),
        9_999,
        &[principal.clone(), object.clone()],
        &[event],
    )?;
    let old_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_principal_id_by_name(&old_snapshot, name)?,
        Some(principal_id)
    );

    principal = principal_write_with_name(principal_id, 1, 2, "renamed_principal");
    principal.expected_value = Some(initial_principal);
    object = object_write_with_name(principal_id, 8, "renamed_principal");
    object.expected_value = Some(initial_object);
    let mut event = audit_event(0x77);
    event.actor_id = Some(builtin_owner_id());
    commit_catalog_and_audit(
        &database,
        id(0x77),
        builtin_owner_id(),
        9_999,
        &[principal, object],
        &[event],
    )?;

    assert_eq!(
        lookup_principal_id_by_name(&old_snapshot, name)?,
        Some(principal_id)
    );
    let new_snapshot = database.begin_read()?;
    assert_eq!(lookup_principal_id_by_name(&new_snapshot, name)?, None);
    assert_eq!(
        lookup_principal_id_by_name(&new_snapshot, "renamed_principal")?,
        Some(principal_id)
    );
    Ok(())
}

pub(super) fn schema_name_index_tracks_renames_per_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let schema_id = id(0x84);
    let initial_object = object_write_with_name(schema_id, 1, "indexed_schema");
    let initial_object_bytes = initial_object.payload.clone().unwrap();
    let mut event = audit_event(0x78);
    event.actor_id = Some(builtin_owner_id());
    commit_catalog_and_audit(
        &database,
        id(0x78),
        builtin_owner_id(),
        9_999,
        &[initial_object, schema_write(schema_id)],
        &[event],
    )?;
    let old_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_schema_id_by_name(&old_snapshot, "indexed_schema")?,
        Some(schema_id)
    );

    let mut renamed_object = object_write_with_name(schema_id, 1, "renamed_schema");
    renamed_object.expected_value = Some(initial_object_bytes);
    let mut event = audit_event(0x79);
    event.actor_id = Some(builtin_owner_id());
    commit_catalog_and_audit(
        &database,
        id(0x79),
        builtin_owner_id(),
        9_999,
        &[renamed_object],
        &[event],
    )?;

    assert_eq!(
        lookup_schema_id_by_name(&old_snapshot, "indexed_schema")?,
        Some(schema_id)
    );
    let new_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_schema_id_by_name(&new_snapshot, "indexed_schema")?,
        None
    );
    assert_eq!(
        lookup_schema_id_by_name(&new_snapshot, "renamed_schema")?,
        Some(schema_id)
    );
    Ok(())
}
