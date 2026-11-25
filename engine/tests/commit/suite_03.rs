use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::{
    lookup_column_id_by_name, lookup_column_id_by_ordinal, lookup_relation_id_by_name,
};
use prdb_engine::commit::{
    CatalogWrite, CommitError, IndexWrite, RowWrite, commit_rows_and_indexes,
};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ROWS};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::ReadableDatabase;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_drop_table_index_entry_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x90);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x91, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x91),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_index_limit_owner"),
            object_write_with_name(actor, 8, "drop_index_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let index_id = id(0x88);
    let (index_key, index_payload) = index_definition(index_id, id(16));
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(0x92),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: index_key.clone(),
                    expected_value: None,
                    payload: Some(index_payload),
                },
                object_write(index_id, 7),
            ],
            &[audit_event(0x92)],
        )?,
        2
    );

    let payload = bytea_tuple(b"index cleanup limit");
    let make_batch = |start: u64, end: u64| {
        let mut rows = Vec::with_capacity((end - start + 1) as usize);
        let mut indexes = Vec::with_capacity((end - start + 1) as usize * 2);
        for row_number in start..=end {
            let mut row_id = [0; 16];
            row_id[8..].copy_from_slice(&row_number.to_be_bytes());
            let row_key = table_row_key(id(16), row_id);
            let first = typed_bytea_index_key(id(0x44), &row_key, &payload);
            let second = typed_bytea_index_key(index_id, &row_key, &payload);
            rows.push(RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(payload.clone()),
            });
            for derived in [first, second] {
                indexes.push(IndexWrite {
                    key: derived.key,
                    row_key,
                    expected_version: None,
                    present: true,
                    unique: false,
                    null_exempt: false,
                });
            }
        }
        (rows, indexes)
    };
    let (first_rows, first_indexes) = make_batch(1, 32_768);
    assert_eq!(first_indexes.len(), 65_536);
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(0x93),
            actor,
            9_999,
            &first_rows,
            &first_indexes,
        )?,
        3
    );
    let (last_rows, last_indexes) = make_batch(32_769, 32_769);
    assert_eq!(
        commit_rows_and_indexes(&database, id(0x94), actor, 9_999, &last_rows, &last_indexes,)?,
        4
    );

    let over_limit_invocation = id(0x95);
    let mut over_limit_audit = identity_audit_event(0x95, actor, id(16));
    over_limit_audit.correlation_id = over_limit_invocation;
    assert_eq!(
        storage.drop_table(
            over_limit_invocation,
            actor,
            id(16),
            9_999,
            over_limit_audit,
        ),
        Err(TableLifecycleError::Commit(
            CommitError::IndexDdlLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, 4);
    assert_eq!(
        lookup_outcome(&database, over_limit_invocation, actor, 1)?
            .ok_or("missing table index-entry limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(id_catalog_key(5, id(16)).as_slice())?
            .is_some()
    );
    assert!(read.open_table(ROWS)?.get(&last_rows[0].key[..])?.is_some());
    assert!(
        read.open_table(schema::INDEXES)?
            .get(&last_indexes[0].key[..])?
            .is_some()
    );
    drop(read);

    let last_row_key = last_rows[0].key;
    let first_index_key = typed_bytea_index_key(id(0x44), &last_row_key, &payload).key;
    let second_index_key = typed_bytea_index_key(index_id, &last_row_key, &payload).key;
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(0x96),
            actor,
            9_999,
            &[RowWrite {
                key: last_row_key,
                expected_version: Some(4),
                payload: None,
            }],
            &[
                IndexWrite {
                    key: first_index_key,
                    row_key: last_row_key,
                    expected_version: Some(4),
                    present: false,
                    unique: false,
                    null_exempt: false,
                },
                IndexWrite {
                    key: second_index_key,
                    row_key: last_row_key,
                    expected_version: Some(4),
                    present: false,
                    unique: false,
                    null_exempt: false,
                },
            ],
        )?,
        5
    );
    let boundary_invocation = id(0x97);
    let mut boundary_audit = identity_audit_event(0x97, actor, id(16));
    boundary_audit.correlation_id = boundary_invocation;
    assert_eq!(
        storage
            .drop_table(boundary_invocation, actor, id(16), 9_999, boundary_audit,)?
            .table_id,
        id(16)
    );
    assert_eq!(commit_sequence(&database)?, 6);
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact table index-entry boundary outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(id_catalog_key(5, id(16)).as_slice())?.is_none());
    for dropped_index_id in [id(0x44), index_id] {
        assert!(
            catalog
                .get(id_catalog_key(9, dropped_index_id).as_slice())?
                .is_none()
        );
        assert!(
            catalog
                .get(id_catalog_key(2, dropped_index_id).as_slice())?
                .is_none()
        );
    }
    assert!(read.open_table(ROWS)?.get(&last_row_key[..])?.is_none());
    for dropped_index_id in [id(0x44), index_id] {
        assert!(
            read.open_table(schema::INDEXES)?
                .range(dropped_index_id.as_slice()..)?
                .next()
                .transpose()?
                .is_none_or(|(key, _)| !key.value().starts_with(&dropped_index_id))
        );
    }
    assert!(
        read.open_table(schema::ROW_INDEXES)?
            .range(id(16).as_slice()..)?
            .next()
            .transpose()?
            .is_none_or(|(key, _)| !key.value().starts_with(&id(16)))
    );
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_renames_tables_and_drop_column_rewrites_rows()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0x60);
    let successor_owner = id(0xd0);
    let successor_role = id(0xd6);
    let outsider = id(0xd7);
    let revocation_admin = id(0xc8);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "column_drop_owner"),
        object_write_with_name(actor, 8, "column_drop_owner"),
        principal_write_with_name(successor_owner, 1, 1, "successor_owner"),
        object_write_with_name(successor_owner, 8, "successor_owner"),
        principal_write_with_name(successor_role, 2, 1, "successor_role"),
        object_write_with_name(successor_role, 9, "successor_role"),
        membership_write(successor_owner, successor_role),
        dependency_write(successor_owner, successor_role, 5),
        principal_write_with_name(outsider, 1, 1, "table_ddl_outsider"),
        object_write_with_name(outsider, 8, "table_ddl_outsider"),
        principal_write_with_name(revocation_admin, 1, 1, "role_revocation_admin"),
        object_write_with_name(revocation_admin, 8, "role_revocation_admin"),
        credential_verifier_write(
            id(0xc9),
            revocation_admin,
            &[0xc9; 32],
            1,
            i64::MAX,
            None,
            1,
        ),
        membership_write(revocation_admin, admin_role),
        dependency_write(revocation_admin, admin_role, 5),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x61, admin_role, id(0x62));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x62), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x63);
    let mut schema_audit = identity_audit_event(0x64, actor, id(0x65));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_drop_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x66);
    let mut create_audit = identity_audit_event(0x67, actor, id(0x68));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "column_drop_me".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "obsolete".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "kept".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let old_relation_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_relation_id_by_name(&old_relation_snapshot, schema.schema_id, "column_drop_me")?,
        Some(table.table_id)
    );
    assert_eq!(
        lookup_column_id_by_name(&old_relation_snapshot, table.table_id, "obsolete")?,
        Some(table.column_ids[0])
    );
    assert_eq!(
        lookup_column_id_by_ordinal(&old_relation_snapshot, table.table_id, 0)?,
        Some(table.column_ids[0])
    );
    let rejected_column_rename = id(0x80);
    let mut rejected_column_rename_audit = identity_audit_event(0x81, actor, id(0x82));
    rejected_column_rename_audit.correlation_id = rejected_column_rename;
    let duplicate_column_rename = storage.rename_column(
        rejected_column_rename,
        actor,
        table.table_id,
        table.column_ids[0],
        "kept",
        9_999,
        rejected_column_rename_audit,
    );
    assert!(
        matches!(
            duplicate_column_rename,
            Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
                CommitError::InvalidCatalog
            ))
        ),
        "unexpected duplicate column rename result: {duplicate_column_rename:?}"
    );
    assert_eq!(
        lookup_outcome(&database, rejected_column_rename, actor, 1)?
            .ok_or("missing duplicate column-name outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let stale_column_writer_invocation = id(0x86);
    let stale_column_row = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x87),
    };
    let mut stale_column_writer =
        storage.begin_invocation(stale_column_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_column_schema = stale_column_writer.load_catalog_table_schema(table.table_id)?;
    stale_column_writer.load_catalog_indexes(&stale_column_schema)?;
    let stale_column_tuple = encode_tuple(
        &stale_column_schema.columns,
        stale_column_schema.schema_version,
        &[
            Some(ScalarValue::Integer(17)),
            Some(ScalarValue::Text("old shape".to_owned())),
        ],
    )?;
    stale_column_writer.stage_put(stale_column_row, &stale_column_tuple)?;
    let column_rename = id(0x83);
    let mut column_rename_audit = identity_audit_event(0x84, actor, id(0x85));
    column_rename_audit.correlation_id = column_rename;
    let column_rename_barrier = Arc::new(Barrier::new(2));
    let storage_ref = &storage;
    let rename_table_id = table.table_id;
    let rename_column_id = table.column_ids[0];
    let (stale_column_write_result, column_rename_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&column_rename_barrier);
        let ddl = scope.spawn(move || {
            barrier.wait();
            storage_ref.rename_column(
                column_rename,
                actor,
                rename_table_id,
                rename_column_id,
                "retired",
                9_999,
                column_rename_audit,
            )
        });
        column_rename_barrier.wait();
        let write = stale_column_writer.commit();
        (write, ddl.join().expect("column rename thread panicked"))
    });
    assert!(
        column_rename_result.is_ok(),
        "column rename failed: {column_rename_result:?}"
    );
    let column_rename_sequence = column_rename_result?;
    assert!(column_rename_sequence > table.commit_sequence);
    assert_eq!(
        lookup_column_id_by_name(&old_relation_snapshot, table.table_id, "obsolete")?,
        Some(table.column_ids[0])
    );
    let renamed_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_column_id_by_name(&renamed_snapshot, table.table_id, "retired")?,
        Some(table.column_ids[0])
    );
    assert_eq!(
        lookup_column_id_by_ordinal(&renamed_snapshot, table.table_id, 0)?,
        Some(table.column_ids[0])
    );
    let stale_column_write_sequence = match stale_column_write_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    if let Some(sequence) = stale_column_write_sequence {
        assert!(sequence < column_rename_sequence);
    }
    assert_eq!(
        row(
            &database,
            &table_row_key(table.table_id, stale_column_row.row)
        )?
        .is_some(),
        stale_column_write_sequence.is_some()
    );
    assert_eq!(
        lookup_outcome(&database, stale_column_writer_invocation, actor, 1)?
            .ok_or("missing stale column-rename writer outcome")?
            .status,
        if stale_column_write_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let column_object = decode_registered_envelope(
            catalog
                .get(id_catalog_key(2, table.column_ids[0]).as_slice())?
                .ok_or("renamed column object missing")?
                .value(),
        )?;
        assert_eq!(
            column_object
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("column object name missing")?
                .payload,
            b"retired"
        );
        assert_eq!(
            u64::from_le_bytes(
                column_object
                    .fields
                    .iter()
                    .find(|field| field.tag == 5)
                    .ok_or("column object version missing")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            2
        );
        let column_record = decode_registered_envelope(
            catalog
                .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
                .ok_or("renamed column record missing")?
                .value(),
        )?;
        assert_eq!(
            column_record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("column record name missing")?
                .payload,
            b"retired"
        );
    }
    let stale_writer_invocation = id(0x5b);
    let stale_row_id = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x5c),
    };
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 2, 64)?;
    let pinned_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    assert_eq!(pinned_schema.schema_version, 1);
    stale_writer.load_catalog_indexes(&pinned_schema)?;
    let stale_payload = prdb_engine::tuple::encode_tuple(
        &pinned_schema.columns,
        pinned_schema.schema_version,
        &[
            Some(ScalarValue::Integer(99)),
            Some(ScalarValue::Text("stale".to_owned())),
        ],
    )?;
    stale_writer.stage_put(stale_row_id, &stale_payload)?;
    let mut old_name_reader = storage.begin_invocation(id(0xf1), actor, 9_999, 1, 1024)?;
    let old_name_schema = old_name_reader.load_catalog_table_schema(table.table_id)?;
    assert_eq!(old_name_schema.table_id, table.table_id);
    let rename_invocation = id(0x76);
    let mut rename_audit = identity_audit_event(0x77, actor, id(0x78));
    rename_audit.correlation_id = rename_invocation;
    let table_rename_barrier = Arc::new(Barrier::new(2));
    let storage_ref = &storage;
    let renamed_table_id = table.table_id;
    let (rename_result, stale_write_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&table_rename_barrier);
        let rename = scope.spawn(move || {
            barrier.wait();
            storage_ref.rename_table(
                rename_invocation,
                actor,
                renamed_table_id,
                "renamed_column_drop_me",
                9_999,
                rename_audit,
            )
        });
        table_rename_barrier.wait();
        let write = stale_writer.commit();
        (rename.join().expect("table rename thread panicked"), write)
    });
    let rename_sequence = rename_result?;
    assert!(rename_sequence > table.commit_sequence);
    assert_eq!(
        lookup_relation_id_by_name(&old_relation_snapshot, schema.schema_id, "column_drop_me")?,
        Some(table.table_id)
    );
    let renamed_relation_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_relation_id_by_name(
            &renamed_relation_snapshot,
            schema.schema_id,
            "column_drop_me"
        )?,
        None
    );
    assert_eq!(
        lookup_relation_id_by_name(
            &renamed_relation_snapshot,
            schema.schema_id,
            "renamed_column_drop_me",
        )?,
        Some(table.table_id)
    );
    let old_name_schema_after_rename = old_name_reader.load_catalog_table_schema(table.table_id)?;
    assert_eq!(old_name_schema_after_rename, old_name_schema);
    let mut renamed_name_reader = storage.begin_invocation(id(0xf2), actor, 9_999, 1, 1024)?;
    assert_eq!(
        renamed_name_reader
            .load_catalog_table_schema(table.table_id)?
            .table_id,
        table.table_id
    );
    old_name_reader
        .commit()
        .map_err(|error| format!("old-name reader commit after rename: {error:?}"))?;
    renamed_name_reader
        .commit()
        .map_err(|error| format!("new-name reader commit after rename: {error:?}"))?;
    let (stale_write_sequence, stale_write_status) = match stale_write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            (None, OutcomeStatus::Aborted)
        }
        Err(error) => panic!("unexpected table-rename race writer result: {error:?}"),
    };
    if let Some(sequence) = stale_write_sequence {
        assert!(sequence < rename_sequence);
    }
    assert_eq!(
        row(&database, &table_row_key(table.table_id, stale_row_id.row))?
            .map(|stored| stored.payload),
        (stale_write_status == OutcomeStatus::Committed).then_some(stale_payload),
        "row publication must match the table-rename race order"
    );
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale-writer outcome")?
            .status,
        stale_write_status
    );
    let duplicate_table = storage.create_table(CreateTableRequest {
        invocation_id: id(0x79),
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "occupied_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: {
            let mut event = identity_audit_event(0x7a, actor, id(0x7b));
            event.correlation_id = id(0x79);
            event
        },
    })?;
    assert_ne!(duplicate_table.table_id, table.table_id);
    let duplicate_rename_invocation = id(0x7c);
    let mut duplicate_rename_audit = identity_audit_event(0x7d, actor, id(0x7e));
    duplicate_rename_audit.correlation_id = duplicate_rename_invocation;
    let duplicate_rename = storage.rename_table(
        duplicate_rename_invocation,
        actor,
        table.table_id,
        "occupied_table",
        9_999,
        duplicate_rename_audit,
    );
    assert!(
        matches!(
            duplicate_rename,
            Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
                prdb_engine::commit::CommitError::InvalidCatalog
            ))
        ),
        "duplicate rename returned {duplicate_rename:?}"
    );
    assert_eq!(
        lookup_outcome(&database, duplicate_rename_invocation, actor, 1)?
            .ok_or("missing duplicate table-name outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_object = decode_registered_envelope(
            catalog
                .get(id_catalog_key(2, table.table_id).as_slice())?
                .ok_or("renamed table object missing")?
                .value(),
        )?;
        assert_eq!(
            table_object
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table name missing")?
                .payload,
            b"renamed_column_drop_me"
        );
        let table_record = decode_registered_envelope(
            catalog
                .get(id_catalog_key(5, table.table_id).as_slice())?
                .ok_or("renamed table record missing")?
                .value(),
        )?;
        assert_eq!(
            u64::from_le_bytes(
                table_record
                    .fields
                    .iter()
                    .find(|field| field.tag == 2)
                    .ok_or("schema version missing")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            1
        );
        assert_eq!(
            u64::from_le_bytes(
                table_record
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("write epoch missing")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            3
        );
    }
    let row_key = table_row_key(table.table_id, id(0x69));
    let old_payload = encode_tuple(
        &[
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        ],
        1,
        &[
            Some(ScalarValue::Integer(73)),
            Some(ScalarValue::Text("preserved".to_owned())),
        ],
    )?;
    let row_record = RowRecord {
        row_version: table.commit_sequence,
        payload: old_payload,
    }
    .encode()?;
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&row_key[..], &row_record[..])?;
    write.commit()?;

    let index_id = id(0x70);
    let (index_catalog_key, index_definition) =
        index_definition_with_columns(index_id, table.table_id, &[table.column_ids[1]]);
    let index_invocation = id(0x71);
    let mut index_audit = identity_audit_event(0x72, actor, index_id);
    index_audit.correlation_id = index_invocation;
    storage.commit_authorized_index_ddl(
        index_invocation,
        actor,
        9_999,
        &[
            CatalogWrite {
                key: index_catalog_key.clone(),
                expected_value: None,
                payload: Some(index_definition),
            },
            object_write(index_id, 7),
        ],
        &[index_audit],
    )?;
    let old_index_key = prdb_engine::index_key::derive_index_key(
        index_id,
        row_key[16..].try_into()?,
        1,
        &table.column_ids,
        &[
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        ],
        &RowRecord::decode(&row_record)?.payload,
        &[table.column_ids[1]],
    )?;
    let old_index_row_version = {
        let read = database.begin_read()?;
        let indexes = read.open_table(schema::INDEXES)?;
        let locator = indexes
            .get(old_index_key.key.as_slice())?
            .ok_or("index entry missing after index build")?;
        IndexLocator::decode_for_key(&old_index_key.key, locator.value())?.row_version
    };

    let denied_drop_invocation = id(0x73);
    let mut denied_drop_audit = identity_audit_event(0x74, actor, id(0x75));
    denied_drop_audit.correlation_id = denied_drop_invocation;
    assert!(matches!(
        storage.drop_column(
            denied_drop_invocation,
            actor,
            table.table_id,
            table.column_ids[1],
            9_999,
            denied_drop_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::ColumnHasDependencies
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, denied_drop_invocation, actor, 1)?
            .ok_or("missing denied column-drop outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let old_reader_invocation = id(0x8a);
    let mut old_reader = storage.begin_invocation(old_reader_invocation, actor, 9_999, 1, 1024)?;
    let old_reader_schema = old_reader.load_catalog_table_schema(table.table_id)?;
    assert_eq!(old_reader_schema.columns.len(), 2);
    let old_reader_row = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x69),
    };
    let before_drop = old_reader
        .read(old_reader_row)?
        .ok_or("old reader row missing before DROP COLUMN")?;
    assert_eq!(
        prdb_engine::tuple::decode_tuple(
            &old_reader_schema.columns,
            old_reader_schema.schema_version,
            &before_drop,
        )?
        .values,
        vec![
            Some(ScalarValue::Integer(73)),
            Some(ScalarValue::Text("preserved".to_owned())),
        ]
    );

    let stale_drop_invocation = id(0x88);
    let stale_drop_row = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x89),
    };
    let mut stale_drop_writer =
        storage.begin_invocation(stale_drop_invocation, actor, 9_999, 1, 1024)?;
    let stale_drop_schema = stale_drop_writer.load_catalog_table_schema(table.table_id)?;
    stale_drop_writer.load_catalog_indexes(&stale_drop_schema)?;
    let stale_drop_tuple = encode_tuple(
        &stale_drop_schema.columns,
        stale_drop_schema.schema_version,
        &[
            Some(ScalarValue::Integer(23)),
            Some(ScalarValue::Text("staged before drop".to_owned())),
        ],
    )?;
    stale_drop_writer.stage_put(stale_drop_row, &stale_drop_tuple)?;

    let drop_invocation = id(0x6a);
    let drop_table_id = table.table_id;
    let drop_column_id = table.column_ids[0];
    let mut drop_audit = identity_audit_event(0x6b, actor, id(0x6c));
    drop_audit.correlation_id = drop_invocation;
    let drop_race_barrier = Arc::new(Barrier::new(2));
    let (write_result, dropped) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&drop_race_barrier);
        let storage = &storage;
        let ddl = scope.spawn(move || {
            barrier.wait();
            storage.drop_column(
                drop_invocation,
                actor,
                drop_table_id,
                drop_column_id,
                9_999,
                drop_audit,
            )
        });
        drop_race_barrier.wait();
        let write = stale_drop_writer.commit();
        let ddl = ddl.join().expect("DROP COLUMN thread panicked");
        (write, ddl)
    });
    let dropped = dropped?;
    assert_eq!(dropped.column_id, table.column_ids[0]);
    let (write_sequence, write_status) = match write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            (None, OutcomeStatus::Aborted)
        }
        Err(error) => panic!("unexpected staged-write result in DROP COLUMN race: {error:?}"),
    };
    if let Some(sequence) = write_sequence {
        assert!(sequence < dropped.commit_sequence);
    }
    let after_drop = old_reader
        .read(old_reader_row)?
        .ok_or("old reader lost its row after DROP COLUMN")?;
    assert_eq!(
        prdb_engine::tuple::decode_tuple(
            &old_reader_schema.columns,
            old_reader_schema.schema_version,
            &after_drop,
        )?
        .values,
        vec![
            Some(ScalarValue::Integer(73)),
            Some(ScalarValue::Text("preserved".to_owned())),
        ],
        "pinned reader must retain the old row version and two-column schema"
    );
    let mut new_reader = storage.begin_invocation(id(0x8b), actor, 9_999, 1, 1024)?;
    let new_reader_schema = new_reader.load_catalog_table_schema(table.table_id)?;
    assert_eq!(new_reader_schema.columns.len(), 1);
    assert_eq!(new_reader_schema.column_ids, vec![table.column_ids[1]]);
    let after_drop_for_new_reader = new_reader
        .read(old_reader_row)?
        .ok_or("new reader lost the rewritten row after DROP COLUMN")?;
    assert_eq!(
        prdb_engine::tuple::decode_tuple(
            &new_reader_schema.columns,
            new_reader_schema.schema_version,
            &after_drop_for_new_reader,
        )?
        .values,
        vec![Some(ScalarValue::Text("preserved".to_owned()))]
    );
    let stale_row_after_drop = row(
        &database,
        &table_row_key(table.table_id, stale_drop_row.row),
    )?;
    match (write_status, stale_row_after_drop) {
        (OutcomeStatus::Committed, Some(record)) => {
            assert_eq!(
                prdb_engine::tuple::decode_tuple(
                    &new_reader_schema.columns,
                    new_reader_schema.schema_version,
                    &record.payload,
                )?
                .values,
                [Some(ScalarValue::Text("staged before drop".to_owned()))],
                "DROP COLUMN must rewrite a row committed immediately before it"
            );
        }
        (OutcomeStatus::Aborted, None) => {}
        (status, record) => panic!(
            "DROP COLUMN race produced inconsistent row state: status={status:?}, present={}",
            record.is_some()
        ),
    }
    assert_eq!(
        lookup_outcome(&database, stale_drop_invocation, actor, 1)?
            .ok_or("missing stale DROP COLUMN writer outcome")?
            .status,
        write_status
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .is_none()
    );
    let retired_column = catalog
        .get(id_catalog_key(17, table.column_ids[0]).as_slice())?
        .ok_or("DROP COLUMN must reserve its retired column ID")?;
    assert!(
        decode_registered_envelope(retired_column.value())?
            .fields
            .is_empty()
    );
    drop(retired_column);
    let surviving = decode_registered_envelope(
        catalog
            .get(id_catalog_key(6, table.column_ids[1]).as_slice())?
            .ok_or("surviving column missing")?
            .value(),
    )?;
    assert_eq!(
        u32::from_le_bytes(
            surviving
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("surviving ordinal missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        0
    );
    let table_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("schema version missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        2
    );
    let stored = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&row_key[..])?
            .ok_or("row missing")?
            .value(),
    )?;
    let new_schema = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let tuple = prdb_engine::tuple::decode_tuple(&new_schema, 2, &stored.payload)?;
    assert_eq!(
        tuple.values,
        vec![Some(ScalarValue::Text("preserved".to_owned()))]
    );
    let rebuilt_index_key = prdb_engine::index_key::derive_index_key(
        index_id,
        row_key[16..].try_into()?,
        2,
        &[table.column_ids[1]],
        &[ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        }],
        &stored.payload,
        &[table.column_ids[1]],
    )?;
    let indexes = read.open_table(schema::INDEXES)?;
    assert_eq!(old_index_key.key, rebuilt_index_key.key);
    let rebuilt_locator = indexes
        .get(rebuilt_index_key.key.as_slice())?
        .ok_or("index entry missing after schema rewrite")?;
    assert_eq!(
        IndexLocator::decode_for_key(&rebuilt_index_key.key, rebuilt_locator.value())?.row_version,
        stored.row_version,
    );
    assert_ne!(stored.row_version, old_index_row_version);
    let mut old_reverse_key = row_key.to_vec();
    old_reverse_key.extend_from_slice(&old_index_key.key[..old_index_key.key.len() - 16]);
    let mut rebuilt_reverse_key = row_key.to_vec();
    rebuilt_reverse_key
        .extend_from_slice(&rebuilt_index_key.key[..rebuilt_index_key.key.len() - 16]);
    let reverse_indexes = read.open_table(schema::ROW_INDEXES)?;
    assert_eq!(old_reverse_key, rebuilt_reverse_key);
    assert!(
        reverse_indexes
            .get(rebuilt_reverse_key.as_slice())?
            .is_some()
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(read);
    let stale_owner_writer_invocation = id(0xdb);
    let stale_owner_row = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0xda),
    };
    let mut stale_owner_writer =
        storage.begin_invocation(stale_owner_writer_invocation, actor, 9_999, 2, 64)?;
    let stale_owner_schema = stale_owner_writer.load_catalog_table_schema(table.table_id)?;
    stale_owner_writer.load_catalog_indexes(&stale_owner_schema)?;
    let stale_owner_tuple = prdb_engine::tuple::encode_tuple(
        &stale_owner_schema.columns,
        stale_owner_schema.schema_version,
        &[Some(ScalarValue::Text("stale".to_owned()))],
    )?;
    stale_owner_writer.stage_put(stale_owner_row, &stale_owner_tuple)?;
    let owner_snapshot = database.begin_read()?;
    let owner_snapshot_catalog = owner_snapshot.open_table(CATALOG)?;
    let previous_owner_object = decode_registered_envelope(
        owner_snapshot_catalog
            .get(id_catalog_key(2, table.table_id).as_slice())?
            .ok_or("table object missing from pinned owner snapshot")?
            .value(),
    )?;
    let previous_owner_version = u64::from_le_bytes(
        previous_owner_object
            .fields
            .iter()
            .find(|field| field.tag == 5)
            .ok_or("table owner object version missing")?
            .payload
            .as_slice()
            .try_into()?,
    );
    let previous_table_epoch = u64::from_le_bytes(
        decode_registered_envelope(
            owner_snapshot_catalog
                .get(id_catalog_key(5, table.table_id).as_slice())?
                .ok_or("table record missing from pinned owner snapshot")?
                .value(),
        )?
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .ok_or("table write epoch missing")?
        .payload
        .as_slice()
        .try_into()?,
    );
    assert_eq!(
        previous_owner_object
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .ok_or("table owner missing from pinned snapshot")?
            .payload,
        actor
    );
    let transfer_invocation = id(0xd1);
    let mut transfer_audit = identity_audit_event(0xd2, actor, table.table_id);
    transfer_audit.correlation_id = transfer_invocation;
    let transfer_result = storage.set_table_owner(
        transfer_invocation,
        actor,
        table.table_id,
        successor_role,
        9_999,
        transfer_audit,
    );
    assert!(
        transfer_result.is_ok(),
        "table owner transfer failed: {transfer_result:?}"
    );
    let transfer_sequence = transfer_result?;
    assert!(transfer_sequence > table.commit_sequence);
    let retained_owner_object = decode_registered_envelope(
        owner_snapshot_catalog
            .get(id_catalog_key(2, table.table_id).as_slice())?
            .ok_or("table object disappeared from pinned owner snapshot")?
            .value(),
    )?;
    assert_eq!(
        retained_owner_object
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .ok_or("pinned table owner field missing")?
            .payload,
        actor
    );
    assert_eq!(
        u64::from_le_bytes(
            retained_owner_object
                .fields
                .iter()
                .find(|field| field.tag == 5)
                .ok_or("pinned table owner version missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        previous_owner_version
    );
    let retained_table_record = decode_registered_envelope(
        owner_snapshot_catalog
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record disappeared from pinned owner snapshot")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            retained_table_record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("pinned table epoch missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        previous_table_epoch
    );
    assert!(matches!(
        stale_owner_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(
        row(
            &database,
            &table_row_key(table.table_id, stale_owner_row.row)
        )?
        .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, stale_owner_writer_invocation, actor, 1)?
            .ok_or("missing stale ownership-transfer writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let object = decode_registered_envelope(
            catalog
                .get(id_catalog_key(2, table.table_id).as_slice())?
                .ok_or("table object disappeared after ownership transfer")?
                .value(),
        )?;
        assert_eq!(
            object
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("table owner field missing")?
                .payload,
            successor_role
        );
        assert_eq!(
            u64::from_le_bytes(
                object
                    .fields
                    .iter()
                    .find(|field| field.tag == 5)
                    .ok_or("new table owner version missing")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            previous_owner_version + 1
        );
        let table_record = decode_registered_envelope(
            catalog
                .get(id_catalog_key(5, table.table_id).as_slice())?
                .ok_or("table record disappeared after owner transfer")?
                .value(),
        )?;
        assert_eq!(
            u64::from_le_bytes(
                table_record
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("new table epoch missing")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            previous_table_epoch + 1
        );
        let mut successor_grants_prefix = 12u16.to_be_bytes().to_vec();
        successor_grants_prefix.push(1);
        successor_grants_prefix.extend_from_slice(&successor_role);
        assert!(
            catalog
                .range(successor_grants_prefix.as_slice()..)?
                .next()
                .transpose()?
                .is_none_or(|(key, _)| !key.value().starts_with(&successor_grants_prefix))
        );
    }
    drop(owner_snapshot_catalog);
    drop(owner_snapshot);
    let outsider_transfer_invocation = id(0xd8);
    let mut outsider_transfer_audit = identity_audit_event(0xd9, outsider, table.table_id);
    outsider_transfer_audit.correlation_id = outsider_transfer_invocation;
    assert!(matches!(
        storage.set_table_owner(
            outsider_transfer_invocation,
            outsider,
            table.table_id,
            actor,
            9_999,
            outsider_transfer_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(&database, outsider_transfer_invocation, outsider, 1)?
            .ok_or("missing unauthorized ownership-transfer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let successor_rename_invocation = id(0xd3);
    let mut successor_rename_audit = identity_audit_event(0xd4, successor_owner, table.table_id);
    successor_rename_audit.correlation_id = successor_rename_invocation;
    storage.rename_table(
        successor_rename_invocation,
        successor_owner,
        table.table_id,
        "successor_owned_table",
        9_999,
        successor_rename_audit,
    )?;
    let membership_revoke_invocation = id(0xdc);
    let mut membership_revoke_audit = identity_audit_event(0xdd, revocation_admin, successor_owner);
    membership_revoke_audit.correlation_id = membership_revoke_invocation;
    membership_revoke_audit.action = AuditAction::AuthorityChange;
    storage
        .set_role_membership(
            membership_revoke_invocation,
            revocation_admin,
            prdb_engine::storage_manager::RoleMembershipChange {
                member_id: successor_owner,
                role_id: successor_role,
                grant: false,
            },
            9_999,
            membership_revoke_audit,
        )
        .map_err(|error| format!("membership revocation failed: {error:?}"))?;
    let revoked_owner_rename_invocation = id(0xde);
    let mut revoked_owner_rename_audit =
        identity_audit_event(0xdf, successor_owner, table.table_id);
    revoked_owner_rename_audit.correlation_id = revoked_owner_rename_invocation;
    assert!(matches!(
        storage.rename_table(
            revoked_owner_rename_invocation,
            successor_owner,
            table.table_id,
            "revoked_role_owner_table",
            9_999,
            revoked_owner_rename_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(
            &database,
            revoked_owner_rename_invocation,
            successor_owner,
            1
        )?
        .ok_or("missing role-owner revocation outcome")?
        .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let object = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(2, table.table_id).as_slice())?
                .ok_or("table object disappeared after revoked owner operation")?
                .value(),
        )?;
        assert_eq!(
            object
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table name missing after revoked owner operation")?
                .payload,
            b"successor_owned_table"
        );
    }
    let return_invocation = id(0xd5);
    let mut return_audit = identity_audit_event(0xd6, actor, table.table_id);
    return_audit.correlation_id = return_invocation;
    storage.set_table_owner(
        return_invocation,
        actor,
        table.table_id,
        actor,
        9_999,
        return_audit,
    )?;
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_sets_and_drops_not_null_with_existing_row_validation()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x31);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "nullability_owner"),
        object_write_with_name(actor, 8, "nullability_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x32, admin_role, id(0x33));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x33), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x34);
    let mut schema_audit = identity_audit_event(0x35, actor, id(0x36));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "nullability_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x37);
    let mut create_audit = identity_audit_event(0x38, actor, id(0x39));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "nullable_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let row_key = table_row_key(table.table_id, id(0x3a));
    let null_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        }],
        1,
        &[None],
    )?;
    let null_row = RowRecord {
        row_version: table.commit_sequence,
        payload: null_tuple,
    }
    .encode()?;
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&row_key[..], &null_row[..])?;
    write.commit()?;

    let denied_invocation = id(0x3b);
    let mut denied_audit = identity_audit_event(0x3c, actor, id(0x3d));
    denied_audit.correlation_id = denied_invocation;
    assert!(matches!(
        storage.set_column_nullable(
            denied_invocation,
            actor,
            table.table_id,
            table.column_ids[0],
            false,
            9_999,
            denied_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::NotNullViolation
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, denied_invocation, actor, 1)?
            .ok_or("missing NOT NULL rejection outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let column = decode_registered_envelope(
        read.open_table(CATALOG)?
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .ok_or("column missing after rejected NOT NULL")?
            .value(),
    )?;
    assert_eq!(
        column
            .fields
            .iter()
            .find(|field| field.tag == 6)
            .ok_or("nullable field missing")?
            .payload,
        [1]
    );
    drop(read);

    let write = database.begin_write()?;
    write.open_table(ROWS)?.remove(&row_key[..])?;
    write.commit()?;
    let stale_writer_invocation = id(0x44);
    let stale_row_key = table_row_key(table.table_id, id(0x45));
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Text("staged before NOT NULL".to_owned()))],
    )?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: id(0x45),
        },
        &stale_tuple,
    )?;
    let set_invocation = id(0x3e);
    let mut set_audit = identity_audit_event(0x3f, actor, id(0x40));
    set_audit.correlation_id = set_invocation;
    storage.set_column_nullable(
        set_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        false,
        9_999,
        set_audit,
    )?;
    let stale_result = stale_writer.commit();
    assert!(matches!(
        stale_result,
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &stale_row_key)?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale writer outcome after SET NOT NULL")?
            .status,
        OutcomeStatus::Aborted
    );
    let not_null_constraint_key = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let prefix = 8u16.to_be_bytes();
        let mut keys = Vec::new();
        for entry in catalog.range(prefix.as_slice()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            let key = key.value().to_vec();
            if key.len() != 19 {
                continue;
            }
            let record = decode_registered_envelope(value.value())?;
            if record
                .fields
                .iter()
                .any(|field| field.tag == 1 && field.payload.as_slice() == table.table_id)
                && record
                    .fields
                    .iter()
                    .any(|field| field.tag == 2 && field.payload.as_slice() == 1u16.to_le_bytes())
            {
                keys.push(key);
            }
        }
        assert_eq!(keys.len(), 1, "expected one NOT NULL constraint");
        keys.pop()
            .ok_or("NOT NULL constraint missing after SET NOT NULL")?
    };
    let drop_invocation = id(0x41);
    let mut drop_audit = identity_audit_event(0x42, actor, id(0x43));
    drop_audit.correlation_id = drop_invocation;
    storage.set_column_nullable(
        drop_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        true,
        9_999,
        drop_audit,
    )?;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let column = decode_registered_envelope(
        catalog
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .ok_or("column missing after DROP NOT NULL")?
            .value(),
    )?;
    assert_eq!(
        column
            .fields
            .iter()
            .find(|field| field.tag == 6)
            .ok_or("nullable field missing")?
            .payload,
        [1]
    );
    let table_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("schema version missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        1
    );
    assert_eq!(
        u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("write epoch missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        3
    );
    assert!(catalog.get(not_null_constraint_key.as_slice())?.is_none());
    let constraint_id: [u8; 16] = not_null_constraint_key[3..19].try_into()?;
    assert!(
        catalog
            .get(id_catalog_key(2, constraint_id).as_slice())?
            .is_none()
    );
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn set_not_null_racing_a_staged_null_insert_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let actor = id(0x51);
    let admin_role = builtin_owner_id();
    let setup = [
        principal_write_with_name(actor, 1, 1, "set_not_null_race_owner"),
        object_write_with_name(actor, 8, "set_not_null_race_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x52, admin_role, id(0x53));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x53), admin_role, 9_999, &setup, &[setup_audit])?;

    let schema_invocation = id(0x54);
    let mut schema_audit = identity_audit_event(0x55, actor, id(0x56));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "set_not_null_race_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let table_invocation = id(0x57);
    let mut table_audit = identity_audit_event(0x58, actor, id(0x59));
    table_audit.correlation_id = table_invocation;
    let table = storage
        .create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: actor,
            schema_id: schema.schema_id,
            name: "nullable_table".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "value".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: 9_999,
            audit_event: table_audit,
        })
        .map_err(|error| format!("create grant-batch table: {error:?}"))?;
    let baseline_sequence = commit_sequence(&database)?;

    let writer_invocation = id(0x5a);
    let mut writer = storage.begin_invocation(writer_invocation, actor, 9_999, 1, 1024)?;
    let writer_schema = writer.load_catalog_table_schema(table.table_id)?;
    writer.load_catalog_indexes(&writer_schema)?;
    writer.load_catalog_check_constraints(&writer_schema)?;
    let null_payload = encode_tuple(
        &writer_schema.columns,
        writer_schema.schema_version,
        &[None],
    )?;
    let row_id = id(0x5b);
    writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: row_id,
        },
        &null_payload,
    )?;

    let ddl_invocation = id(0x5c);
    let table_id = table.table_id;
    let column_id = table.column_ids[0];
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, ddl_result) = std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer_task = scope.spawn(move || {
            writer_barrier.wait();
            writer.commit()
        });
        let ddl_barrier = barrier.clone();
        let ddl_storage = &storage;
        let ddl_task = scope.spawn(move || {
            let mut audit = identity_audit_event(0x5d, actor, column_id);
            audit.correlation_id = ddl_invocation;
            ddl_barrier.wait();
            ddl_storage.set_column_nullable(
                ddl_invocation,
                actor,
                table_id,
                column_id,
                false,
                9_999,
                audit,
            )
        });
        barrier.wait();
        (writer_task.join().unwrap(), ddl_task.join().unwrap())
    });

    let writer_status = match writer_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        other => panic!("unexpected NULL writer result: {other:?}"),
    };
    let ddl_status = match ddl_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::NotNullViolation,
        )) => OutcomeStatus::Aborted,
        other => panic!("unexpected SET NOT NULL result: {other:?}"),
    };
    assert_ne!(writer_status, ddl_status);
    assert_eq!(
        lookup_outcome(&database, writer_invocation, actor, 1)?
            .ok_or("missing staged NULL writer outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, ddl_invocation, actor, 1)?
            .ok_or("missing SET NOT NULL outcome")?
            .status,
        ddl_status
    );
    assert_eq!(commit_sequence(&database)?, baseline_sequence + 1);
    assert_eq!(
        row(&database, &table_row_key(table.table_id, row_id))?.is_some(),
        writer_status == OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let column = catalog
        .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
        .ok_or("column missing after SET NOT NULL race")?;
    let record = decode_registered_envelope(column.value())?;
    let nullable = record
        .fields
        .iter()
        .find(|field| field.tag == 6)
        .ok_or("column nullability missing after race")?;
    assert_eq!(
        nullable.payload,
        [u8::from(ddl_status == OutcomeStatus::Aborted)]
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}
