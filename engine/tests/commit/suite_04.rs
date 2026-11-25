use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::{lookup_index_id_by_name, lookup_relation_id_by_name};
use prdb_engine::commit::CommitError;
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ROWS};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::{Database, ReadableDatabase};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn drop_not_null_racing_a_staged_writer_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let actor = id(0x61);
    let admin_role = builtin_owner_id();
    let setup = [
        principal_write_with_name(actor, 1, 1, "drop_not_null_race_owner"),
        object_write_with_name(actor, 8, "drop_not_null_race_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x62, admin_role, id(0x63));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x63), admin_role, 9_999, &setup, &[setup_audit])?;

    let schema_invocation = id(0x64);
    let mut schema_audit = identity_audit_event(0x65, actor, id(0x66));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "drop_not_null_race_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let table_invocation = id(0x67);
    let mut table_audit = identity_audit_event(0x68, actor, id(0x69));
    table_audit.correlation_id = table_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "required_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: table_audit,
    })?;
    let baseline_sequence = commit_sequence(&database)?;

    let writer_invocation = id(0x6a);
    let mut writer = storage.begin_invocation(writer_invocation, actor, 9_999, 1, 1024)?;
    let writer_schema = writer.load_catalog_table_schema(table.table_id)?;
    writer.load_catalog_indexes(&writer_schema)?;
    writer.load_catalog_check_constraints(&writer_schema)?;
    let payload = encode_tuple(
        &writer_schema.columns,
        writer_schema.schema_version,
        &[Some(ScalarValue::Integer(42))],
    )?;
    let row_id = id(0x6b);
    writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: row_id,
        },
        &payload,
    )?;

    let ddl_invocation = id(0x6c);
    let table_id = table.table_id;
    let column_id = table.column_ids[0];
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, ddl_result) = std::thread::scope(|scope| {
        let writer_barrier = Arc::clone(&barrier);
        let writer_task = scope.spawn(move || {
            writer_barrier.wait();
            writer.commit()
        });
        let ddl_barrier = Arc::clone(&barrier);
        let ddl_storage = &storage;
        let ddl_task = scope.spawn(move || {
            let mut audit = identity_audit_event(0x6d, actor, column_id);
            audit.correlation_id = ddl_invocation;
            ddl_barrier.wait();
            ddl_storage.set_column_nullable(
                ddl_invocation,
                actor,
                table_id,
                column_id,
                true,
                9_999,
                audit,
            )
        });
        barrier.wait();
        (writer_task.join().unwrap(), ddl_task.join().unwrap())
    });

    let (writer_sequence, writer_status) = match writer_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(TransactionError::Commit(CommitError::Conflict)) => (None, OutcomeStatus::Aborted),
        other => panic!("unexpected staged writer result during DROP NOT NULL: {other:?}"),
    };
    let ddl_sequence = ddl_result?;
    if let Some(sequence) = writer_sequence {
        assert!(sequence < ddl_sequence);
    }
    assert_eq!(
        commit_sequence(&database)?,
        baseline_sequence + 1 + u64::from(writer_sequence.is_some())
    );
    assert_eq!(
        lookup_outcome(&database, writer_invocation, actor, 1)?
            .ok_or("missing staged writer outcome during DROP NOT NULL")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, ddl_invocation, actor, 1)?
            .ok_or("missing DROP NOT NULL outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        row(&database, &table_row_key(table.table_id, row_id))?.map(|record| record.payload),
        (writer_status == OutcomeStatus::Committed).then_some(payload)
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let column = decode_registered_envelope(
        catalog
            .get(id_catalog_key(6, column_id).as_slice())?
            .ok_or("column missing after DROP NOT NULL race")?
            .value(),
    )?;
    assert_eq!(
        column
            .fields
            .iter()
            .find(|field| field.tag == 6)
            .ok_or("column nullability missing after DROP NOT NULL race")?
            .payload,
        [1]
    );
    let table_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table missing after DROP NOT NULL race")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("schema version missing after DROP NOT NULL race")?
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
                .ok_or("write epoch missing after DROP NOT NULL race")?
                .payload
                .as_slice()
                .try_into()?
        ),
        2
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_widens_varchar_atomically_and_keeps_index_current()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateIndexRequest, CreateTableRequest, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x71);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "varchar_widen_owner"),
        object_write_with_name(actor, 8, "varchar_widen_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x72, admin_role, id(0x73));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x73), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x74);
    let mut schema_audit = identity_audit_event(0x75, actor, id(0x76));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "varchar_widen_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x77);
    let mut create_audit = identity_audit_event(0x78, actor, id(0x79));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "widened_table".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "label".to_owned(),
                scalar_type: ScalarType::Varchar { max_chars: 8 },
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "sequence".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let row_id = id(0x83);
    let original_columns = [
        ColumnSchema {
            scalar_type: ScalarType::Varchar { max_chars: 8 },
            nullable: true,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
    ];
    let old_tuple = encode_tuple(
        &original_columns,
        1,
        &[
            Some(ScalarValue::Varchar("persist".to_owned())),
            Some(ScalarValue::Integer(7)),
        ],
    )?;
    let row_record_bytes = RowRecord {
        row_version: table.commit_sequence,
        payload: old_tuple.clone(),
    }
    .encode()?;
    let write = database.begin_write()?;
    write.open_table(ROWS)?.insert(
        &table_row_key(table.table_id, row_id)[..],
        &row_record_bytes[..],
    )?;
    write.commit()?;
    let index_invocation = id(0x7a);
    let mut index_audit = identity_audit_event(0x7b, actor, id(0x7c));
    index_audit.correlation_id = index_invocation;
    let index = storage.create_index(CreateIndexRequest {
        invocation_id: index_invocation,
        authenticated_actor_id: actor,
        table_id: table.table_id,
        name: "widened_label_idx".to_owned(),
        column_ids: table.column_ids.clone(),
        unique: false,
        retention_deadline_us: 9_999,
        audit_event: index_audit,
    })?;
    let original_index_key = prdb_engine::index_key::derive_index_key(
        index.index_id,
        row_id,
        1,
        &table.column_ids,
        &original_columns,
        &old_tuple,
        &table.column_ids,
    )?;
    let indexed_snapshot = database.begin_read()?;
    assert!(
        indexed_snapshot
            .open_table(prdb_engine::schema::INDEXES)?
            .get(original_index_key.key.as_slice())?
            .is_some()
    );
    drop(indexed_snapshot);
    let stale_writer_invocation = id(0x84);
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[
            Some(ScalarValue::Varchar("stale".to_owned())),
            Some(ScalarValue::Integer(8)),
        ],
    )?;
    let stale_row_id = id(0x85);
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: stale_row_id,
        },
        &stale_tuple,
    )?;
    let old_snapshot = database.begin_read()?;
    let widen_invocation = id(0x7d);
    let mut widen_audit = identity_audit_event(0x7e, actor, id(0x7f));
    widen_audit.correlation_id = widen_invocation;
    let widen_table_id = table.table_id;
    let widen_column_id = table.column_ids[0];
    let widen_barrier = Arc::new(Barrier::new(3));
    let storage_ref = &storage;
    let (stale_write_result, widen_result) = std::thread::scope(|scope| {
        let writer_barrier = Arc::clone(&widen_barrier);
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            stale_writer.commit()
        });
        let ddl_barrier = Arc::clone(&widen_barrier);
        let ddl = scope.spawn(move || {
            ddl_barrier.wait();
            storage_ref.widen_varchar_column(
                widen_invocation,
                actor,
                widen_table_id,
                widen_column_id,
                64,
                9_999,
                widen_audit,
            )
        });
        widen_barrier.wait();
        (
            writer.join().expect("VARCHAR widening writer panicked"),
            ddl.join().expect("VARCHAR widening DDL panicked"),
        )
    });
    let widen_sequence =
        widen_result.map_err(|error| format!("VARCHAR widening rejected: {error:?}"))?;
    let stale_write_sequence = match stale_write_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    if let Some(sequence) = stale_write_sequence {
        assert!(sequence < widen_sequence);
    }
    assert_eq!(
        row(&database, &table_row_key(table.table_id, stale_row_id))?.is_some(),
        stale_write_sequence.is_some()
    );
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing staged VARCHAR-widen writer outcome")?
            .status,
        if stale_write_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        lookup_outcome(&database, widen_invocation, actor, 1)?
            .ok_or("missing VARCHAR-widen DDL outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(commit_sequence(&database)?, widen_sequence);

    let old_catalog = old_snapshot.open_table(CATALOG)?;
    let old_column = decode_registered_envelope(
        old_catalog
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .ok_or("old snapshot lost the column")?
            .value(),
    )?;
    assert_eq!(
        old_column
            .fields
            .iter()
            .find(|field| field.tag == 5)
            .ok_or("old VARCHAR parameters missing")?
            .payload,
        [0, 0, 0, 0, 8, 0, 0, 0]
    );
    drop(old_catalog);
    drop(old_snapshot);

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let table_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table disappeared during VARCHAR widening")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("table schema version missing")?
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
                .ok_or("table write epoch missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        3
    );
    let column = decode_registered_envelope(
        catalog
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .ok_or("widened column missing")?
            .value(),
    )?;
    assert_eq!(
        column
            .fields
            .iter()
            .find(|field| field.tag == 5)
            .ok_or("widened VARCHAR parameters missing")?
            .payload,
        [0, 0, 0, 0, 64, 0, 0, 0]
    );
    let index_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(9, index.index_id).as_slice())?
            .ok_or("index disappeared during VARCHAR widening")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            index_record
                .fields
                .iter()
                .find(|field| field.tag == 10)
                .ok_or("index schema version missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        1
    );
    let widened_row = decode_registered_envelope(
        catalog
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .ok_or("widened column missing")?
            .value(),
    )?;
    let current_parameters = widened_row
        .fields
        .iter()
        .find(|field| field.tag == 5)
        .ok_or("current VARCHAR parameters missing")?
        .payload
        .clone();
    assert_eq!(current_parameters, [0, 0, 0, 0, 64, 0, 0, 0]);
    drop(catalog);
    let rows = read.open_table(ROWS)?;
    let stored_row = RowRecord::decode(
        rows.get(table_row_key(table.table_id, row_id).as_slice())?
            .ok_or("stored row disappeared during metadata-only widening")?
            .value(),
    )?;
    assert_eq!(stored_row.payload, old_tuple);
    assert_eq!(
        decode_tuple(
            &[
                ColumnSchema {
                    scalar_type: ScalarType::Varchar { max_chars: 64 },
                    nullable: true,
                },
                ColumnSchema {
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                },
            ],
            1,
            &stored_row.payload,
        )?
        .values,
        [
            Some(ScalarValue::Varchar("persist".to_owned())),
            Some(ScalarValue::Integer(7)),
        ]
    );
    let widened_columns = [
        ColumnSchema {
            scalar_type: ScalarType::Varchar { max_chars: 64 },
            nullable: true,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
    ];
    let widened_index_key = prdb_engine::index_key::derive_index_key(
        index.index_id,
        row_id,
        1,
        &table.column_ids,
        &widened_columns,
        &stored_row.payload,
        &table.column_ids,
    )?;
    assert_eq!(widened_index_key.key, original_index_key.key);
    assert!(
        read.open_table(prdb_engine::schema::INDEXES)?
            .get(widened_index_key.key.as_slice())?
            .is_some()
    );
    drop(rows);
    drop(read);

    let stale_followup_invocation = id(0x8c);
    let mut stale_followup =
        storage.begin_invocation(stale_followup_invocation, actor, 9_999, 1, 1024)?;
    let followup_schema = stale_followup.load_catalog_table_schema(table.table_id)?;
    stale_followup.load_catalog_indexes(&followup_schema)?;
    let followup_tuple = encode_tuple(
        &followup_schema.columns,
        followup_schema.schema_version,
        &[
            Some(ScalarValue::Varchar(
                "pinned before second widen".to_owned(),
            )),
            Some(ScalarValue::Integer(9)),
        ],
    )?;
    let followup_row_id = id(0x8d);
    stale_followup.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: followup_row_id,
        },
        &followup_tuple,
    )?;
    let before_second_widen = commit_sequence(&database)?;
    let second_widen_invocation = id(0x8e);
    let mut second_widen_audit = identity_audit_event(0x8f, actor, table.table_id);
    second_widen_audit.correlation_id = second_widen_invocation;
    let second_widen_sequence = storage.widen_varchar_column(
        second_widen_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        128,
        9_999,
        second_widen_audit,
    )?;
    assert_eq!(second_widen_sequence, before_second_widen + 1);
    assert!(matches!(
        stale_followup.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert_eq!(commit_sequence(&database)?, second_widen_sequence);
    assert!(row(&database, &table_row_key(table.table_id, followup_row_id))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_followup_invocation, actor, 1)?
            .ok_or("missing stale writer outcome after VARCHAR widening")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, second_widen_invocation, actor, 1)?
            .ok_or("missing second VARCHAR widening outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let before_rejected = commit_sequence(&database)?;
    let reject_invocation = id(0x80);
    let mut reject_audit = identity_audit_event(0x81, actor, id(0x82));
    reject_audit.correlation_id = reject_invocation;
    assert!(matches!(
        storage.widen_varchar_column(
            reject_invocation,
            actor,
            table.table_id,
            table.column_ids[0],
            64,
            9_999,
            reject_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::InvalidRequest)
    ));
    assert_eq!(commit_sequence(&database)?, before_rejected);

    let denied_actor = id(0x86);
    let denied_setup = vec![
        principal_write_with_name(denied_actor, 1, 1, "varchar_widen_denied"),
        object_write_with_name(denied_actor, 8, "varchar_widen_denied"),
    ];
    let mut denied_setup_audit = identity_audit_event(0x87, actor, id(0x88));
    denied_setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x88),
        actor,
        9_999,
        &denied_setup,
        &[denied_setup_audit],
    )?;
    let denied_invocation = id(0x89);
    let mut denied_audit = identity_audit_event(0x8a, denied_actor, id(0x8b));
    denied_audit.correlation_id = denied_invocation;
    assert!(matches!(
        storage.widen_varchar_column(
            denied_invocation,
            denied_actor,
            table.table_id,
            table.column_ids[0],
            256,
            9_999,
            denied_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(&database, denied_invocation, denied_actor, 1)?
            .ok_or("missing denied VARCHAR widening outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn storage_manager_truncates_rows_and_index_memberships_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateIndexRequest, CreateTableRequest, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x61);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "truncate_table_owner"),
        object_write_with_name(actor, 8, "truncate_table_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x62, admin_role, id(0x63));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x63), admin_role, 9_999, &setup, &[setup_audit])?;

    let create_schema_invocation = id(0x64);
    let mut create_schema_audit = identity_audit_event(0x65, actor, id(0x66));
    create_schema_audit.correlation_id = create_schema_invocation;
    let schema = storage.create_schema(
        create_schema_invocation,
        actor,
        "truncate_schema",
        actor,
        9_999,
        create_schema_audit,
    )?;
    let create_table_invocation = id(0x67);
    let mut create_table_audit = identity_audit_event(0x68, actor, id(0x69));
    create_table_audit.correlation_id = create_table_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_table_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "truncate_target".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_table_audit,
    })?;
    let schema_id = schema.schema_id;
    let schema = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: true,
    }];
    let row_ids = [id(0x6a), id(0x6b)];
    let write = database.begin_write()?;
    {
        let mut rows = write.open_table(ROWS)?;
        for (row_id, value) in row_ids.iter().zip([11i32, 22]) {
            let payload = encode_tuple(&schema, 1, &[Some(ScalarValue::Integer(value))])?;
            let bytes = RowRecord {
                row_version: table.commit_sequence,
                payload,
            }
            .encode()?;
            rows.insert(&table_row_key(table.table_id, *row_id)[..], &bytes[..])?;
        }
    }
    write.commit()?;

    let create_index_invocation = id(0x6c);
    let mut create_index_audit = identity_audit_event(0x6d, actor, id(0x6e));
    create_index_audit.correlation_id = create_index_invocation;
    let index = storage.create_index(CreateIndexRequest {
        invocation_id: create_index_invocation,
        authenticated_actor_id: actor,
        table_id: table.table_id,
        name: "truncate_value_idx".to_owned(),
        column_ids: table.column_ids.clone(),
        unique: false,
        retention_deadline_us: 9_999,
        audit_event: create_index_audit,
    })?;

    let stale_invocation = id(0x6f);
    let mut stale = storage.begin_invocation(stale_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale.load_catalog_table_schema(table.table_id)?;
    stale.load_catalog_indexes(&stale_schema)?;
    let stale_payload = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Integer(33))],
    )?;
    let stale_row_id = id(0x70);
    let stale_row = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: stale_row_id,
    };
    stale.stage_put(stale_row, &stale_payload)?;
    let stale_index_key = prdb_engine::index_key::derive_index_key(
        index.index_id,
        stale_row_id,
        stale_schema.schema_version,
        &stale_schema.column_ids,
        &stale_schema.columns,
        &stale_payload,
        &table.column_ids,
    )?;
    stale.stage_index_membership(stale_row, stale_index_key.key, true)?;
    let denied_invocation = id(0x74);
    let mut denied_audit = identity_audit_event(0x75, id(0x76), id(0x77));
    denied_audit.correlation_id = denied_invocation;
    assert_eq!(
        storage.truncate_table(
            denied_invocation,
            id(0x76),
            table.table_id,
            9_999,
            denied_audit
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Unauthorized)
    );
    assert_eq!(
        lookup_outcome(&database, denied_invocation, id(0x76), 1)?
            .ok_or("unauthorized truncate outcome missing")?
            .status,
        OutcomeStatus::Aborted
    );
    let old_snapshot = database.begin_read()?;
    let truncate_invocation = id(0x71);
    let truncate_table_id = table.table_id;
    let mut truncate_audit = identity_audit_event(0x72, actor, id(0x73));
    truncate_audit.correlation_id = truncate_invocation;
    let truncate_race_barrier = Arc::new(Barrier::new(2));
    let (stale_write_result, truncate_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&truncate_race_barrier);
        let storage = &storage;
        let truncate = scope.spawn(move || {
            barrier.wait();
            storage.truncate_table(
                truncate_invocation,
                actor,
                truncate_table_id,
                9_999,
                truncate_audit,
            )
        });
        truncate_race_barrier.wait();
        let write = stale.commit();
        let truncate = truncate.join().expect("TRUNCATE thread panicked");
        (write, truncate)
    });
    let truncated = truncate_result?;
    let (stale_write_sequence, stale_write_status) = match stale_write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            (None, OutcomeStatus::Aborted)
        }
        Err(error) => panic!("unexpected staged writer result during TRUNCATE: {error:?}"),
    };
    if let Some(sequence) = stale_write_sequence {
        assert!(sequence < truncated.commit_sequence);
    }
    assert_eq!(
        truncated.deleted_rows,
        2 + usize::from(stale_write_sequence.is_some())
    );
    assert_eq!(truncated.commit_sequence, commit_sequence(&database)?);

    let old_rows = old_snapshot.open_table(ROWS)?;
    let old_indexes = old_snapshot.open_table(schema::INDEXES)?;
    let old_reverse = old_snapshot.open_table(schema::ROW_INDEXES)?;
    for row_id in row_ids {
        let row_key = table_row_key(table.table_id, row_id);
        let row = old_rows
            .get(&row_key[..])?
            .ok_or("old snapshot lost a row")?;
        let record = RowRecord::decode(row.value())?;
        assert!(
            old_indexes
                .range(index.index_id.as_slice()..)?
                .any(|entry| entry
                    .map(|(key, _)| key.value().ends_with(&row_id))
                    .unwrap_or(false))
        );
        assert!(
            old_reverse
                .range(row_key.as_slice()..)?
                .next()
                .transpose()?
                .is_some()
        );
        assert_eq!(record.row_version, table.commit_sequence);
    }
    drop(old_reverse);
    drop(old_indexes);
    drop(old_rows);
    drop(old_snapshot);

    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    let indexes = read.open_table(schema::INDEXES)?;
    let reverse = read.open_table(schema::ROW_INDEXES)?;
    assert!(
        rows.get(&table_row_key(table.table_id, row_ids[0])[..])?
            .is_none()
    );
    assert!(
        indexes
            .range(index.index_id.as_slice()..)?
            .next()
            .transpose()?
            .is_none()
    );
    assert!(
        reverse
            .range(table.table_id.as_slice()..)?
            .next()
            .transpose()?
            .is_none()
    );
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(9, index.index_id).as_slice())?
            .is_some()
    );
    drop(catalog);
    drop(reverse);
    drop(indexes);
    drop(rows);
    drop(read);
    assert!(row(&database, &table_row_key(table.table_id, stale_row_id))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_invocation, actor, 1)?
            .ok_or("TRUNCATE staged-writer outcome missing")?
            .status,
        stale_write_status
    );
    assert_eq!(
        lookup_outcome(&database, truncate_invocation, actor, 1)?
            .ok_or("truncate outcome missing")?
            .status,
        OutcomeStatus::Committed
    );
    prdb_engine::startup::validate_persisted_records(&database)?;

    drop(storage);
    drop(database);
    let reopened = Database::open(fixture.path())?;
    prdb_engine::startup::validate_persisted_records(&reopened)?;
    assert_eq!(commit_sequence(&reopened)?, truncated.commit_sequence);
    let reopened_snapshot = reopened.begin_read()?;
    assert_eq!(
        lookup_index_id_by_name(&reopened_snapshot, schema_id, "truncate_value_idx")?,
        Some(index.index_id),
        "TRUNCATE preserves its index definition across database reopen"
    );
    let reopened_rows = reopened_snapshot.open_table(ROWS)?;
    assert!(
        reopened_rows
            .get(&table_row_key(table.table_id, row_ids[0])[..])?
            .is_none()
    );
    let reopened_indexes = reopened_snapshot.open_table(schema::INDEXES)?;
    assert!(
        reopened_indexes
            .range(index.index_id.as_slice()..)?
            .next()
            .transpose()?
            .is_none()
    );
    let reopened_reverse = reopened_snapshot.open_table(schema::ROW_INDEXES)?;
    assert!(
        reopened_reverse
            .range(table.table_id.as_slice()..)?
            .next()
            .transpose()?
            .is_none()
    );
    drop(reopened_reverse);
    drop(reopened_indexes);
    drop(reopened_rows);
    drop(reopened_snapshot);
    assert_eq!(
        lookup_outcome(&reopened, truncate_invocation, actor, 1)?
            .ok_or("reopened TRUNCATE outcome missing")?
            .status,
        OutcomeStatus::Committed
    );
    Ok(())
}

pub(super) fn storage_manager_truncate_restricts_real_incoming_foreign_key()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableForeignKeyDefinition, CreateTableRequest, TableColumnDefinition,
        TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x91);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "truncate_fk_owner"),
        object_write_with_name(actor, 8, "truncate_fk_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x92, admin_role, id(0x93));
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(id(0x93), admin_role, 9_999, &setup, &[setup_audit])
        .map_err(|error| format!("truncate FK setup: {error:?}"))?;

    let schema_invocation = id(0x94);
    let mut schema_audit = identity_audit_event(0x95, actor, id(0x96));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage
        .create_schema(
            schema_invocation,
            actor,
            "truncate_fk_schema",
            actor,
            9_999,
            schema_audit,
        )
        .map_err(|error| format!("truncate FK schema creation: {error:?}"))?;
    let parent_invocation = id(0x97);
    let mut parent_audit = identity_audit_event(0x98, actor, id(0x99));
    parent_audit.correlation_id = parent_invocation;
    let parent = storage
        .create_table_with_primary_key(
            CreateTableRequest {
                invocation_id: parent_invocation,
                authenticated_actor_id: actor,
                schema_id: schema.schema_id,
                name: "truncate_parent".to_owned(),
                columns: vec![TableColumnDefinition {
                    name: "id".to_owned(),
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: None,
                }],
                retention_deadline_us: 9_999,
                audit_event: parent_audit,
            },
            &["id".to_owned()],
        )
        .map_err(|error| format!("truncate FK parent creation: {error:?}"))?;
    let target_constraint_id = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let prefix = 8u16.to_be_bytes();
        catalog
            .range(prefix.as_slice()..)?
            .find_map(|entry| {
                let (key, value) = entry.ok()?;
                let key = key.value();
                if !key.starts_with(&prefix) {
                    return None;
                }
                let record = decode_registered_envelope(value.value()).ok()?;
                (record.family_code == 8
                    && record
                        .fields
                        .iter()
                        .any(|field| field.tag == 1 && field.payload.as_slice() == parent.table_id)
                    && record.fields.iter().any(|field| {
                        field.tag == 2 && field.payload.as_slice() == 2u16.to_le_bytes()
                    }))
                .then(|| <[u8; 16]>::try_from(&key[3..19]).ok())?
            })
            .ok_or("created PRIMARY KEY constraint missing")?
    };

    let grant_invocation = id(0x9a);
    let mut actor_epoch = principal_write_with_name(actor, 1, 2, "truncate_fk_owner");
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        actor_epoch.expected_value = Some(
            catalog
                .get(id_catalog_key(10, actor).as_slice())?
                .ok_or("truncate FK actor missing")?
                .value()
                .to_vec(),
        );
    }
    let grant_writes = [
        grant_write(actor, parent.table_id, 6, 1, parent.table_id, admin_role),
        dependency_write(actor, parent.table_id, 5),
        actor_epoch,
    ];
    let mut grant_audit = identity_audit_event(0x9b, admin_role, id(0x9c));
    grant_audit.action = AuditAction::AuthorityChange;
    grant_audit.correlation_id = grant_invocation;
    storage
        .commit_catalog_and_audit(
            grant_invocation,
            admin_role,
            9_999,
            &grant_writes,
            &[grant_audit],
        )
        .map_err(|error| format!("truncate FK privilege setup: {error:?}"))?;

    let child_invocation = id(0x9d);
    let mut child_audit = identity_audit_event(0x9e, actor, id(0x9f));
    child_audit.correlation_id = child_invocation;
    let child = storage
        .create_table_with_foreign_keys(
            CreateTableRequest {
                invocation_id: child_invocation,
                authenticated_actor_id: actor,
                schema_id: schema.schema_id,
                name: "truncate_child".to_owned(),
                columns: vec![TableColumnDefinition {
                    name: "parent_id".to_owned(),
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                }],
                retention_deadline_us: 9_999,
                audit_event: child_audit,
            },
            None,
            &[],
            &[],
            &[CreateTableForeignKeyDefinition {
                source_columns: vec!["parent_id".to_owned()],
                target_constraint_id,
                target_columns: None,
            }],
        )
        .map_err(|error| format!("truncate FK child creation: {error:?}"))?;
    let _ = child;

    let before_truncate = commit_sequence(&database)?;
    let denied_actor = id(0xa3);
    let denied_invocation = id(0xa4);
    let mut denied_audit = identity_audit_event(0xa5, denied_actor, id(0xa6));
    denied_audit.correlation_id = denied_invocation;
    assert_eq!(
        storage.truncate_table(
            denied_invocation,
            denied_actor,
            parent.table_id,
            9_999,
            denied_audit,
        ),
        Err(TableLifecycleError::Unauthorized),
        "an unauthorized caller cannot distinguish an incoming FK dependency"
    );
    assert_eq!(commit_sequence(&database)?, before_truncate);
    assert_eq!(
        lookup_outcome(&database, denied_invocation, denied_actor, 1)?
            .ok_or("missing unauthorized truncate outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let truncate_invocation = id(0xa0);
    let mut truncate_audit = identity_audit_event(0xa1, actor, id(0xa2));
    truncate_audit.correlation_id = truncate_invocation;
    assert_eq!(
        storage.truncate_table(
            truncate_invocation,
            actor,
            parent.table_id,
            9_999,
            truncate_audit,
        ),
        Err(TableLifecycleError::Commit(
            CommitError::TableHasDependencies
        )),
        "TRUNCATE RESTRICT rejects an incoming FK even when both tables are empty"
    );
    assert_eq!(commit_sequence(&database)?, before_truncate);
    assert_eq!(
        lookup_outcome(&database, truncate_invocation, actor, 1)?
            .ok_or("missing rejected truncate outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_sets_replaces_and_drops_column_defaults()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{
        AccessTarget, AuthorizationContext, Effect, Expr, Grant, Operation, Policy, Subject,
    };
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefault, TableColumnDefinition,
    };
    use prdb_engine::transaction::{InsertInput, InvocationTransaction};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x46);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "column_default_owner"),
        object_write_with_name(actor, 8, "column_default_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x47, admin_role, id(0x48));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x48), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x49);
    let mut schema_audit = identity_audit_event(0x4a, actor, id(0x4b));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_default_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x4c);
    let mut create_audit = identity_audit_event(0x4d, actor, id(0x4e));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "default_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let stale_writer_invocation = id(0x5b);
    let stale_row_id = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x5c),
    };
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Integer(7))],
    )?;
    stale_writer.stage_put(stale_row_id, &stale_tuple)?;
    let set_invocation = id(0x4f);
    let default_table_id = table.table_id;
    let default_column_id = table.column_ids[0];
    let mut set_audit = identity_audit_event(0x50, actor, id(0x51));
    set_audit.correlation_id = set_invocation;
    let race_barrier = Arc::new(Barrier::new(2));
    let (write_result, set_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&race_barrier);
        let storage = &storage;
        let ddl = scope.spawn(move || {
            barrier.wait();
            storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
                invocation_id: set_invocation,
                authenticated_actor_id: actor,
                table_id: default_table_id,
                column_id: default_column_id,
                default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(42)))),
                retention_deadline_us: 9_999,
                audit_event: set_audit,
            })
        });
        race_barrier.wait();
        let write = stale_writer.commit();
        let ddl = ddl.join().expect("default ALTER thread panicked");
        (write, ddl)
    });
    assert!(
        set_result.is_ok(),
        "setting a typed default failed: {set_result:?}"
    );
    let set_sequence = set_result?;
    let (write_sequence, write_status) = match write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            (None, OutcomeStatus::Aborted)
        }
        Err(error) => panic!("unexpected staged-write result in default ALTER race: {error:?}"),
    };
    if let Some(sequence) = write_sequence {
        assert!(sequence < set_sequence);
    }
    let expected_row = (write_status == OutcomeStatus::Committed).then(|| stale_tuple.clone());
    assert_eq!(
        row(
            &database,
            &table_row_key(default_table_id, stale_row_id.row)
        )?
        .map(|stored| stored.payload),
        expected_row
    );
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale writer outcome after default change")?
            .status,
        write_status
    );

    let default_insert_invocation = id(0x5f);
    let default_row = prdb_engine::overlay::RowId {
        table: default_table_id,
        row: id(0x60),
    };
    let mut default_insert =
        InvocationTransaction::begin(&database, default_insert_invocation, actor, 9_999, 1, 1024)?;
    let default_schema = default_insert.load_catalog_table_schema(default_table_id)?;
    default_insert.load_catalog_indexes(&default_schema)?;
    let default_context = AuthorizationContext {
        principal_id: actor,
        role_ids: std::collections::HashSet::from([admin_role]),
        scope: std::collections::HashSet::from([
            (
                Operation::Usage,
                AccessTarget::Schema(default_schema.schema_id),
            ),
            (Operation::Insert, AccessTarget::Table(default_table_id)),
            (
                Operation::Insert,
                AccessTarget::Column {
                    table_id: default_table_id,
                    column_id: default_column_id,
                },
            ),
        ]),
    };
    let default_grants = [
        Grant {
            subject: Subject::Principal(actor),
            operation: Operation::Usage,
            target: AccessTarget::Schema(default_schema.schema_id),
        },
        Grant {
            subject: Subject::Principal(actor),
            operation: Operation::Insert,
            target: AccessTarget::Table(default_table_id),
        },
        Grant {
            subject: Subject::Principal(actor),
            operation: Operation::Insert,
            target: AccessTarget::Column {
                table_id: default_table_id,
                column_id: default_column_id,
            },
        },
    ];
    let default_policies = [
        Policy {
            subjects: vec![Subject::Principal(actor)],
            operation: Operation::Insert,
            target: AccessTarget::Table(default_table_id),
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Principal(actor)],
            operation: Operation::Insert,
            target: AccessTarget::Column {
                table_id: default_table_id,
                column_id: default_column_id,
            },
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let default_projection = prdb_engine::authorization::AuthorizedProjection {
        columns: &default_schema.columns,
        column_ids: &default_schema.column_ids,
        schema_version: default_schema.schema_version,
        schema_id: default_schema.schema_id,
        context: &default_context,
        grants: &default_grants,
        policies: &default_policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    default_insert.stage_insert_authorized_from_catalog_defaults(
        default_row,
        &default_projection,
        &[InsertInput::Default],
        &default_schema,
    )?;

    let replace_invocation = id(0x61);
    let mut replace_audit = identity_audit_event(0x62, actor, id(0x63));
    replace_audit.correlation_id = replace_invocation;
    let replace_barrier = Arc::new(Barrier::new(2));
    let (default_insert_result, replace_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&replace_barrier);
        let storage = &storage;
        let ddl = scope.spawn(move || {
            barrier.wait();
            storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
                invocation_id: replace_invocation,
                authenticated_actor_id: actor,
                table_id: default_table_id,
                column_id: default_column_id,
                default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(43)))),
                retention_deadline_us: 9_999,
                audit_event: replace_audit,
            })
        });
        replace_barrier.wait();
        let insert = default_insert.commit();
        let ddl = ddl.join().expect("default replacement thread panicked");
        (insert, ddl)
    });
    assert!(
        replace_result.is_ok(),
        "replacing the catalog default failed: {replace_result:?}"
    );
    let replace_sequence = replace_result?;
    let (default_insert_sequence, default_insert_status) = match default_insert_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            (None, OutcomeStatus::Aborted)
        }
        Err(error) => panic!("unexpected DEFAULT insert result: {error:?}"),
    };
    if let Some(sequence) = default_insert_sequence {
        assert!(sequence < replace_sequence);
    }
    let default_row_value = row(&database, &table_row_key(default_table_id, default_row.row))?;
    assert_eq!(
        default_row_value.as_ref().map(|stored| {
            decode_tuple(
                &default_schema.columns,
                default_schema.schema_version,
                &stored.payload,
            )
            .expect("stored default row must decode")
            .values
        }),
        (default_insert_status == OutcomeStatus::Committed)
            .then(|| vec![Some(ScalarValue::Integer(42))])
    );
    assert_eq!(
        lookup_outcome(&database, default_insert_invocation, actor, 1)?
            .ok_or("missing catalog DEFAULT insert outcome")?
            .status,
        default_insert_status
    );
    assert_eq!(
        lookup_outcome(&database, replace_invocation, actor, 1)?
            .ok_or("missing default replacement outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let drop_invocation = id(0x55);
    let stale_drop_writer_invocation = id(0x5d);
    let stale_drop_row_id = prdb_engine::overlay::RowId {
        table: default_table_id,
        row: id(0x5e),
    };
    let mut stale_drop_writer =
        storage.begin_invocation(stale_drop_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_drop_schema = stale_drop_writer.load_catalog_table_schema(default_table_id)?;
    stale_drop_writer.load_catalog_indexes(&stale_drop_schema)?;
    let stale_drop_tuple = encode_tuple(
        &stale_drop_schema.columns,
        stale_drop_schema.schema_version,
        &[Some(ScalarValue::Integer(8))],
    )?;
    stale_drop_writer.stage_put(stale_drop_row_id, &stale_drop_tuple)?;
    let mut drop_audit = identity_audit_event(0x56, actor, id(0x57));
    drop_audit.correlation_id = drop_invocation;
    let drop_barrier = Arc::new(Barrier::new(2));
    let (stale_drop_result, drop_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&drop_barrier);
        let storage = &storage;
        let ddl = scope.spawn(move || {
            barrier.wait();
            storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
                invocation_id: drop_invocation,
                authenticated_actor_id: actor,
                table_id: default_table_id,
                column_id: default_column_id,
                default: None,
                retention_deadline_us: 9_999,
                audit_event: drop_audit,
            })
        });
        drop_barrier.wait();
        let writer = stale_drop_writer.commit();
        let ddl = ddl.join().expect("default DROP thread panicked");
        (writer, ddl)
    });
    let drop_sequence = drop_result?;
    let (stale_drop_sequence, stale_drop_status) = match stale_drop_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            (None, OutcomeStatus::Aborted)
        }
        Err(error) => panic!("unexpected staged-write result in default DROP race: {error:?}"),
    };
    if let Some(sequence) = stale_drop_sequence {
        assert!(sequence < drop_sequence);
    }
    assert_eq!(
        row(
            &database,
            &table_row_key(default_table_id, stale_drop_row_id.row)
        )?
        .map(|stored| stored.payload),
        (stale_drop_status == OutcomeStatus::Committed).then_some(stale_drop_tuple)
    );
    assert_eq!(
        lookup_outcome(&database, stale_drop_writer_invocation, actor, 1)?
            .ok_or("missing stale writer outcome after default drop")?
            .status,
        stale_drop_status
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing default DROP outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let empty_drop_invocation = id(0x58);
    let mut empty_drop_audit = identity_audit_event(0x59, actor, id(0x5a));
    empty_drop_audit.correlation_id = empty_drop_invocation;
    assert_eq!(
        storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
            invocation_id: empty_drop_invocation,
            authenticated_actor_id: actor,
            table_id: default_table_id,
            column_id: default_column_id,
            default: None,
            retention_deadline_us: 9_999,
            audit_event: empty_drop_audit,
        }),
        Err(prdb_engine::storage_manager::TableLifecycleError::InvalidRequest)
    );

    let rename_id = id(0x71);
    let alter_id = id(0x72);
    let cross_alter_barrier = Arc::new(Barrier::new(2));
    let (rename_result, alter_result) = std::thread::scope(|scope| {
        let rename_barrier = Arc::clone(&cross_alter_barrier);
        let rename_storage = &storage;
        let rename = scope.spawn(move || {
            let mut audit = identity_audit_event(0x73, actor, default_table_id);
            audit.correlation_id = rename_id;
            rename_barrier.wait();
            rename_storage.rename_table(
                rename_id,
                actor,
                default_table_id,
                "concurrent_alter_table",
                9_999,
                audit,
            )
        });
        cross_alter_barrier.wait();
        let mut audit = identity_audit_event(0x74, actor, default_column_id);
        audit.correlation_id = alter_id;
        let alter =
            storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
                invocation_id: alter_id,
                authenticated_actor_id: actor,
                table_id: default_table_id,
                column_id: default_column_id,
                default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(99)))),
                retention_deadline_us: 9_999,
                audit_event: audit,
            });
        (rename.join().expect("table rename thread panicked"), alter)
    });
    let rename_conflicted = matches!(
        rename_result,
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::Conflict
        ))
    );
    if !rename_conflicted {
        rename_result.map_err(|error| format!("cross-ALTER table rename failed: {error:?}"))?;
    }
    let alter_conflicted = matches!(
        alter_result,
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::Conflict
        ))
    );
    if !alter_conflicted {
        alter_result.map_err(|error| format!("cross-ALTER default change failed: {error:?}"))?;
    }
    if rename_conflicted {
        let mut audit = identity_audit_event(0x75, actor, default_table_id);
        audit.correlation_id = id(0x75);
        storage.rename_table(
            id(0x75),
            actor,
            default_table_id,
            "concurrent_alter_table",
            9_999,
            audit,
        )?;
    }
    if alter_conflicted {
        let mut audit = identity_audit_event(0x76, actor, default_column_id);
        audit.correlation_id = id(0x76);
        storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
            invocation_id: id(0x76),
            authenticated_actor_id: actor,
            table_id: default_table_id,
            column_id: default_column_id,
            default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(99)))),
            retention_deadline_us: 9_999,
            audit_event: audit,
        })?;
    }
    assert_eq!(
        lookup_outcome(&database, rename_id, actor, 1)?
            .ok_or("missing cross-ALTER rename outcome")?
            .status,
        if rename_conflicted {
            OutcomeStatus::Aborted
        } else {
            OutcomeStatus::Committed
        }
    );
    assert_eq!(
        lookup_outcome(&database, alter_id, actor, 1)?
            .ok_or("missing cross-ALTER default outcome")?
            .status,
        if alter_conflicted {
            OutcomeStatus::Aborted
        } else {
            OutcomeStatus::Committed
        }
    );
    let renamed_table_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_relation_id_by_name(
            &renamed_table_snapshot,
            schema.schema_id,
            "concurrent_alter_table",
        )?,
        Some(default_table_id)
    );
    let mut verify_alters = storage.begin_invocation(id(0x77), actor, 9_999, 1, 64)?;
    let verified_schema = verify_alters.load_catalog_table_schema(default_table_id)?;
    assert!(
        verified_schema.default_expression_ids[0].is_some(),
        "concurrent default ALTER must survive table rename"
    );
    verify_alters.abort()?;
    Ok(())
}
