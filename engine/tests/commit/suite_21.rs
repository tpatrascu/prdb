use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::{
    lookup_column_id_by_name, lookup_column_id_by_ordinal, lookup_index_id_by_name,
    lookup_relation_id_by_name,
};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::schema::{self, CATALOG};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::{Database, ReadableDatabase};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::helpers::*;

pub(super) fn process_crash_during_schema_owner_transfer_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_SCHEMA_OWNER_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_SCHEMA_OWNER_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xb1; 16];
    const TRANSFER_INVOCATION_ID: [u8; 16] = [0xb2; 16];
    const TRANSFER_AUDIT_ID: [u8; 16] = [0xb3; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let setup = [
            principal_write_with_name(ACTOR_ID, 1, 1, "schema_owner_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "schema_owner_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ];
        let mut setup_audit = identity_audit_event(0xb4, admin_role, id(0xb5));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(id(0xb5), admin_role, i64::MAX, &setup, &[setup_audit])?;

        let create_invocation_id = id(0xb6);
        let mut create_audit = identity_audit_event(0xb7, ACTOR_ID, create_invocation_id);
        create_audit.correlation_id = create_invocation_id;
        let schema_id = storage
            .create_schema(
                create_invocation_id,
                ACTOR_ID,
                "schema_owner_crash",
                ACTOR_ID,
                i64::MAX,
                create_audit,
            )?
            .schema_id;
        let before_transfer_sequence = commit_sequence(&database)?;
        let metadata_path = database_path.with_extension("schema-owner-crash-metadata");
        let mut metadata = Vec::with_capacity(24);
        metadata.extend_from_slice(&before_transfer_sequence.to_le_bytes());
        metadata.extend_from_slice(&schema_id);
        std::fs::write(&metadata_path, metadata)?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let metadata = std::fs::read(&metadata_path)?;
        let schema_id: [u8; 16] = metadata[8..24].try_into()?;
        let mut transfer_audit =
            identity_audit_event(TRANSFER_AUDIT_ID[0], ACTOR_ID, TRANSFER_INVOCATION_ID);
        transfer_audit.event_id = TRANSFER_AUDIT_ID;
        transfer_audit.correlation_id = TRANSFER_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown schema owner crash boundary {boundary}").into());
        }
        let result = storage.set_schema_owner(
            TRANSFER_INVOCATION_ID,
            ACTOR_ID,
            schema_id,
            builtin_owner_id(),
            i64::MAX,
            transfer_audit,
        );
        if boundary == "write" {
            return Err(format!(
                "schema owner transfer returned without injected backend-write exit: {result:?}"
            )
            .into());
        }
        result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for (boundary, expected_exit) in [
        ("write", CHILD_EXIT_CODE),
        ("after_commit", POST_COMMIT_EXIT_CODE),
    ] {
        let fixture = Fixture::new();
        let database_path = fixture.path();
        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_schema_owner_transfer_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "schema owner child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("schema-owner-crash-metadata"))?;
        assert_eq!(metadata.len(), 24);
        let before_transfer_sequence = u64::from_le_bytes(metadata[0..8].try_into()?);
        let schema_id: [u8; 16] = metadata[8..24].try_into()?;
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let committed = boundary == "after_commit";
        let read = reopened.begin_read()?;
        let object = read
            .open_table(CATALOG)?
            .get(id_catalog_key(2, schema_id).as_slice())?
            .ok_or("schema object missing after owner-transfer recovery")?;
        let object = decode_registered_envelope(object.value())?;
        let owner_id = object
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .ok_or("schema object owner field missing")?
            .payload
            .as_slice();
        let owner_version = u64::from_le_bytes(
            object
                .fields
                .iter()
                .find(|field| field.tag == 5)
                .ok_or("schema object version field missing")?
                .payload
                .as_slice()
                .try_into()?,
        );
        let expected_owner_id = if committed {
            builtin_owner_id()
        } else {
            ACTOR_ID
        };
        assert_eq!(owner_id, &expected_owner_id[..]);
        assert_eq!(owner_version, 1 + u64::from(committed));
        let sequence = commit_sequence(&reopened)?;
        assert_eq!(sequence, before_transfer_sequence + u64::from(committed));
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&TRANSFER_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&TRANSFER_AUDIT_ID[..])?
            .is_some();
        assert_eq!(audit_exists, committed);
        if committed {
            let outcome = outcome.ok_or("committed owner-transfer outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
        } else {
            assert_eq!(outcome, None);
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_table_owner_transfer_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_TABLE_OWNER_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_TABLE_OWNER_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xa1; 16];
    const SUCCESSOR_ROLE_ID: [u8; 16] = [0xa2; 16];
    const TRANSFER_INVOCATION_ID: [u8; 16] = [0xa3; 16];
    const TRANSFER_AUDIT_ID: [u8; 16] = [0xa4; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let setup = [
            principal_write_with_name(ACTOR_ID, 1, 1, "table_owner_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "table_owner_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
            principal_write_with_name(SUCCESSOR_ROLE_ID, 2, 1, "table_owner_crash_role"),
            object_write_with_name(SUCCESSOR_ROLE_ID, 9, "table_owner_crash_role"),
        ];
        let mut setup_audit = identity_audit_event(0xa5, admin_role, id(0xa6));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(id(0xa6), admin_role, i64::MAX, &setup, &[setup_audit])?;

        let create_schema_invocation = id(0xa7);
        let mut create_schema_audit = identity_audit_event(0xa8, ACTOR_ID, id(0xa9));
        create_schema_audit.correlation_id = create_schema_invocation;
        let schema = storage.create_schema(
            create_schema_invocation,
            ACTOR_ID,
            "table_owner_crash_schema",
            ACTOR_ID,
            i64::MAX,
            create_schema_audit,
        )?;
        let create_table_invocation = id(0xaa);
        let mut create_table_audit = identity_audit_event(0xab, ACTOR_ID, id(0xac));
        create_table_audit.correlation_id = create_table_invocation;
        let table = storage.create_table(CreateTableRequest {
            invocation_id: create_table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "owner_transfer".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "value".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: create_table_audit,
        })?;
        let before_transfer_sequence = commit_sequence(&database)?;
        let metadata_path = database_path.with_extension("table-owner-crash-metadata");
        let mut metadata = Vec::with_capacity(40);
        metadata.extend_from_slice(&before_transfer_sequence.to_le_bytes());
        metadata.extend_from_slice(&table.table_id);
        let (owner_version, table_epoch) = {
            let read = database.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let object = decode_registered_envelope(
                catalog
                    .get(id_catalog_key(2, table.table_id).as_slice())?
                    .ok_or("created table object missing")?
                    .value(),
            )?;
            let record = decode_registered_envelope(
                catalog
                    .get(id_catalog_key(5, table.table_id).as_slice())?
                    .ok_or("created table record missing")?
                    .value(),
            )?;
            let owner_version = u64::from_le_bytes(
                object
                    .fields
                    .iter()
                    .find(|field| field.tag == 5)
                    .ok_or("table owner version missing")?
                    .payload
                    .as_slice()
                    .try_into()?,
            );
            let table_epoch = u64::from_le_bytes(
                record
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("table write epoch missing")?
                    .payload
                    .as_slice()
                    .try_into()?,
            );
            (owner_version, table_epoch)
        };
        metadata.extend_from_slice(&owner_version.to_le_bytes());
        metadata.extend_from_slice(&table_epoch.to_le_bytes());
        std::fs::write(&metadata_path, metadata)?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let metadata = std::fs::read(&metadata_path)?;
        let table_id: [u8; 16] = metadata[8..24].try_into()?;
        let mut transfer_audit =
            identity_audit_event(TRANSFER_AUDIT_ID[0], ACTOR_ID, TRANSFER_INVOCATION_ID);
        transfer_audit.event_id = TRANSFER_AUDIT_ID;
        transfer_audit.correlation_id = TRANSFER_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown table owner crash boundary {boundary}").into());
        }
        let result = storage.set_table_owner(
            TRANSFER_INVOCATION_ID,
            ACTOR_ID,
            table_id,
            SUCCESSOR_ROLE_ID,
            i64::MAX,
            transfer_audit,
        );
        if boundary == "write" {
            return Err(format!(
                "table owner transfer returned without injected backend-write exit: {result:?}"
            )
            .into());
        }
        result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for (boundary, expected_exit) in [
        ("write", CHILD_EXIT_CODE),
        ("after_commit", POST_COMMIT_EXIT_CODE),
    ] {
        let fixture = Fixture::new();
        let database_path = fixture.path();
        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_table_owner_transfer_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "table owner child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("table-owner-crash-metadata"))?;
        assert_eq!(metadata.len(), 40);
        let before_transfer_sequence = u64::from_le_bytes(metadata[0..8].try_into()?);
        let table_id: [u8; 16] = metadata[8..24].try_into()?;
        let owner_version = u64::from_le_bytes(metadata[24..32].try_into()?);
        let table_epoch = u64::from_le_bytes(metadata[32..40].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let committed = boundary == "after_commit";
        let read = reopened.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let object = decode_registered_envelope(
            catalog
                .get(id_catalog_key(2, table_id).as_slice())?
                .ok_or("table object missing after owner-transfer recovery")?
                .value(),
        )?;
        let record = decode_registered_envelope(
            catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("table record missing after owner-transfer recovery")?
                .value(),
        )?;
        let expected_owner_id = if committed {
            SUCCESSOR_ROLE_ID
        } else {
            ACTOR_ID
        };
        assert_eq!(
            object
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("table owner missing after recovery")?
                .payload,
            expected_owner_id
        );
        assert_eq!(
            u64::from_le_bytes(
                object
                    .fields
                    .iter()
                    .find(|field| field.tag == 5)
                    .ok_or("table owner version missing after recovery")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            owner_version + u64::from(committed)
        );
        assert_eq!(
            u64::from_le_bytes(
                record
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("table epoch missing after recovery")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            table_epoch + u64::from(committed)
        );
        let sequence = commit_sequence(&reopened)?;
        assert_eq!(sequence, before_transfer_sequence + u64::from(committed));
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&TRANSFER_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&TRANSFER_AUDIT_ID[..])?
            .is_some();
        assert_eq!(audit_exists, committed);
        if committed {
            let outcome = outcome.ok_or("committed owner-transfer outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
        } else {
            assert_eq!(outcome, None);
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_drop_column_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_DROP_COLUMN_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_DROP_COLUMN_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xd1; 16];
    const DROP_INVOCATION_ID: [u8; 16] = [0xd2; 16];
    const DROP_AUDIT_ID: [u8; 16] = [0xd3; 16];
    const ROW_ID: [u8; 16] = [0xd4; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let setup = [
            principal_write_with_name(ACTOR_ID, 1, 1, "drop_column_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "drop_column_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ];
        let mut setup_audit = identity_audit_event(0xd5, admin_role, id(0xd6));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(id(0xd6), admin_role, i64::MAX, &setup, &[setup_audit])?;
        let schema_invocation = id(0xd7);
        let mut schema_audit = identity_audit_event(0xd8, ACTOR_ID, id(0xd9));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "drop_column_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let table_invocation = id(0xda);
        let mut table_audit = identity_audit_event(0xdb, ACTOR_ID, id(0xdc));
        table_audit.correlation_id = table_invocation;
        let table = storage.create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "drop_column_crash_table".to_owned(),
            columns: vec![
                TableColumnDefinition {
                    name: "removed".to_owned(),
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: None,
                },
                TableColumnDefinition {
                    name: "retained".to_owned(),
                    scalar_type: ScalarType::Text,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: None,
                },
            ],
            retention_deadline_us: i64::MAX,
            audit_event: table_audit,
        })?;
        let seed_invocation = id(0xdd);
        let mut seed = storage.begin_invocation(seed_invocation, ACTOR_ID, i64::MAX, 1, 1024)?;
        let seed_schema = seed.load_catalog_table_schema(table.table_id)?;
        seed.load_catalog_indexes(&seed_schema)?;
        let row_id = prdb_engine::overlay::RowId {
            table: table.table_id,
            row: ROW_ID,
        };
        seed.stage_put(
            row_id,
            &encode_tuple(
                &seed_schema.columns,
                seed_schema.schema_version,
                &[
                    Some(ScalarValue::Integer(27)),
                    Some(ScalarValue::Text("retained-value".to_owned())),
                ],
            )?,
        )?;
        seed.commit()?;
        let before_drop_sequence = commit_sequence(&database)?;
        let mut metadata = Vec::with_capacity(80);
        metadata.extend_from_slice(&table.table_id);
        metadata.extend_from_slice(&table.column_ids[0]);
        metadata.extend_from_slice(&table.column_ids[1]);
        metadata.extend_from_slice(&ROW_ID);
        metadata.extend_from_slice(&seed_schema.schema_version.to_le_bytes());
        metadata.extend_from_slice(&before_drop_sequence.to_le_bytes());
        std::fs::write(
            database_path.with_extension("drop-column-crash-metadata"),
            metadata,
        )?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let mut drop_audit = identity_audit_event(DROP_AUDIT_ID[0], ACTOR_ID, id(0xde));
        drop_audit.event_id = DROP_AUDIT_ID;
        drop_audit.correlation_id = DROP_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown DROP COLUMN crash boundary {boundary}").into());
        }
        let drop_result = storage.drop_column(
            DROP_INVOCATION_ID,
            ACTOR_ID,
            table.table_id,
            table.column_ids[0],
            i64::MAX,
            drop_audit,
        );
        if boundary == "write" {
            return Err(format!(
                "DROP COLUMN returned without injected backend-write exit: {drop_result:?}"
            )
            .into());
        }
        drop_result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for (boundary, expected_exit) in [
        ("write", CHILD_EXIT_CODE),
        ("after_commit", POST_COMMIT_EXIT_CODE),
    ] {
        let fixture = Fixture::new();
        let database_path = fixture.path();
        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_drop_column_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "DROP COLUMN child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("drop-column-crash-metadata"))?;
        assert_eq!(metadata.len(), 80);
        let table_id: [u8; 16] = metadata[0..16].try_into()?;
        let dropped_column_id: [u8; 16] = metadata[16..32].try_into()?;
        let retained_column_id: [u8; 16] = metadata[32..48].try_into()?;
        let row_id: [u8; 16] = metadata[48..64].try_into()?;
        let old_schema_version = u64::from_le_bytes(metadata[64..72].try_into()?);
        let before_drop_sequence = u64::from_le_bytes(metadata[72..80].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_record = decode_registered_envelope(
            catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("recovered table record missing")?
                .value(),
        )?;
        let schema_version = u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("table schema version missing")?
                .payload
                .as_slice()
                .try_into()?,
        );
        let dropped_column = catalog
            .get(id_catalog_key(6, dropped_column_id).as_slice())?
            .is_some();
        let retained_column = decode_registered_envelope(
            catalog
                .get(id_catalog_key(6, retained_column_id).as_slice())?
                .ok_or("retained column missing")?
                .value(),
        )?;
        let retained_ordinal = u32::from_le_bytes(
            retained_column
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("retained column ordinal missing")?
                .payload
                .as_slice()
                .try_into()?,
        );
        let row_record =
            row(&reopened, &table_row_key(table_id, row_id))?.ok_or("recovered row missing")?;
        let sequence = commit_sequence(&reopened)?;
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&DROP_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&DROP_AUDIT_ID[..])?
            .is_some();
        if boundary == "write" {
            assert_eq!(schema_version, old_schema_version);
            assert!(dropped_column);
            assert_eq!(retained_ordinal, 1);
            assert_eq!(sequence, before_drop_sequence);
            assert_eq!(outcome, None);
            assert!(!audit_exists);
            assert_eq!(
                decode_tuple(
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
                    old_schema_version,
                    &row_record.payload,
                )?
                .values,
                vec![
                    Some(ScalarValue::Integer(27)),
                    Some(ScalarValue::Text("retained-value".to_owned())),
                ]
            );
        } else {
            assert_eq!(schema_version, old_schema_version + 1);
            assert!(!dropped_column);
            assert_eq!(retained_ordinal, 0);
            assert_eq!(sequence, before_drop_sequence + 1);
            let outcome = outcome.ok_or("committed DROP COLUMN outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
            assert!(audit_exists);
            assert_eq!(
                decode_tuple(
                    &[ColumnSchema {
                        scalar_type: ScalarType::Text,
                        nullable: false,
                    }],
                    old_schema_version + 1,
                    &row_record.payload,
                )?
                .values,
                vec![Some(ScalarValue::Text("retained-value".to_owned()))]
            );
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_add_column_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AddTableColumnRequest, CreateTableRequest, TableColumnDefinition,
    };
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_ADD_COLUMN_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_ADD_COLUMN_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xe1; 16];
    const ADD_INVOCATION_ID: [u8; 16] = [0xe2; 16];
    const ADD_AUDIT_ID: [u8; 16] = [0xe3; 16];
    const ROW_ID: [u8; 16] = [0xe4; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let setup = [
            principal_write_with_name(ACTOR_ID, 1, 1, "add_column_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "add_column_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ];
        let mut setup_audit = identity_audit_event(0xe5, admin_role, id(0xe6));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(id(0xe6), admin_role, i64::MAX, &setup, &[setup_audit])?;
        let schema_invocation = id(0xe7);
        let mut schema_audit = identity_audit_event(0xe8, ACTOR_ID, id(0xe9));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "add_column_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let table_invocation = id(0xea);
        let mut table_audit = identity_audit_event(0xeb, ACTOR_ID, id(0xec));
        table_audit.correlation_id = table_invocation;
        let table = storage.create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "add_column_crash_table".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "payload".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: table_audit,
        })?;
        let seed_invocation = id(0xed);
        let mut seed = storage.begin_invocation(seed_invocation, ACTOR_ID, i64::MAX, 1, 1024)?;
        let seed_schema = seed.load_catalog_table_schema(table.table_id)?;
        seed.load_catalog_indexes(&seed_schema)?;
        let row_id = prdb_engine::overlay::RowId {
            table: table.table_id,
            row: ROW_ID,
        };
        seed.stage_put(
            row_id,
            &encode_tuple(
                &seed_schema.columns,
                seed_schema.schema_version,
                &[Some(ScalarValue::Text("before-add".to_owned()))],
            )?,
        )?;
        seed.commit()?;
        let before_add_sequence = commit_sequence(&database)?;
        let mut metadata = Vec::with_capacity(64);
        metadata.extend_from_slice(&table.table_id);
        metadata.extend_from_slice(&table.column_ids[0]);
        metadata.extend_from_slice(&ROW_ID);
        metadata.extend_from_slice(&seed_schema.schema_version.to_le_bytes());
        metadata.extend_from_slice(&before_add_sequence.to_le_bytes());
        std::fs::write(
            database_path.with_extension("add-column-crash-metadata"),
            metadata,
        )?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let mut add_audit = identity_audit_event(ADD_AUDIT_ID[0], ACTOR_ID, id(0xee));
        add_audit.event_id = ADD_AUDIT_ID;
        add_audit.correlation_id = ADD_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown ADD COLUMN crash boundary {boundary}").into());
        }
        let add_result = storage.add_table_column(AddTableColumnRequest {
            invocation_id: ADD_INVOCATION_ID,
            authenticated_actor_id: ACTOR_ID,
            table_id: table.table_id,
            column: TableColumnDefinition {
                name: "extra".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            retention_deadline_us: i64::MAX,
            audit_event: add_audit,
        });
        if boundary == "write" {
            return Err(format!(
                "ADD COLUMN returned without injected backend-write exit: {add_result:?}"
            )
            .into());
        }
        add_result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for (boundary, expected_exit) in [
        ("write", CHILD_EXIT_CODE),
        ("after_commit", POST_COMMIT_EXIT_CODE),
    ] {
        let fixture = Fixture::new();
        let database_path = fixture.path();
        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_add_column_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "ADD COLUMN child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("add-column-crash-metadata"))?;
        assert_eq!(metadata.len(), 64);
        let table_id: [u8; 16] = metadata[0..16].try_into()?;
        let retained_column_id: [u8; 16] = metadata[16..32].try_into()?;
        let row_id: [u8; 16] = metadata[32..48].try_into()?;
        let old_schema_version = u64::from_le_bytes(metadata[48..56].try_into()?);
        let before_add_sequence = u64::from_le_bytes(metadata[56..64].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_record = decode_registered_envelope(
            catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("recovered table record missing")?
                .value(),
        )?;
        let schema_version = u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("table schema version missing")?
                .payload
                .as_slice()
                .try_into()?,
        );
        let new_column_id = lookup_column_id_by_ordinal(&read, table_id, 1)?;
        let new_column_exists = if let Some(column_id) = new_column_id {
            catalog
                .get(id_catalog_key(6, column_id).as_slice())?
                .is_some()
        } else {
            false
        };
        let retained_column = decode_registered_envelope(
            catalog
                .get(id_catalog_key(6, retained_column_id).as_slice())?
                .ok_or("retained column missing")?
                .value(),
        )?;
        let retained_ordinal = u32::from_le_bytes(
            retained_column
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("retained column ordinal missing")?
                .payload
                .as_slice()
                .try_into()?,
        );
        let row_record =
            row(&reopened, &table_row_key(table_id, row_id))?.ok_or("recovered row missing")?;
        let sequence = commit_sequence(&reopened)?;
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&ADD_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&ADD_AUDIT_ID[..])?
            .is_some();
        if boundary == "write" {
            assert_eq!(schema_version, old_schema_version);
            assert_eq!(new_column_id, None);
            assert!(!new_column_exists);
            assert_eq!(retained_ordinal, 0);
            assert_eq!(sequence, before_add_sequence);
            assert_eq!(outcome, None);
            assert!(!audit_exists);
            assert_eq!(
                decode_tuple(
                    &[ColumnSchema {
                        scalar_type: ScalarType::Text,
                        nullable: false,
                    }],
                    old_schema_version,
                    &row_record.payload,
                )?
                .values,
                vec![Some(ScalarValue::Text("before-add".to_owned()))]
            );
        } else {
            assert_eq!(schema_version, old_schema_version + 1);
            assert!(new_column_id.is_some());
            assert!(new_column_exists);
            assert_eq!(retained_ordinal, 0);
            assert_eq!(sequence, before_add_sequence + 1);
            let outcome = outcome.ok_or("committed ADD COLUMN outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
            assert!(audit_exists);
            assert_eq!(
                decode_tuple(
                    &[
                        ColumnSchema {
                            scalar_type: ScalarType::Text,
                            nullable: false,
                        },
                        ColumnSchema {
                            scalar_type: ScalarType::Integer,
                            nullable: true,
                        },
                    ],
                    old_schema_version + 1,
                    &row_record.payload,
                )?
                .values,
                vec![Some(ScalarValue::Text("before-add".to_owned())), None]
            );
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_drop_table_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateCheckConstraintRequest, CreateForeignKeyRequest, CreateIndexRequest,
        CreatePolicyRequest, CreatePrimaryKeyRequest, CreateTableRequest,
        CreateUniqueConstraintRequest, ObjectGrantPrivilege, ObjectGrantTarget, PolicyKind,
        SetObjectGrantRequest, TableColumnDefinition,
    };
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_DROP_TABLE_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_DROP_TABLE_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xf1; 16];
    const GRANTEE_ID: [u8; 16] = [0x18; 16];
    const INDEX_INVOCATION_ID: [u8; 16] = [0xf2; 16];
    const DROP_INVOCATION_ID: [u8; 16] = [0xf3; 16];
    const DROP_AUDIT_ID: [u8; 16] = [0xf4; 16];
    const ROW_ID: [u8; 16] = [0xf5; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let mut setup_audit = identity_audit_event(0xf6, admin_role, id(0xf7));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(
            id(0xf7),
            admin_role,
            i64::MAX,
            &[
                principal_write_with_name(ACTOR_ID, 1, 1, "drop_table_crash_actor"),
                object_write_with_name(ACTOR_ID, 8, "drop_table_crash_actor"),
                principal_write_with_name(GRANTEE_ID, 1, 1, "drop_table_crash_grantee"),
                object_write_with_name(GRANTEE_ID, 8, "drop_table_crash_grantee"),
                membership_write(ACTOR_ID, admin_role),
                dependency_write(ACTOR_ID, admin_role, 5),
            ],
            &[setup_audit],
        )?;
        let schema_invocation = id(0xf8);
        let mut schema_audit = identity_audit_event(0xf9, ACTOR_ID, id(0xfa));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "drop_table_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let table_invocation = id(0xfb);
        let mut table_audit = identity_audit_event(0xfc, ACTOR_ID, id(0xfd));
        table_audit.correlation_id = table_invocation;
        let table = storage.create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "drop_table_crash_table".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "value".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: table_audit,
        })?;
        let mut seed = storage.begin_invocation(id(0xfe), ACTOR_ID, i64::MAX, 1, 1024)?;
        let table_schema = seed.load_catalog_table_schema(table.table_id)?;
        seed.load_catalog_indexes(&table_schema)?;
        let row_id = prdb_engine::overlay::RowId {
            table: table.table_id,
            row: ROW_ID,
        };
        seed.stage_put(
            row_id,
            &encode_tuple(
                &table_schema.columns,
                table_schema.schema_version,
                &[Some(ScalarValue::Integer(23))],
            )?,
        )?;
        seed.commit()?;
        let mut index_audit = identity_audit_event(0xff, ACTOR_ID, id(0x10));
        index_audit.correlation_id = INDEX_INVOCATION_ID;
        let index = storage.create_index(CreateIndexRequest {
            invocation_id: INDEX_INVOCATION_ID,
            authenticated_actor_id: ACTOR_ID,
            table_id: table.table_id,
            name: "drop_table_crash_index".to_owned(),
            column_ids: table.column_ids.clone(),
            unique: false,
            retention_deadline_us: i64::MAX,
            audit_event: index_audit,
        })?;
        let mut primary_key_audit = identity_audit_event(0x12, ACTOR_ID, table.table_id);
        primary_key_audit.correlation_id = id(0x13);
        let primary_key = storage.create_primary_key(CreatePrimaryKeyRequest {
            invocation_id: id(0x13),
            authenticated_actor_id: ACTOR_ID,
            table_id: table.table_id,
            column_ids: vec![table.column_ids[0]],
            retention_deadline_us: i64::MAX,
            audit_event: primary_key_audit,
        })?;
        let mut references_audit = identity_audit_event(0x28, builtin_owner_id(), id(0x29));
        references_audit.action = AuditAction::AuthorityChange;
        let mut actor_epoch = principal_write_with_name(ACTOR_ID, 1, 2, "drop_table_crash_actor");
        actor_epoch.expected_value =
            principal_write_with_name(ACTOR_ID, 1, 1, "drop_table_crash_actor").payload;
        storage.commit_catalog_and_audit(
            id(0x29),
            builtin_owner_id(),
            i64::MAX,
            &[
                grant_write(
                    ACTOR_ID,
                    table.table_id,
                    6,
                    1,
                    table.table_id,
                    builtin_owner_id(),
                ),
                dependency_write(ACTOR_ID, table.table_id, 5),
                actor_epoch,
            ],
            &[references_audit],
        )?;
        let mut foreign_key_audit = identity_audit_event(0x19, ACTOR_ID, table.table_id);
        foreign_key_audit.correlation_id = id(0x1a);
        let foreign_key = storage.create_foreign_key(CreateForeignKeyRequest {
            invocation_id: id(0x1a),
            authenticated_actor_id: ACTOR_ID,
            source_table_id: table.table_id,
            source_column_ids: vec![table.column_ids[0]],
            target_constraint_id: primary_key.constraint_id,
            retention_deadline_us: i64::MAX,
            audit_event: foreign_key_audit,
        })?;
        let mut unique_audit = identity_audit_event(0x14, ACTOR_ID, table.table_id);
        unique_audit.correlation_id = id(0x15);
        let unique = storage.create_unique_constraint(CreateUniqueConstraintRequest {
            invocation_id: id(0x15),
            authenticated_actor_id: ACTOR_ID,
            table_id: table.table_id,
            column_ids: vec![table.column_ids[0]],
            retention_deadline_us: i64::MAX,
            audit_event: unique_audit,
        })?;
        let mut check_audit = identity_audit_event(0x16, ACTOR_ID, table.table_id);
        check_audit.correlation_id = id(0x17);
        let check = storage.create_check_constraint(CreateCheckConstraintRequest {
            invocation_id: id(0x17),
            authenticated_actor_id: ACTOR_ID,
            table_id: table.table_id,
            name: None,
            expression: prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(true))),
            dependency_column_ids: Vec::new(),
            retention_deadline_us: i64::MAX,
            audit_event: check_audit,
        })?;
        let mut policy_audit = identity_audit_event(0x1a, ACTOR_ID, table.table_id);
        policy_audit.correlation_id = id(0x1b);
        let policy = storage.create_policy(CreatePolicyRequest {
            invocation_id: id(0x1b),
            authenticated_actor_id: ACTOR_ID,
            table_id: table.table_id,
            name: "drop_table_crash_policy".to_owned(),
            kind: PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients: vec![prdb_engine::authorization::Subject::Principal(ACTOR_ID)],
            target_column_ids: Vec::new(),
            using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                ScalarValue::Boolean(true),
            ))),
            with_check_expression: None,
            retention_deadline_us: i64::MAX,
            audit_event: policy_audit,
        })?;
        let mut grant_audit = identity_audit_event(0x1c, ACTOR_ID, table.table_id);
        grant_audit.action = AuditAction::AuthorityChange;
        grant_audit.correlation_id = id(0x1d);
        storage.set_object_grant(SetObjectGrantRequest {
            invocation_id: id(0x1d),
            authenticated_actor_id: ACTOR_ID,
            grantee_id: GRANTEE_ID,
            object_id: table.table_id,
            privilege: ObjectGrantPrivilege::Select,
            target: ObjectGrantTarget::Object,
            grant: true,
            retention_deadline_us: i64::MAX,
            audit_event: grant_audit,
        })?;
        let before_drop_sequence = commit_sequence(&database)?;
        let mut metadata = Vec::with_capacity(240);
        metadata.extend_from_slice(&schema.schema_id);
        metadata.extend_from_slice(&table.table_id);
        metadata.extend_from_slice(&table.column_ids[0]);
        metadata.extend_from_slice(&index.index_id);
        metadata.extend_from_slice(&ROW_ID);
        metadata.extend_from_slice(&table_schema.schema_version.to_le_bytes());
        metadata.extend_from_slice(&before_drop_sequence.to_le_bytes());
        metadata.extend_from_slice(&primary_key.constraint_id);
        metadata.extend_from_slice(&primary_key.index_id);
        metadata.extend_from_slice(&unique.constraint_id);
        metadata.extend_from_slice(&unique.index_id);
        metadata.extend_from_slice(&check.constraint_id);
        metadata.extend_from_slice(&check.expression_id);
        metadata.extend_from_slice(&policy.policy_id);
        metadata.extend_from_slice(
            &policy
                .using_expression_id
                .ok_or("created row policy is missing its USING expression")?,
        );
        metadata.extend_from_slice(&foreign_key.foreign_key_id);
        std::fs::write(
            database_path.with_extension("drop-table-crash-metadata"),
            metadata,
        )?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let mut drop_audit = identity_audit_event(DROP_AUDIT_ID[0], ACTOR_ID, id(0x11));
        drop_audit.event_id = DROP_AUDIT_ID;
        drop_audit.correlation_id = DROP_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown DROP TABLE crash boundary {boundary}").into());
        }
        let drop_result = storage.drop_table(
            DROP_INVOCATION_ID,
            ACTOR_ID,
            table.table_id,
            i64::MAX,
            drop_audit,
        );
        if boundary == "write" {
            return Err(format!(
                "DROP TABLE returned without injected backend-write exit: {drop_result:?}"
            )
            .into());
        }
        drop_result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for (boundary, expected_exit) in [
        ("write", CHILD_EXIT_CODE),
        ("after_commit", POST_COMMIT_EXIT_CODE),
    ] {
        let fixture = Fixture::new();
        let database_path = fixture.path();
        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_drop_table_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "DROP TABLE child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("drop-table-crash-metadata"))?;
        assert_eq!(metadata.len(), 240);
        let schema_id: [u8; 16] = metadata[0..16].try_into()?;
        let table_id: [u8; 16] = metadata[16..32].try_into()?;
        let column_id: [u8; 16] = metadata[32..48].try_into()?;
        let index_id: [u8; 16] = metadata[48..64].try_into()?;
        let row_id: [u8; 16] = metadata[64..80].try_into()?;
        let schema_version = u64::from_le_bytes(metadata[80..88].try_into()?);
        let before_drop_sequence = u64::from_le_bytes(metadata[88..96].try_into()?);
        let primary_key_id: [u8; 16] = metadata[96..112].try_into()?;
        let primary_key_index_id: [u8; 16] = metadata[112..128].try_into()?;
        let unique_id: [u8; 16] = metadata[128..144].try_into()?;
        let unique_index_id: [u8; 16] = metadata[144..160].try_into()?;
        let check_id: [u8; 16] = metadata[160..176].try_into()?;
        let check_expression_id: [u8; 16] = metadata[176..192].try_into()?;
        let policy_id: [u8; 16] = metadata[192..208].try_into()?;
        let policy_expression_id: [u8; 16] = metadata[208..224].try_into()?;
        let foreign_key_id: [u8; 16] = metadata[224..240].try_into()?;
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_exists = catalog
            .get(id_catalog_key(5, table_id).as_slice())?
            .is_some();
        let column_exists = catalog
            .get(id_catalog_key(6, column_id).as_slice())?
            .is_some();
        let recovered_index = lookup_index_id_by_name(&read, schema_id, "drop_table_crash_index")?;
        let relation = lookup_relation_id_by_name(&read, schema_id, "drop_table_crash_table")?;
        let mut recovered_indexes =
            prdb_engine::catalog_index::scan_index_ids_for_table(&read, table_id, 8)?;
        recovered_indexes.sort_unstable();
        let mut expected_indexes = vec![index_id, primary_key_index_id, unique_index_id];
        expected_indexes.sort_unstable();
        let mut recovered_constraints =
            prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, table_id, 8)?;
        recovered_constraints.sort_unstable();
        let mut expected_constraints = vec![primary_key_id, foreign_key_id, unique_id, check_id];
        expected_constraints.sort_unstable();
        let grant_key = grant_write(GRANTEE_ID, table_id, 2, 1, table_id, ACTOR_ID).key;
        let grant_dependency_key = dependency_write(GRANTEE_ID, table_id, 5).key;
        let references_grant_key =
            grant_write(ACTOR_ID, table_id, 6, 1, table_id, builtin_owner_id()).key;
        let references_grant_dependency_key = dependency_write(ACTOR_ID, table_id, 5).key;
        let policy_expression_dependency_key =
            dependency_write(policy_id, policy_expression_id, 2).key;
        let policy_recipient_dependency_key = dependency_write(policy_id, ACTOR_ID, 5).key;
        let foreign_key_target_dependency_key =
            dependency_write(foreign_key_id, primary_key_id, 3).key;
        let row_key = table_row_key(table_id, row_id);
        let row_exists = row(&reopened, &row_key)?.is_some();
        let row_payload = encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            }],
            schema_version,
            &[Some(ScalarValue::Integer(23))],
        )?;
        let index_key = prdb_engine::index_key::derive_index_key(
            index_id,
            row_id,
            schema_version,
            &[column_id],
            &[ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            }],
            &row_payload,
            &[column_id],
        )?;
        let mut reverse_index_key = row_key.to_vec();
        reverse_index_key.extend_from_slice(&index_key.key[..index_key.key.len() - 16]);
        for owned_index_id in [primary_key_index_id, unique_index_id] {
            let owned_key = prdb_engine::index_key::derive_index_key(
                owned_index_id,
                row_id,
                schema_version,
                &[column_id],
                &[ColumnSchema {
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                }],
                &row_payload,
                &[column_id],
            )?;
            let mut owned_reverse_key = row_key.to_vec();
            owned_reverse_key.extend_from_slice(&owned_key.key[..owned_key.key.len() - 16]);
            let owned_forward_exists = read
                .open_table(schema::INDEXES)?
                .get(owned_key.key.as_slice())?
                .is_some();
            let owned_reverse_exists = read
                .open_table(schema::ROW_INDEXES)?
                .get(owned_reverse_key.as_slice())?
                .is_some();
            assert_eq!(owned_forward_exists, boundary == "write");
            assert_eq!(owned_reverse_exists, boundary == "write");
        }
        let forward_index_exists = read
            .open_table(schema::INDEXES)?
            .get(index_key.key.as_slice())?
            .is_some();
        let reverse_index_exists = read
            .open_table(schema::ROW_INDEXES)?
            .get(reverse_index_key.as_slice())?
            .is_some();
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&DROP_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&DROP_AUDIT_ID[..])?
            .is_some();
        let sequence = commit_sequence(&reopened)?;

        if boundary == "write" {
            assert!(table_exists);
            assert!(column_exists);
            assert_eq!(recovered_index, Some(index_id));
            assert_eq!(recovered_indexes, expected_indexes);
            assert_eq!(recovered_constraints, expected_constraints);
            for retained_id in [policy_id, policy_expression_id] {
                assert!(
                    catalog
                        .get(id_catalog_key(2, retained_id).as_slice())?
                        .is_some()
                );
            }
            assert!(
                catalog
                    .get(id_catalog_key(13, policy_id).as_slice())?
                    .is_some()
            );
            assert!(
                catalog
                    .get(id_catalog_key(7, policy_expression_id).as_slice())?
                    .is_some()
            );
            for retained_key in [
                grant_key.as_slice(),
                grant_dependency_key.as_slice(),
                references_grant_key.as_slice(),
                references_grant_dependency_key.as_slice(),
                policy_expression_dependency_key.as_slice(),
                policy_recipient_dependency_key.as_slice(),
                foreign_key_target_dependency_key.as_slice(),
            ] {
                assert!(catalog.get(retained_key)?.is_some());
            }
            for retained_index_id in &expected_indexes {
                assert!(
                    catalog
                        .get(id_catalog_key(2, *retained_index_id).as_slice())?
                        .is_some()
                );
                assert!(
                    catalog
                        .get(id_catalog_key(9, *retained_index_id).as_slice())?
                        .is_some()
                );
            }
            for retained_constraint_id in &expected_constraints {
                assert!(
                    catalog
                        .get(id_catalog_key(2, *retained_constraint_id).as_slice())?
                        .is_some()
                );
                assert!(
                    catalog
                        .get(id_catalog_key(8, *retained_constraint_id).as_slice())?
                        .is_some()
                );
            }
            assert!(
                catalog
                    .get(id_catalog_key(7, check_expression_id).as_slice())?
                    .is_some()
            );
            assert_eq!(relation, Some(table_id));
            assert!(row_exists);
            assert!(forward_index_exists);
            assert!(reverse_index_exists);
            assert_eq!(sequence, before_drop_sequence);
            assert_eq!(outcome, None);
            assert!(!audit_exists);
            assert_eq!(
                row(&reopened, &row_key)?
                    .ok_or("pre-drop row missing")?
                    .payload,
                row_payload
            );
        } else {
            assert!(!table_exists);
            assert!(!column_exists);
            assert_eq!(recovered_index, None);
            assert!(recovered_indexes.is_empty());
            assert!(recovered_constraints.is_empty());
            for removed_id in [policy_id, policy_expression_id] {
                assert!(
                    catalog
                        .get(id_catalog_key(2, removed_id).as_slice())?
                        .is_none()
                );
            }
            assert!(
                catalog
                    .get(id_catalog_key(13, policy_id).as_slice())?
                    .is_none()
            );
            assert!(
                catalog
                    .get(id_catalog_key(7, policy_expression_id).as_slice())?
                    .is_none()
            );
            for removed_key in [
                grant_key.as_slice(),
                grant_dependency_key.as_slice(),
                references_grant_key.as_slice(),
                references_grant_dependency_key.as_slice(),
                policy_expression_dependency_key.as_slice(),
                policy_recipient_dependency_key.as_slice(),
                foreign_key_target_dependency_key.as_slice(),
            ] {
                assert!(catalog.get(removed_key)?.is_none());
            }
            for removed_id in expected_indexes {
                assert!(
                    catalog
                        .get(id_catalog_key(2, removed_id).as_slice())?
                        .is_none()
                );
                assert!(
                    catalog
                        .get(id_catalog_key(9, removed_id).as_slice())?
                        .is_none()
                );
            }
            for removed_id in expected_constraints {
                assert!(
                    catalog
                        .get(id_catalog_key(2, removed_id).as_slice())?
                        .is_none()
                );
                assert!(
                    catalog
                        .get(id_catalog_key(8, removed_id).as_slice())?
                        .is_none()
                );
            }
            assert!(
                catalog
                    .get(id_catalog_key(7, check_expression_id).as_slice())?
                    .is_none()
            );
            assert_eq!(relation, None);
            assert!(!row_exists);
            assert!(!forward_index_exists);
            assert!(!reverse_index_exists);
            assert_eq!(sequence, before_drop_sequence + 1);
            let outcome = outcome.ok_or("committed DROP TABLE outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
            assert!(audit_exists);
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_create_table_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_CREATE_TABLE_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_CREATE_TABLE_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xd1; 16];
    const CREATE_INVOCATION_ID: [u8; 16] = [0xd2; 16];
    const CREATE_AUDIT_ID: [u8; 16] = [0xd3; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let mut setup_audit = identity_audit_event(0xd4, admin_role, id(0xd5));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(
            id(0xd5),
            admin_role,
            i64::MAX,
            &[
                principal_write_with_name(ACTOR_ID, 1, 1, "create_table_crash_actor"),
                object_write_with_name(ACTOR_ID, 8, "create_table_crash_actor"),
                membership_write(ACTOR_ID, admin_role),
                dependency_write(ACTOR_ID, admin_role, 5),
            ],
            &[setup_audit],
        )?;
        let schema_invocation = id(0xd6);
        let mut schema_audit = identity_audit_event(0xd7, ACTOR_ID, id(0xd8));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "create_table_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let before_create_sequence = commit_sequence(&database)?;
        let mut metadata = Vec::with_capacity(24);
        metadata.extend_from_slice(&schema.schema_id);
        metadata.extend_from_slice(&before_create_sequence.to_le_bytes());
        std::fs::write(
            database_path.with_extension("create-table-crash-metadata"),
            metadata,
        )?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let mut create_audit = identity_audit_event(CREATE_AUDIT_ID[0], ACTOR_ID, id(0xd9));
        create_audit.event_id = CREATE_AUDIT_ID;
        create_audit.correlation_id = CREATE_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown CREATE TABLE crash boundary {boundary}").into());
        }
        let create_result = storage.create_table_with_constraints(
            CreateTableRequest {
                invocation_id: CREATE_INVOCATION_ID,
                authenticated_actor_id: ACTOR_ID,
                schema_id: schema.schema_id,
                name: "crash_table".to_owned(),
                columns: vec![TableColumnDefinition {
                    name: "payload".to_owned(),
                    scalar_type: ScalarType::Text,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: None,
                }],
                retention_deadline_us: i64::MAX,
                audit_event: create_audit,
            },
            Some(&["payload".to_owned()]),
            &[],
        );
        if boundary == "write" {
            return Err(format!(
                "CREATE TABLE returned without injected backend-write exit: {create_result:?}"
            )
            .into());
        }
        create_result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for (boundary, expected_exit) in [
        ("write", CHILD_EXIT_CODE),
        ("after_commit", POST_COMMIT_EXIT_CODE),
    ] {
        let fixture = Fixture::new();
        let database_path = fixture.path();
        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_create_table_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "CREATE TABLE child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("create-table-crash-metadata"))?;
        assert_eq!(metadata.len(), 24);
        let schema_id: [u8; 16] = metadata[0..16].try_into()?;
        let before_create_sequence = u64::from_le_bytes(metadata[16..24].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let table_id = lookup_relation_id_by_name(&read, schema_id, "crash_table")?;
        let sequence = commit_sequence(&reopened)?;
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&CREATE_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&CREATE_AUDIT_ID[..])?
            .is_some();

        if boundary == "write" {
            assert_eq!(table_id, None);
            assert_eq!(sequence, before_create_sequence);
            assert_eq!(outcome, None);
            assert!(!audit_exists);
        } else {
            let table_id = table_id.ok_or("committed table name was not recovered")?;
            let column_id = lookup_column_id_by_name(&read, table_id, "payload")?
                .ok_or("committed table column name was not recovered")?;
            let constraint_ids =
                prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, table_id, 1)?;
            assert_eq!(constraint_ids.len(), 1);
            let index_ids =
                prdb_engine::catalog_index::scan_index_ids_for_table(&read, table_id, 1)?;
            assert_eq!(index_ids.len(), 1);
            let catalog = read.open_table(CATALOG)?;
            let table_record = catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("committed table record was not recovered")?;
            let column_record = catalog
                .get(id_catalog_key(6, column_id).as_slice())?
                .ok_or("committed column record was not recovered")?;
            let constraint_id = constraint_ids[0];
            let constraint_key = id_catalog_key(8, constraint_id);
            let constraint_record = catalog
                .get(constraint_key.as_slice())?
                .ok_or("committed PRIMARY KEY record was not recovered")?;
            let constraint_record = decode_registered_envelope(constraint_record.value())?;
            let constraint_kind = constraint_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("committed PRIMARY KEY kind was not recovered")?
                .payload
                .as_slice()
                .try_into()?;
            assert_eq!(u16::from_le_bytes(constraint_kind), 2);
            let index_id: [u8; 16] = constraint_record
                .fields
                .iter()
                .find(|field| field.tag == 11)
                .ok_or("committed PRIMARY KEY index ID was not recovered")?
                .payload
                .as_slice()
                .try_into()?;
            assert_eq!(index_id, index_ids[0]);
            assert!(
                catalog
                    .get(id_catalog_key(9, index_id).as_slice())?
                    .is_some(),
                "committed PRIMARY KEY index record was not recovered"
            );
            assert!(
                catalog
                    .get(dependency_write(constraint_id, index_id, 4).key.as_slice())?
                    .is_some(),
                "committed PRIMARY KEY index dependency was not recovered"
            );
            decode_registered_envelope(table_record.value())?;
            decode_registered_envelope(column_record.value())?;
            assert_eq!(sequence, before_create_sequence + 1);
            let outcome = outcome.ok_or("committed CREATE TABLE outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
            assert!(audit_exists);
        }
    }
    Ok(())
}
