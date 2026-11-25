use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::lookup_relation_id_by_name;
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::schema::{self, CATALOG};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadableDatabase};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::helpers::*;

pub(super) fn process_crash_during_unique_constraint_creation_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, CreateUniqueConstraintRequest, TableColumnDefinition,
    };
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_UNIQUE_CONSTRAINT_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_UNIQUE_CONSTRAINT_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xc1; 16];
    const CONSTRAINT_INVOCATION_ID: [u8; 16] = [0xc2; 16];
    const CONSTRAINT_AUDIT_ID: [u8; 16] = [0xc3; 16];
    const ROW_ID: [u8; 16] = [0xc4; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let mut setup_audit = identity_audit_event(0xc5, admin_role, id(0xc6));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(
            id(0xc6),
            admin_role,
            i64::MAX,
            &[
                principal_write_with_name(ACTOR_ID, 1, 1, "unique_crash_actor"),
                object_write_with_name(ACTOR_ID, 8, "unique_crash_actor"),
                membership_write(ACTOR_ID, admin_role),
                dependency_write(ACTOR_ID, admin_role, 5),
            ],
            &[setup_audit],
        )?;
        let schema_invocation = id(0xc7);
        let mut schema_audit = identity_audit_event(0xc8, ACTOR_ID, id(0xc9));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "unique_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let table_invocation = id(0xca);
        let mut table_audit = identity_audit_event(0xcb, ACTOR_ID, id(0xcc));
        table_audit.correlation_id = table_invocation;
        let table = storage.create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "unique_crash_table".to_owned(),
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
        let mut seed = storage.begin_invocation(id(0xcd), ACTOR_ID, i64::MAX, 1, 1024)?;
        let table_schema = seed.load_catalog_table_schema(table.table_id)?;
        seed.load_catalog_indexes(&table_schema)?;
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: table.table_id,
                row: ROW_ID,
            },
            &encode_tuple(
                &table_schema.columns,
                table_schema.schema_version,
                &[Some(ScalarValue::Integer(37))],
            )?,
        )?;
        seed.commit()?;
        let before_constraint_sequence = commit_sequence(&database)?;
        let mut metadata = Vec::with_capacity(80);
        metadata.extend_from_slice(&schema.schema_id);
        metadata.extend_from_slice(&table.table_id);
        metadata.extend_from_slice(&table.column_ids[0]);
        metadata.extend_from_slice(&ROW_ID);
        metadata.extend_from_slice(&table_schema.schema_version.to_le_bytes());
        metadata.extend_from_slice(&before_constraint_sequence.to_le_bytes());
        std::fs::write(
            database_path.with_extension("unique-constraint-crash-metadata"),
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
        let mut constraint_audit = identity_audit_event(CONSTRAINT_AUDIT_ID[0], ACTOR_ID, id(0xce));
        constraint_audit.event_id = CONSTRAINT_AUDIT_ID;
        constraint_audit.correlation_id = CONSTRAINT_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown UNIQUE crash boundary {boundary}").into());
        }
        let create_result = storage.create_unique_constraint(CreateUniqueConstraintRequest {
            invocation_id: CONSTRAINT_INVOCATION_ID,
            authenticated_actor_id: ACTOR_ID,
            table_id: table.table_id,
            column_ids: table.column_ids,
            retention_deadline_us: i64::MAX,
            audit_event: constraint_audit,
        });
        if boundary == "write" {
            return Err(format!(
                "UNIQUE creation returned without injected backend-write exit: {create_result:?}"
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
            .arg("process_crash_during_unique_constraint_creation_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "UNIQUE child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata =
            std::fs::read(database_path.with_extension("unique-constraint-crash-metadata"))?;
        assert_eq!(metadata.len(), 80);
        let schema_id: [u8; 16] = metadata[0..16].try_into()?;
        let table_id: [u8; 16] = metadata[16..32].try_into()?;
        let column_id: [u8; 16] = metadata[32..48].try_into()?;
        let row_id: [u8; 16] = metadata[48..64].try_into()?;
        let schema_version = u64::from_le_bytes(metadata[64..72].try_into()?);
        let before_constraint_sequence = u64::from_le_bytes(metadata[72..80].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let constraint_ids =
            prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, table_id, 3)?;
        let row_key = table_row_key(table_id, row_id);
        let row_payload = encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            }],
            schema_version,
            &[Some(ScalarValue::Integer(37))],
        )?;
        let row_record = row(&reopened, &row_key)?.ok_or("constraint seed row missing")?;
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&CONSTRAINT_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&CONSTRAINT_AUDIT_ID[..])?
            .is_some();
        let sequence = commit_sequence(&reopened)?;

        if boundary == "write" {
            assert!(constraint_ids.is_empty());
            assert_eq!(sequence, before_constraint_sequence);
            assert_eq!(outcome, None);
            assert!(!audit_exists);
            assert_eq!(row_record.payload, row_payload);
        } else {
            assert_eq!(constraint_ids.len(), 1);
            let constraint_id = constraint_ids[0];
            let catalog = read.open_table(CATALOG)?;
            let constraint_key = id_catalog_key(8, constraint_id);
            let constraint_value = catalog
                .get(constraint_key.as_slice())?
                .ok_or("UNIQUE constraint record missing")?;
            let constraint_record = decode_registered_envelope(constraint_value.value())?;
            assert_eq!(
                u16::from_le_bytes(
                    constraint_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 2)
                        .ok_or("UNIQUE constraint kind missing")?
                        .payload
                        .as_slice()
                        .try_into()?
                ),
                3
            );
            let index_id: [u8; 16] = constraint_record
                .fields
                .iter()
                .find(|field| field.tag == 11)
                .ok_or("UNIQUE backing index ID missing")?
                .payload
                .as_slice()
                .try_into()?;
            assert!(
                catalog
                    .get(id_catalog_key(9, index_id).as_slice())?
                    .is_some()
            );
            assert!(
                catalog
                    .get(id_catalog_key(2, constraint_id).as_slice())?
                    .is_some()
            );
            assert!(
                catalog
                    .get(id_catalog_key(2, index_id).as_slice())?
                    .is_some()
            );

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
            let locator = read
                .open_table(schema::INDEXES)?
                .get(index_key.key.as_slice())?
                .ok_or("UNIQUE backing index locator missing")?;
            assert_eq!(
                IndexLocator::decode_for_key(&index_key.key, locator.value())?.row_version,
                before_constraint_sequence
            );
            let mut reverse_key = row_key.to_vec();
            reverse_key.extend_from_slice(&index_key.key[..index_key.key.len() - 16]);
            assert!(
                read.open_table(schema::ROW_INDEXES)?
                    .get(reverse_key.as_slice())?
                    .is_some()
            );
            assert_eq!(row_record.payload, row_payload);
            assert_eq!(sequence, before_constraint_sequence + 1);
            let outcome = outcome.ok_or("committed UNIQUE outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
            assert!(audit_exists);
            assert_eq!(
                lookup_relation_id_by_name(&read, schema_id, "unique_crash_table")?,
                Some(table_id)
            );
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_check_constraint_creation_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::CreateCheckConstraintRequest;
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_CHECK_CONSTRAINT_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_CHECK_CONSTRAINT_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xc7; 16];
    const CONSTRAINT_INVOCATION_ID: [u8; 16] = [0xc8; 16];
    const CONSTRAINT_AUDIT_ID: [u8; 16] = [0xc9; 16];
    const ROW_ID: [u8; 16] = [0xca; 16];
    const TABLE_ID: [u8; 16] = [82; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let metadata = std::fs::read(database_path.with_extension("check-constraint-crash-meta"))?;
        if metadata.len() != 56 {
            return Err(format!(
                "invalid CHECK constraint crash metadata length: {}",
                metadata.len()
            )
            .into());
        }
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
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
        let mut audit = identity_audit_event(CONSTRAINT_AUDIT_ID[0], ACTOR_ID, TABLE_ID);
        audit.event_id = CONSTRAINT_AUDIT_ID;
        audit.correlation_id = CONSTRAINT_INVOCATION_ID;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown CHECK constraint crash boundary {boundary}").into());
        }
        let create_result = storage.create_check_constraint(CreateCheckConstraintRequest {
            invocation_id: CONSTRAINT_INVOCATION_ID,
            authenticated_actor_id: ACTOR_ID,
            table_id: TABLE_ID,
            name: None,
            expression: prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(true))),
            dependency_column_ids: vec![],
            retention_deadline_us: i64::MAX,
            audit_event: audit,
        });
        if boundary == "write" {
            return Err(format!(
                "CHECK constraint creation returned without injected backend-write exit: {create_result:?}"
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
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let mut setup_writes = foreign_key_catalog_writes(false, true)
            .into_iter()
            .filter(|write| {
                write.key != id_catalog_key(8, id(86))
                    && write.key != id_catalog_key(2, id(86))
                    && write.key != dependency_write(id(86), id(85), 3).key
            })
            .collect::<Vec<_>>();
        setup_writes.extend([
            principal_write_with_name(ACTOR_ID, 1, 1, "check_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "check_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ]);
        let mut setup_audit = identity_audit_event(0xcb, admin_role, id(0xcc));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(
            id(0xcc),
            admin_role,
            i64::MAX,
            &setup_writes,
            &[setup_audit],
        )?;

        let mut seed = storage.begin_invocation(id(0xcd), ACTOR_ID, i64::MAX, 1, 1024)?;
        let table_schema = seed.load_catalog_table_schema(TABLE_ID)?;
        seed.load_catalog_indexes(&table_schema)?;
        let row_payload = encode_tuple(
            &table_schema.columns,
            table_schema.schema_version,
            &[Some(ScalarValue::Bytea(b"CHECK crash row".to_vec()))],
        )?;
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: TABLE_ID,
                row: ROW_ID,
            },
            &row_payload,
        )?;
        seed.commit()?;
        let before_sequence = commit_sequence(&database)?;
        let before_epoch = {
            let read = database.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let table = catalog
                .get(id_catalog_key(5, TABLE_ID).as_slice())?
                .ok_or("CHECK crash table missing before DDL")?;
            let epoch = decode_registered_envelope(table.value())?
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("CHECK crash table epoch missing before DDL")?
                .payload
                .as_slice()
                .try_into()?;
            u64::from_le_bytes(epoch)
        };
        let mut metadata = Vec::with_capacity(56);
        metadata.extend_from_slice(&TABLE_ID);
        metadata.extend_from_slice(&ROW_ID);
        metadata.extend_from_slice(&table_schema.schema_version.to_le_bytes());
        metadata.extend_from_slice(&before_sequence.to_le_bytes());
        metadata.extend_from_slice(&before_epoch.to_le_bytes());
        std::fs::write(
            database_path.with_extension("check-constraint-crash-meta"),
            metadata,
        )?;
        drop(storage);
        drop(database);

        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_check_constraint_creation_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "CHECK constraint child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("check-constraint-crash-meta"))?;
        assert_eq!(metadata.len(), 56);
        let table_id: [u8; 16] = metadata[0..16].try_into()?;
        let row_id: [u8; 16] = metadata[16..32].try_into()?;
        let schema_version = u64::from_le_bytes(metadata[32..40].try_into()?);
        let before_sequence = u64::from_le_bytes(metadata[40..48].try_into()?);
        let before_epoch = u64::from_le_bytes(metadata[48..56].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let constraint_ids =
            prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, table_id, 4)?;
        let catalog = read.open_table(CATALOG)?;
        let row_payload = encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            }],
            schema_version,
            &[Some(ScalarValue::Bytea(b"CHECK crash row".to_vec()))],
        )?;
        assert_eq!(
            row(&reopened, &table_row_key(table_id, row_id))?
                .ok_or("CHECK crash seed row missing")?
                .payload,
            row_payload
        );
        let sequence = commit_sequence(&reopened)?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&CONSTRAINT_AUDIT_ID[..])?
            .is_some();
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&CONSTRAINT_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let table = catalog
            .get(id_catalog_key(5, table_id).as_slice())?
            .ok_or("CHECK crash table missing after reopen")?;
        let epoch = u64::from_le_bytes(
            decode_registered_envelope(table.value())?
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("CHECK crash table epoch missing after reopen")?
                .payload
                .as_slice()
                .try_into()?,
        );

        if boundary == "write" {
            assert!(constraint_ids.is_empty());
            assert_eq!(epoch, before_epoch);
            assert_eq!(sequence, before_sequence);
            assert_eq!(outcome, None);
            assert!(!audit_exists);
        } else {
            assert_eq!(constraint_ids.len(), 1);
            let constraint_id = constraint_ids[0];
            let constraint_record = decode_registered_envelope(
                catalog
                    .get(id_catalog_key(8, constraint_id).as_slice())?
                    .ok_or("CHECK constraint record missing after commit")?
                    .value(),
            )?;
            assert_eq!(
                u16::from_le_bytes(
                    constraint_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 2)
                        .ok_or("CHECK constraint kind missing")?
                        .payload
                        .as_slice()
                        .try_into()?
                ),
                4
            );
            let expression_id: [u8; 16] = constraint_record
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("CHECK expression ID missing")?
                .payload
                .as_slice()
                .try_into()?;
            for key in [
                id_catalog_key(2, constraint_id),
                id_catalog_key(7, expression_id),
                dependency_write(constraint_id, expression_id, 2).key,
            ] {
                assert!(catalog.get(key.as_slice())?.is_some());
            }
            assert_eq!(epoch, before_epoch + 1);
            assert_eq!(sequence, before_sequence + 1);
            assert!(audit_exists);
            let outcome = outcome.ok_or("committed CHECK outcome missing")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
        }
        drop(catalog);
        drop(read);
        prdb_engine::startup::validate_persisted_records(&reopened)?;
    }
    Ok(())
}

pub(super) fn process_crash_during_check_constraint_drop_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::CreateCheckConstraintRequest;
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_CHECK_DROP_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_CHECK_DROP_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xd7; 16];
    const DROP_INVOCATION_ID: [u8; 16] = [0xd8; 16];
    const DROP_AUDIT_ID: [u8; 16] = [0xd9; 16];
    const ROW_ID: [u8; 16] = [0xda; 16];
    const TABLE_ID: [u8; 16] = [82; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let metadata = std::fs::read(database_path.with_extension("check-drop-crash-meta"))?;
        if metadata.len() != 88 {
            return Err(format!("invalid CHECK drop metadata length: {}", metadata.len()).into());
        }
        let constraint_id: [u8; 16] = metadata[32..48].try_into()?;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
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
        let mut audit = identity_audit_event(DROP_AUDIT_ID[0], ACTOR_ID, constraint_id);
        audit.event_id = DROP_AUDIT_ID;
        audit.correlation_id = DROP_INVOCATION_ID;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown CHECK drop crash boundary {boundary}").into());
        }
        let result = storage.drop_check_constraint(
            DROP_INVOCATION_ID,
            ACTOR_ID,
            constraint_id,
            i64::MAX,
            audit,
        );
        if boundary == "write" {
            return Err(format!(
                "CHECK drop returned without injected backend-write exit: {result:?}"
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
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let mut setup_writes = foreign_key_catalog_writes(false, true)
            .into_iter()
            .filter(|write| {
                write.key != id_catalog_key(8, id(86))
                    && write.key != id_catalog_key(2, id(86))
                    && write.key != dependency_write(id(86), id(85), 3).key
            })
            .collect::<Vec<_>>();
        setup_writes.extend([
            principal_write_with_name(ACTOR_ID, 1, 1, "check_drop_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "check_drop_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ]);
        let mut setup_audit = identity_audit_event(0xdb, admin_role, id(0xdc));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(
            id(0xdc),
            admin_role,
            i64::MAX,
            &setup_writes,
            &[setup_audit],
        )?;

        let mut seed = storage.begin_invocation(id(0xdd), ACTOR_ID, i64::MAX, 1, 1024)?;
        let table_schema = seed.load_catalog_table_schema(TABLE_ID)?;
        seed.load_catalog_indexes(&table_schema)?;
        let row_payload = encode_tuple(
            &table_schema.columns,
            table_schema.schema_version,
            &[Some(ScalarValue::Bytea(b"CHECK drop crash row".to_vec()))],
        )?;
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: TABLE_ID,
                row: ROW_ID,
            },
            &row_payload,
        )?;
        seed.commit()?;
        let mut create_audit = identity_audit_event(0xde, ACTOR_ID, TABLE_ID);
        create_audit.correlation_id = id(0xdf);
        let created = storage.create_check_constraint(CreateCheckConstraintRequest {
            invocation_id: id(0xdf),
            authenticated_actor_id: ACTOR_ID,
            table_id: TABLE_ID,
            name: None,
            expression: prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(true))),
            dependency_column_ids: vec![],
            retention_deadline_us: i64::MAX,
            audit_event: create_audit,
        })?;
        let before_sequence = commit_sequence(&database)?;
        let before_epoch = {
            let read = database.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let table = catalog
                .get(id_catalog_key(5, TABLE_ID).as_slice())?
                .ok_or("CHECK drop table missing before DDL")?;
            let epoch = decode_registered_envelope(table.value())?
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("CHECK drop table epoch missing before DDL")?
                .payload
                .as_slice()
                .try_into()?;
            u64::from_le_bytes(epoch)
        };
        let schema_version = table_schema.schema_version;
        let mut metadata = Vec::with_capacity(88);
        metadata.extend_from_slice(&TABLE_ID);
        metadata.extend_from_slice(&ROW_ID);
        metadata.extend_from_slice(&created.constraint_id);
        metadata.extend_from_slice(&created.expression_id);
        metadata.extend_from_slice(&schema_version.to_le_bytes());
        metadata.extend_from_slice(&before_sequence.to_le_bytes());
        metadata.extend_from_slice(&before_epoch.to_le_bytes());
        std::fs::write(
            database_path.with_extension("check-drop-crash-meta"),
            metadata,
        )?;
        drop(storage);
        drop(database);

        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_check_constraint_drop_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "CHECK drop child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("check-drop-crash-meta"))?;
        assert_eq!(metadata.len(), 88);
        let table_id: [u8; 16] = metadata[0..16].try_into()?;
        let row_id: [u8; 16] = metadata[16..32].try_into()?;
        let constraint_id: [u8; 16] = metadata[32..48].try_into()?;
        let expression_id: [u8; 16] = metadata[48..64].try_into()?;
        let schema_version = u64::from_le_bytes(metadata[64..72].try_into()?);
        let before_sequence = u64::from_le_bytes(metadata[72..80].try_into()?);
        let before_epoch = u64::from_le_bytes(metadata[80..88].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let constraint_ids =
            prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, table_id, 4)?;
        let committed = boundary == "after_commit";
        assert_eq!(constraint_ids.contains(&constraint_id), !committed);
        for key in [
            id_catalog_key(8, constraint_id),
            id_catalog_key(2, constraint_id),
            id_catalog_key(7, expression_id),
            dependency_write(constraint_id, expression_id, 2).key,
        ] {
            assert_eq!(catalog.get(key.as_slice())?.is_some(), !committed);
        }
        let table = catalog
            .get(id_catalog_key(5, table_id).as_slice())?
            .ok_or("CHECK drop table missing after reopen")?;
        let epoch = u64::from_le_bytes(
            decode_registered_envelope(table.value())?
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("CHECK drop table epoch missing after reopen")?
                .payload
                .as_slice()
                .try_into()?,
        );
        assert_eq!(epoch, before_epoch + u64::from(committed));
        let sequence = commit_sequence(&reopened)?;
        assert_eq!(sequence, before_sequence + u64::from(committed));
        assert_eq!(
            row(&reopened, &table_row_key(table_id, row_id))?
                .ok_or("CHECK drop row missing after reopen")?
                .payload,
            encode_tuple(
                &[ColumnSchema {
                    scalar_type: ScalarType::Bytea,
                    nullable: true,
                }],
                schema_version,
                &[Some(ScalarValue::Bytea(b"CHECK drop crash row".to_vec()))],
            )?
        );
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&DROP_AUDIT_ID[..])?
            .is_some();
        assert_eq!(audit_exists, committed);
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&DROP_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        if committed {
            let outcome = outcome.ok_or("committed CHECK drop outcome missing")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
        } else {
            assert_eq!(outcome, None);
        }
        drop(catalog);
        drop(read);
        prdb_engine::startup::validate_persisted_records(&reopened)?;
    }
    Ok(())
}

pub(super) fn process_crash_during_column_default_change_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AlterColumnDefaultRequest, CreateTableRequest, TableColumnDefault, TableColumnDefinition,
    };
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_DEFAULT_REPLACEMENT_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_DEFAULT_REPLACEMENT_BOUNDARY";
    const CHILD_OPERATION_ENV: &str = "PRDB_TEST_CRASH_DURING_DEFAULT_REPLACEMENT_OPERATION";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xe1; 16];
    const INVOCATION_ID: [u8; 16] = [0xe2; 16];
    const AUDIT_ID: [u8; 16] = [0xe3; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let metadata = std::fs::read(database_path.with_extension("default-replace-meta"))?;
        if metadata.len() != 64 {
            return Err(format!(
                "invalid default replacement metadata length: {}",
                metadata.len()
            )
            .into());
        }
        let table_id: [u8; 16] = metadata[0..16].try_into()?;
        let column_id: [u8; 16] = metadata[16..32].try_into()?;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        let operation = std::env::var(CHILD_OPERATION_ENV)?;
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
        let mut audit = identity_audit_event(AUDIT_ID[0], ACTOR_ID, column_id);
        audit.event_id = AUDIT_ID;
        audit.correlation_id = INVOCATION_ID;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown default replacement crash boundary {boundary}").into());
        }
        let default = match operation.as_str() {
            "replace" => Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(43)))),
            "principal" => Some(TableColumnDefault::CurrentPrincipalId),
            "drop" => None,
            _ => return Err(format!("unknown default operation {operation}").into()),
        };
        let result = storage.alter_column_default(AlterColumnDefaultRequest {
            invocation_id: INVOCATION_ID,
            authenticated_actor_id: ACTOR_ID,
            table_id,
            column_id,
            default,
            retention_deadline_us: i64::MAX,
            audit_event: audit,
        });
        if boundary == "write" {
            return Err(format!(
                "default replacement returned without injected backend-write exit: {result:?}"
            )
            .into());
        }
        result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for operation in ["replace", "principal", "drop"] {
        for (boundary, expected_exit) in [
            ("write", CHILD_EXIT_CODE),
            ("after_commit", POST_COMMIT_EXIT_CODE),
        ] {
            let fixture = Fixture::new();
            let database_path = fixture.path();
            let database = initialize(&database_path)?;
            let storage = StorageManager::new(&database, Duration::from_millis(100))?;
            let admin_role = builtin_owner_id();
            let mut setup_audit = identity_audit_event(0xe4, admin_role, id(0xe5));
            setup_audit.action = AuditAction::AuthorityChange;
            storage.commit_catalog_and_audit(
                id(0xe5),
                admin_role,
                i64::MAX,
                &[
                    principal_write_with_name(ACTOR_ID, 1, 1, "default_recovery_actor"),
                    object_write_with_name(ACTOR_ID, 8, "default_recovery_actor"),
                    membership_write(ACTOR_ID, admin_role),
                    dependency_write(ACTOR_ID, admin_role, 5),
                ],
                &[setup_audit],
            )?;
            let schema_invocation = id(0xe6);
            let mut schema_audit = identity_audit_event(0xe7, ACTOR_ID, id(0xe8));
            schema_audit.correlation_id = schema_invocation;
            let schema = storage.create_schema(
                schema_invocation,
                ACTOR_ID,
                "default_recovery_schema",
                ACTOR_ID,
                i64::MAX,
                schema_audit,
            )?;
            let table_invocation = id(0xe9);
            let mut table_audit = identity_audit_event(0xea, ACTOR_ID, id(0xeb));
            table_audit.correlation_id = table_invocation;
            let (column_type, initial_default) = if operation == "principal" {
                (
                    ScalarType::Uuid,
                    TableColumnDefault::Literal(Some(ScalarValue::Uuid([0x42; 16]))),
                )
            } else {
                (
                    ScalarType::Integer,
                    TableColumnDefault::Literal(Some(ScalarValue::Integer(42))),
                )
            };
            let table = storage.create_table(CreateTableRequest {
                invocation_id: table_invocation,
                authenticated_actor_id: ACTOR_ID,
                schema_id: schema.schema_id,
                name: "defaults".to_owned(),
                columns: vec![TableColumnDefinition {
                    name: "value".to_owned(),
                    scalar_type: column_type,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: Some(initial_default),
                }],
                retention_deadline_us: i64::MAX,
                audit_event: table_audit,
            })?;
            let before_sequence = commit_sequence(&database)?;
            let (old_default_id, before_epoch) = {
                let read = database.begin_read()?;
                let catalog = read.open_table(CATALOG)?;
                let column = catalog
                    .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
                    .ok_or("default recovery column missing")?;
                let fields = decode_registered_envelope(column.value())?;
                let old_default_id: [u8; 16] = fields
                    .fields
                    .iter()
                    .find(|field| field.tag == 7)
                    .ok_or("initial default expression ID missing")?
                    .payload
                    .as_slice()
                    .try_into()?;
                let table_record = catalog
                    .get(id_catalog_key(5, table.table_id).as_slice())?
                    .ok_or("default recovery table missing")?;
                let epoch = u64::from_le_bytes(
                    decode_registered_envelope(table_record.value())?
                        .fields
                        .iter()
                        .find(|field| field.tag == 3)
                        .ok_or("default recovery table epoch missing")?
                        .payload
                        .as_slice()
                        .try_into()?,
                );
                (old_default_id, epoch)
            };
            let mut metadata = Vec::with_capacity(64);
            metadata.extend_from_slice(&table.table_id);
            metadata.extend_from_slice(&table.column_ids[0]);
            metadata.extend_from_slice(&old_default_id);
            metadata.extend_from_slice(&before_epoch.to_le_bytes());
            metadata.extend_from_slice(&before_sequence.to_le_bytes());
            std::fs::write(
                database_path.with_extension("default-replace-meta"),
                metadata,
            )?;
            drop(storage);
            drop(database);

            let child_status = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("process_crash_during_column_default_change_recovers_all_or_none")
                .arg("--nocapture")
                .env(CHILD_DATABASE_ENV, &database_path)
                .env(CHILD_BOUNDARY_ENV, boundary)
                .env(CHILD_OPERATION_ENV, operation)
                .status()?;
            assert_eq!(
                child_status.code(),
                Some(expected_exit),
                "default {operation} child missed the {boundary} crash boundary: {child_status}"
            );

            let metadata = std::fs::read(database_path.with_extension("default-replace-meta"))?;
            let table_id: [u8; 16] = metadata[0..16].try_into()?;
            let column_id: [u8; 16] = metadata[16..32].try_into()?;
            let old_default_id: [u8; 16] = metadata[32..48].try_into()?;
            let before_epoch = u64::from_le_bytes(metadata[48..56].try_into()?);
            let before_sequence = u64::from_le_bytes(metadata[56..64].try_into()?);
            let committed = boundary == "after_commit";
            let reopened = Database::open(&database_path)?;
            let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
            let read = reopened.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let column = catalog
                .get(id_catalog_key(6, column_id).as_slice())?
                .ok_or("default recovery column missing after reopen")?;
            let current_default_id: Option<[u8; 16]> = decode_registered_envelope(column.value())?
                .fields
                .iter()
                .find(|field| field.tag == 7)
                .map(|field| field.payload.as_slice().try_into())
                .transpose()?;
            match (operation, committed, current_default_id) {
                ("replace", true, Some(current_id)) | ("principal", true, Some(current_id)) => {
                    assert_ne!(current_id, old_default_id);
                }
                ("replace", false, Some(current_id))
                | ("principal", false, Some(current_id))
                | ("drop", false, Some(current_id)) => {
                    assert_eq!(current_id, old_default_id);
                }
                ("drop", true, None) => {}
                _ => {
                    return Err("recovered default reference does not match operation state".into());
                }
            }
            let old_expression_key = id_catalog_key(7, old_default_id);
            let old_object_key = id_catalog_key(2, old_default_id);
            let old_dependency_key = dependency_write(column_id, old_default_id, 2).key;
            for key in [old_expression_key, old_object_key, old_dependency_key] {
                assert_eq!(catalog.get(key.as_slice())?.is_some(), !committed);
            }
            if let Some(current_default_id) = current_default_id {
                let current_expression_key = id_catalog_key(7, current_default_id);
                let current_object_key = id_catalog_key(2, current_default_id);
                let current_dependency_key = dependency_write(column_id, current_default_id, 2).key;
                for key in [
                    &current_expression_key,
                    &current_object_key,
                    &current_dependency_key,
                ] {
                    assert!(catalog.get(key.as_slice())?.is_some());
                }
                if operation == "principal" && committed {
                    let expression = catalog
                        .get(current_expression_key.as_slice())?
                        .ok_or("committed principal default expression missing")?;
                    let decoded =
                        prdb_engine::expression::decode_catalog_expression(expression.value())?;
                    assert_eq!(
                        decoded.purpose,
                        prdb_engine::expression::ExpressionPurpose::Default
                    );
                    assert_eq!(
                        decoded.expression,
                        prdb_engine::authorization::Expr::CurrentPrincipalId
                    );
                }
            }
            let table = catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("default recovery table missing after reopen")?;
            let epoch = u64::from_le_bytes(
                decode_registered_envelope(table.value())?
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("default recovery epoch missing after reopen")?
                    .payload
                    .as_slice()
                    .try_into()?,
            );
            assert_eq!(epoch, before_epoch + u64::from(committed));
            let sequence = commit_sequence(&reopened)?;
            assert_eq!(sequence, before_sequence + u64::from(committed));
            let audit_exists = read
                .open_table(schema::AUDIT)?
                .get(&AUDIT_ID[..])?
                .is_some();
            assert_eq!(audit_exists, committed);
            let outcome = read
                .open_table(schema::OUTCOMES)?
                .get(&INVOCATION_ID[..])?
                .map(|value| InvocationOutcome::decode(value.value()))
                .transpose()?;
            if committed {
                let outcome = outcome.ok_or("committed default replacement outcome missing")?;
                assert_eq!(outcome.status, OutcomeStatus::Committed);
                assert_eq!(outcome.commit_sequence, sequence);
            } else {
                assert_eq!(outcome, None);
            }
            drop(catalog);
            drop(read);
            prdb_engine::startup::validate_persisted_records(&reopened)?;
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_not_null_change_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_NOT_NULL_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_NOT_NULL_BOUNDARY";
    const CHILD_OPERATION_ENV: &str = "PRDB_TEST_CRASH_DURING_NOT_NULL_OPERATION";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xe6; 16];
    const TABLE_ID: [u8; 16] = [82; 16];
    const COLUMN_ID: [u8; 16] = [84; 16];
    const INVOCATION_ID: [u8; 16] = [0xe7; 16];
    const AUDIT_ID: [u8; 16] = [0xe8; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        let operation = std::env::var(CHILD_OPERATION_ENV)?;
        let nullable = match operation.as_str() {
            "set" => false,
            "drop" => true,
            _ => return Err(format!("unknown NOT NULL operation {operation}").into()),
        };
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
        let mut audit = identity_audit_event(AUDIT_ID[0], ACTOR_ID, COLUMN_ID);
        audit.event_id = AUDIT_ID;
        audit.correlation_id = INVOCATION_ID;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown NOT NULL crash boundary {boundary}").into());
        }
        let result = storage.set_column_nullable(
            INVOCATION_ID,
            ACTOR_ID,
            TABLE_ID,
            COLUMN_ID,
            nullable,
            i64::MAX,
            audit,
        );
        if boundary == "write" {
            return Err(format!(
                "NOT NULL change returned without injected backend-write exit: {result:?}"
            )
            .into());
        }
        result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for operation in ["set", "drop"] {
        for (boundary, expected_exit) in [
            ("write", CHILD_EXIT_CODE),
            ("after_commit", POST_COMMIT_EXIT_CODE),
        ] {
            let fixture = Fixture::new();
            let database_path = fixture.path();
            let database = initialize(&database_path)?;
            let storage = StorageManager::new(&database, Duration::from_millis(100))?;
            let admin_role = builtin_owner_id();
            let mut setup_writes = foreign_key_catalog_writes(false, true)
                .into_iter()
                .filter(|write| {
                    write.key != id_catalog_key(8, id(86))
                        && write.key != id_catalog_key(2, id(86))
                        && write.key != dependency_write(id(86), id(85), 3).key
                })
                .collect::<Vec<_>>();
            setup_writes.extend([
                principal_write_with_name(ACTOR_ID, 1, 1, "not_null_crash_actor"),
                object_write_with_name(ACTOR_ID, 8, "not_null_crash_actor"),
                membership_write(ACTOR_ID, admin_role),
                dependency_write(ACTOR_ID, admin_role, 5),
            ]);
            let mut setup_audit = identity_audit_event(0xe9, admin_role, id(0xea));
            setup_audit.action = AuditAction::AuthorityChange;
            storage.commit_catalog_and_audit(
                id(0xea),
                admin_role,
                i64::MAX,
                &setup_writes,
                &[setup_audit],
            )?;
            if operation == "drop" {
                let mut seed_audit = identity_audit_event(0xeb, ACTOR_ID, COLUMN_ID);
                seed_audit.correlation_id = id(0xec);
                storage.set_column_nullable(
                    id(0xec),
                    ACTOR_ID,
                    TABLE_ID,
                    COLUMN_ID,
                    false,
                    i64::MAX,
                    seed_audit,
                )?;
            }
            let before_sequence = commit_sequence(&database)?;
            let before_epoch = {
                let read = database.begin_read()?;
                let catalog = read.open_table(CATALOG)?;
                let table = catalog
                    .get(id_catalog_key(5, TABLE_ID).as_slice())?
                    .ok_or("NOT NULL table missing before DDL")?;
                u64::from_le_bytes(
                    decode_registered_envelope(table.value())?
                        .fields
                        .iter()
                        .find(|field| field.tag == 3)
                        .ok_or("NOT NULL table epoch missing before DDL")?
                        .payload
                        .as_slice()
                        .try_into()?,
                )
            };
            drop(storage);
            drop(database);

            let child_status = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("process_crash_during_not_null_change_recovers_all_or_none")
                .arg("--nocapture")
                .env(CHILD_DATABASE_ENV, &database_path)
                .env(CHILD_BOUNDARY_ENV, boundary)
                .env(CHILD_OPERATION_ENV, operation)
                .status()?;
            assert_eq!(
                child_status.code(),
                Some(expected_exit),
                "NOT NULL {operation} child missed the {boundary} crash boundary: {child_status}"
            );

            let reopened = Database::open(&database_path)?;
            let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
            let read = reopened.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let column = catalog
                .get(id_catalog_key(6, COLUMN_ID).as_slice())?
                .ok_or("NOT NULL column missing after reopen")?;
            let column_record = decode_registered_envelope(column.value())?;
            let nullable_field = column_record
                .fields
                .iter()
                .find(|field| field.tag == 6)
                .ok_or("NOT NULL nullable flag missing after reopen")?
                .payload
                .as_slice();
            let nullable = match nullable_field {
                [0] => false,
                [1] => true,
                _ => return Err("invalid recovered nullable flag".into()),
            };
            let committed = boundary == "after_commit";
            let expected_nullable = if operation == "set" {
                !committed
            } else {
                committed
            };
            assert_eq!(nullable, expected_nullable);
            let constraint_ids =
                prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, TABLE_ID, 1)?;
            assert_eq!(constraint_ids.len(), usize::from(!expected_nullable));
            if let Some(constraint_id) = constraint_ids.first() {
                assert!(
                    catalog
                        .get(id_catalog_key(2, *constraint_id).as_slice())?
                        .is_some()
                );
            }
            let table = catalog
                .get(id_catalog_key(5, TABLE_ID).as_slice())?
                .ok_or("NOT NULL table missing after reopen")?;
            let epoch = u64::from_le_bytes(
                decode_registered_envelope(table.value())?
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("NOT NULL table epoch missing after reopen")?
                    .payload
                    .as_slice()
                    .try_into()?,
            );
            assert_eq!(epoch, before_epoch + u64::from(committed));
            let sequence = commit_sequence(&reopened)?;
            assert_eq!(sequence, before_sequence + u64::from(committed));
            let audit_exists = read
                .open_table(schema::AUDIT)?
                .get(&AUDIT_ID[..])?
                .is_some();
            assert_eq!(audit_exists, committed);
            let outcome = read
                .open_table(schema::OUTCOMES)?
                .get(&INVOCATION_ID[..])?
                .map(|value| InvocationOutcome::decode(value.value()))
                .transpose()?;
            if committed {
                let outcome = outcome.ok_or("committed NOT NULL outcome missing")?;
                assert_eq!(outcome.status, OutcomeStatus::Committed);
                assert_eq!(outcome.commit_sequence, sequence);
            } else {
                assert_eq!(outcome, None);
            }
            drop(catalog);
            drop(read);
            prdb_engine::startup::validate_persisted_records(&reopened)?;
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_primary_key_creation_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreatePrimaryKeyRequest, CreateTableRequest, TableColumnDefinition,
    };
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_PRIMARY_KEY_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_PRIMARY_KEY_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xc1; 16];
    const CONSTRAINT_INVOCATION_ID: [u8; 16] = [0xc2; 16];
    const CONSTRAINT_AUDIT_ID: [u8; 16] = [0xc3; 16];
    const ROW_ID: [u8; 16] = [0xc4; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let mut setup_audit = identity_audit_event(0xc5, admin_role, id(0xc6));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(
            id(0xc6),
            admin_role,
            i64::MAX,
            &[
                principal_write_with_name(ACTOR_ID, 1, 1, "primary_key_crash_actor"),
                object_write_with_name(ACTOR_ID, 8, "primary_key_crash_actor"),
                membership_write(ACTOR_ID, admin_role),
                dependency_write(ACTOR_ID, admin_role, 5),
            ],
            &[setup_audit],
        )?;
        let schema_invocation = id(0xc7);
        let mut schema_audit = identity_audit_event(0xc8, ACTOR_ID, id(0xc9));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "primary_key_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let table_invocation = id(0xca);
        let mut table_audit = identity_audit_event(0xcb, ACTOR_ID, id(0xcc));
        table_audit.correlation_id = table_invocation;
        let table = storage.create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "primary_key_crash_table".to_owned(),
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
        let mut seed = storage.begin_invocation(id(0xcd), ACTOR_ID, i64::MAX, 1, 1024)?;
        let table_schema = seed.load_catalog_table_schema(table.table_id)?;
        seed.load_catalog_indexes(&table_schema)?;
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: table.table_id,
                row: ROW_ID,
            },
            &encode_tuple(
                &table_schema.columns,
                table_schema.schema_version,
                &[Some(ScalarValue::Integer(37))],
            )?,
        )?;
        seed.commit()?;
        let before_constraint_sequence = commit_sequence(&database)?;
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_record = catalog
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("PRIMARY KEY table record missing before publication")?;
        let before_table_epoch = u64::from_le_bytes(
            decode_registered_envelope(table_record.value())?
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("PRIMARY KEY table epoch missing before publication")?
                .payload
                .as_slice()
                .try_into()?,
        );
        drop(catalog);
        drop(read);
        let mut metadata = Vec::with_capacity(88);
        metadata.extend_from_slice(&schema.schema_id);
        metadata.extend_from_slice(&table.table_id);
        metadata.extend_from_slice(&table.column_ids[0]);
        metadata.extend_from_slice(&ROW_ID);
        metadata.extend_from_slice(&table_schema.schema_version.to_le_bytes());
        metadata.extend_from_slice(&before_constraint_sequence.to_le_bytes());
        metadata.extend_from_slice(&before_table_epoch.to_le_bytes());
        std::fs::write(
            database_path.with_extension("primary-key-crash-metadata"),
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
        let mut constraint_audit = identity_audit_event(CONSTRAINT_AUDIT_ID[0], ACTOR_ID, id(0xce));
        constraint_audit.event_id = CONSTRAINT_AUDIT_ID;
        constraint_audit.correlation_id = CONSTRAINT_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown PRIMARY KEY crash boundary {boundary}").into());
        }
        let create_result = storage.create_primary_key(CreatePrimaryKeyRequest {
            invocation_id: CONSTRAINT_INVOCATION_ID,
            authenticated_actor_id: ACTOR_ID,
            table_id: table.table_id,
            column_ids: table.column_ids,
            retention_deadline_us: i64::MAX,
            audit_event: constraint_audit,
        });
        if boundary == "write" {
            return Err(format!(
                "PRIMARY KEY creation returned without injected backend-write exit: {create_result:?}"
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
            .arg("process_crash_during_primary_key_creation_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "PRIMARY KEY child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("primary-key-crash-metadata"))?;
        assert_eq!(metadata.len(), 88);
        let schema_id: [u8; 16] = metadata[0..16].try_into()?;
        let table_id: [u8; 16] = metadata[16..32].try_into()?;
        let column_id: [u8; 16] = metadata[32..48].try_into()?;
        let row_id: [u8; 16] = metadata[48..64].try_into()?;
        let schema_version = u64::from_le_bytes(metadata[64..72].try_into()?);
        let before_constraint_sequence = u64::from_le_bytes(metadata[72..80].try_into()?);
        let before_table_epoch = u64::from_le_bytes(metadata[80..88].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let constraint_ids =
            prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, table_id, 2)?;
        let row_key = table_row_key(table_id, row_id);
        let row_payload = encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            }],
            schema_version,
            &[Some(ScalarValue::Integer(37))],
        )?;
        let row_record = row(&reopened, &row_key)?.ok_or("constraint seed row missing")?;
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&CONSTRAINT_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&CONSTRAINT_AUDIT_ID[..])?
            .is_some();
        let sequence = commit_sequence(&reopened)?;
        let catalog = read.open_table(CATALOG)?;
        let current_table_record = catalog
            .get(id_catalog_key(5, table_id).as_slice())?
            .ok_or("PRIMARY KEY table record missing after reopen")?;
        let current_table_epoch = u64::from_le_bytes(
            decode_registered_envelope(current_table_record.value())?
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("PRIMARY KEY table epoch missing after reopen")?
                .payload
                .as_slice()
                .try_into()?,
        );

        if boundary == "write" {
            assert!(constraint_ids.is_empty());
            assert_eq!(current_table_epoch, before_table_epoch);
            assert_eq!(sequence, before_constraint_sequence);
            assert_eq!(outcome, None);
            assert!(!audit_exists);
            assert_eq!(row_record.payload, row_payload);
        } else {
            assert_eq!(constraint_ids.len(), 1);
            assert_eq!(current_table_epoch, before_table_epoch + 1);
            let constraint_id = constraint_ids[0];
            let catalog = read.open_table(CATALOG)?;
            let constraint_key = id_catalog_key(8, constraint_id);
            let constraint_value = catalog
                .get(constraint_key.as_slice())?
                .ok_or("PRIMARY KEY constraint record missing")?;
            let constraint_record = decode_registered_envelope(constraint_value.value())?;
            assert_eq!(
                u16::from_le_bytes(
                    constraint_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 2)
                        .ok_or("PRIMARY KEY constraint kind missing")?
                        .payload
                        .as_slice()
                        .try_into()?
                ),
                2
            );
            let index_id: [u8; 16] = constraint_record
                .fields
                .iter()
                .find(|field| field.tag == 11)
                .ok_or("PRIMARY KEY backing index ID missing")?
                .payload
                .as_slice()
                .try_into()?;
            assert!(
                catalog
                    .get(id_catalog_key(9, index_id).as_slice())?
                    .is_some()
            );
            assert!(
                catalog
                    .get(id_catalog_key(2, constraint_id).as_slice())?
                    .is_some()
            );
            assert!(
                catalog
                    .get(id_catalog_key(2, index_id).as_slice())?
                    .is_some()
            );

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
            let locator = read
                .open_table(schema::INDEXES)?
                .get(index_key.key.as_slice())?
                .ok_or("PRIMARY KEY backing index locator missing")?;
            assert_eq!(
                IndexLocator::decode_for_key(&index_key.key, locator.value())?.row_version,
                before_constraint_sequence
            );
            let mut reverse_key = row_key.to_vec();
            reverse_key.extend_from_slice(&index_key.key[..index_key.key.len() - 16]);
            assert!(
                read.open_table(schema::ROW_INDEXES)?
                    .get(reverse_key.as_slice())?
                    .is_some()
            );
            assert_eq!(row_record.payload, row_payload);
            assert_eq!(sequence, before_constraint_sequence + 1);
            let outcome = outcome.ok_or("committed PRIMARY KEY outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
            assert!(audit_exists);
            assert_eq!(
                lookup_relation_id_by_name(&read, schema_id, "primary_key_crash_table")?,
                Some(table_id)
            );
        }
    }
    Ok(())
}
