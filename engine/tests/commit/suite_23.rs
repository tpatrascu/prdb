use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::lookup_relation_id_by_name;
use prdb_engine::commit::{CommitError, commit_catalog_and_audit};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus, lookup_outcome};
use prdb_engine::schema::{self, CATALOG};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadableDatabase};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::helpers::*;

pub(super) fn process_crash_during_key_constraint_drop_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreatePrimaryKeyRequest, CreateTableRequest, CreateUniqueConstraintRequest,
        TableColumnDefinition,
    };
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_KEY_CONSTRAINT_DROP_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_KEY_CONSTRAINT_DROP_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xd1; 16];
    const DROP_INVOCATION_ID: [u8; 16] = [0xd2; 16];
    const DROP_AUDIT_ID: [u8; 16] = [0xd3; 16];
    const ROW_ID: [u8; 16] = [0xd4; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let metadata = std::fs::read(database_path.with_extension("key-constraint-drop-metadata"))?;
        if metadata.len() != 121 {
            return Err(format!(
                "invalid key-constraint drop metadata length: {}",
                metadata.len()
            )
            .into());
        }
        let constraint_id: [u8; 16] = metadata[32..48].try_into()?;
        let constraint_kind = metadata[120];
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
            return Err(format!("unknown key-constraint drop crash boundary {boundary}").into());
        }
        let result = match constraint_kind {
            2 => storage.drop_primary_key(
                DROP_INVOCATION_ID,
                ACTOR_ID,
                constraint_id,
                i64::MAX,
                audit,
            ),
            3 => storage.drop_unique_constraint(
                DROP_INVOCATION_ID,
                ACTOR_ID,
                constraint_id,
                i64::MAX,
                audit,
            ),
            kind => return Err(format!("unknown key-constraint kind {kind}").into()),
        };
        if boundary == "write" {
            return Err(format!(
                "key-constraint drop returned without injected backend-write exit: {result:?}"
            )
            .into());
        }
        result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for (constraint_kind, constraint_name) in [(2, "primary_key"), (3, "unique")] {
        for (boundary, expected_exit) in [
            ("write", CHILD_EXIT_CODE),
            ("after_commit", POST_COMMIT_EXIT_CODE),
        ] {
            let fixture = Fixture::new();
            let database_path = fixture.path();
            let database = initialize(&database_path)?;
            let storage = StorageManager::new(&database, Duration::from_millis(100))?;
            let admin_role = builtin_owner_id();
            let mut setup_audit = identity_audit_event(0xd5, admin_role, id(0xd6));
            setup_audit.action = AuditAction::AuthorityChange;
            storage.commit_catalog_and_audit(
                id(0xd6),
                admin_role,
                i64::MAX,
                &[
                    principal_write_with_name(ACTOR_ID, 1, 1, "key_constraint_drop_actor"),
                    object_write_with_name(ACTOR_ID, 8, "key_constraint_drop_actor"),
                    membership_write(ACTOR_ID, admin_role),
                    dependency_write(ACTOR_ID, admin_role, 5),
                ],
                &[setup_audit],
            )?;
            let schema_invocation = id(0xd7);
            let mut schema_audit = identity_audit_event(0xd8, ACTOR_ID, id(0xd9));
            schema_audit.correlation_id = schema_invocation;
            let schema = storage.create_schema(
                schema_invocation,
                ACTOR_ID,
                &format!("{constraint_name}_drop_schema"),
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
                name: format!("{constraint_name}_drop_table"),
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
            let mut seed = storage.begin_invocation(id(0xdd), ACTOR_ID, i64::MAX, 1, 1024)?;
            let table_schema = seed.load_catalog_table_schema(table.table_id)?;
            seed.load_catalog_indexes(&table_schema)?;
            let row_id = ROW_ID;
            let row_payload = encode_tuple(
                &table_schema.columns,
                table_schema.schema_version,
                &[Some(ScalarValue::Integer(37))],
            )?;
            seed.stage_put(
                prdb_engine::overlay::RowId {
                    table: table.table_id,
                    row: row_id,
                },
                &row_payload,
            )?;
            seed.commit()?;
            let mut create_audit = identity_audit_event(0xde, ACTOR_ID, table.table_id);
            create_audit.correlation_id = id(0xdf);
            let (constraint_id, index_id) = match constraint_kind {
                2 => {
                    let created = storage.create_primary_key(CreatePrimaryKeyRequest {
                        invocation_id: id(0xdf),
                        authenticated_actor_id: ACTOR_ID,
                        table_id: table.table_id,
                        column_ids: table.column_ids.clone(),
                        retention_deadline_us: i64::MAX,
                        audit_event: create_audit,
                    })?;
                    (created.constraint_id, created.index_id)
                }
                3 => {
                    let created =
                        storage.create_unique_constraint(CreateUniqueConstraintRequest {
                            invocation_id: id(0xdf),
                            authenticated_actor_id: ACTOR_ID,
                            table_id: table.table_id,
                            column_ids: table.column_ids.clone(),
                            retention_deadline_us: i64::MAX,
                            audit_event: create_audit,
                        })?;
                    (created.constraint_id, created.index_id)
                }
                _ => unreachable!(),
            };
            let before_sequence = commit_sequence(&database)?;
            let read = database.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let table_record = catalog
                .get(id_catalog_key(5, table.table_id).as_slice())?
                .ok_or("key-constraint table record missing before drop")?;
            let before_epoch = u64::from_le_bytes(
                decode_registered_envelope(table_record.value())?
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("key-constraint table epoch missing before drop")?
                    .payload
                    .as_slice()
                    .try_into()?,
            );
            drop(catalog);
            drop(read);
            let mut metadata = Vec::with_capacity(120);
            metadata.extend_from_slice(&schema.schema_id);
            metadata.extend_from_slice(&table.table_id);
            metadata.extend_from_slice(&constraint_id);
            metadata.extend_from_slice(&index_id);
            metadata.extend_from_slice(&table.column_ids[0]);
            metadata.extend_from_slice(&row_id);
            metadata.extend_from_slice(&table_schema.schema_version.to_le_bytes());
            metadata.extend_from_slice(&before_sequence.to_le_bytes());
            metadata.extend_from_slice(&before_epoch.to_le_bytes());
            metadata.push(constraint_kind as u8);
            std::fs::write(
                database_path.with_extension("key-constraint-drop-metadata"),
                metadata,
            )?;
            drop(storage);
            drop(database);

            let child_status = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("process_crash_during_key_constraint_drop_recovers_all_or_none")
                .arg("--nocapture")
                .env(CHILD_DATABASE_ENV, &database_path)
                .env(CHILD_BOUNDARY_ENV, boundary)
                .status()?;
            assert_eq!(
                child_status.code(),
                Some(expected_exit),
                "{constraint_name} drop child missed the {boundary} crash boundary: {child_status}"
            );

            let metadata =
                std::fs::read(database_path.with_extension("key-constraint-drop-metadata"))?;
            assert_eq!(metadata.len(), 121);
            let schema_id: [u8; 16] = metadata[0..16].try_into()?;
            let table_id: [u8; 16] = metadata[16..32].try_into()?;
            let constraint_id: [u8; 16] = metadata[32..48].try_into()?;
            let constraint_kind = metadata[120];
            let index_id: [u8; 16] = metadata[48..64].try_into()?;
            let column_id: [u8; 16] = metadata[64..80].try_into()?;
            let row_id: [u8; 16] = metadata[80..96].try_into()?;
            let schema_version = u64::from_le_bytes(metadata[96..104].try_into()?);
            let before_sequence = u64::from_le_bytes(metadata[104..112].try_into()?);
            let before_epoch = u64::from_le_bytes(metadata[112..120].try_into()?);
            assert_eq!(metadata[120], constraint_kind as u8);
            let reopened = Database::open(&database_path)?;
            let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
            let read = reopened.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let constraint_ids =
                prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, table_id, 2)?;
            let sequence = commit_sequence(&reopened)?;
            let table_record = catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("key-constraint table record missing after reopen")?;
            let table_epoch = u64::from_le_bytes(
                decode_registered_envelope(table_record.value())?
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("key-constraint table epoch missing after reopen")?
                    .payload
                    .as_slice()
                    .try_into()?,
            );
            let row_key = table_row_key(table_id, row_id);
            let row_payload = encode_tuple(
                &[ColumnSchema {
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                }],
                schema_version,
                &[Some(ScalarValue::Integer(37))],
            )?;
            assert_eq!(
                row(&reopened, &row_key)?
                    .ok_or("key-constraint row missing")?
                    .payload,
                row_payload
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
            let forward_exists = read
                .open_table(schema::INDEXES)?
                .get(index_key.key.as_slice())?
                .is_some();
            let mut reverse_key = row_key.to_vec();
            reverse_key.extend_from_slice(&index_key.key[..index_key.key.len() - 16]);
            let reverse_exists = read
                .open_table(schema::ROW_INDEXES)?
                .get(reverse_key.as_slice())?
                .is_some();
            let audit_exists = read
                .open_table(schema::AUDIT)?
                .get(&DROP_AUDIT_ID[..])?
                .is_some();
            let outcome = read
                .open_table(schema::OUTCOMES)?
                .get(&DROP_INVOCATION_ID[..])?
                .map(|value| InvocationOutcome::decode(value.value()))
                .transpose()?;
            let committed = boundary == "after_commit";
            for key in [
                id_catalog_key(8, constraint_id),
                id_catalog_key(9, index_id),
                id_catalog_key(2, constraint_id),
                id_catalog_key(2, index_id),
                dependency_write(constraint_id, index_id, 4).key,
            ] {
                assert_eq!(
                    catalog.get(key.as_slice())?.is_some(),
                    !committed,
                    "unexpected key-constraint drop catalog state for {key:?}"
                );
            }
            assert_eq!(constraint_ids.len(), if committed { 0 } else { 1 });
            assert_eq!(forward_exists, !committed);
            assert_eq!(reverse_exists, !committed);
            assert_eq!(table_epoch, before_epoch + u64::from(committed));
            assert_eq!(sequence, before_sequence + u64::from(committed));
            assert_eq!(audit_exists, committed);
            if committed {
                let outcome = outcome.ok_or("key-constraint drop outcome missing")?;
                assert_eq!(outcome.status, OutcomeStatus::Committed);
                assert_eq!(outcome.commit_sequence, sequence);
            } else {
                assert_eq!(outcome, None);
            }
            assert_eq!(
                lookup_relation_id_by_name(
                    &read,
                    schema_id,
                    &format!("{constraint_name}_drop_table")
                )?,
                Some(table_id)
            );
            drop(catalog);
            drop(read);
            prdb_engine::startup::validate_persisted_records(&reopened)?;
        }
    }
    Ok(())
}

pub(super) fn process_exit_after_rejected_table_drop_preserves_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_REJECTED_DROP_TABLE_DATABASE";
    const ACTOR_ID: [u8; 16] = [0xb4; 16];
    const DROP_INVOCATION_ID: [u8; 16] = [0xb2; 16];
    const DROP_AUDIT_ID: [u8; 16] = [0xb3; 16];
    const CHILD_EXIT_CODE: i32 = 89;

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database = Database::open(std::path::PathBuf::from(database_path))?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let mut drop_audit = identity_audit_event(DROP_AUDIT_ID[0], ACTOR_ID, id(81));
        drop_audit.event_id = DROP_AUDIT_ID;
        drop_audit.correlation_id = DROP_INVOCATION_ID;
        match storage.drop_table(DROP_INVOCATION_ID, ACTOR_ID, id(81), 9_999, drop_audit) {
            Err(TableLifecycleError::Commit(CommitError::TableHasDependencies)) => {
                std::process::exit(CHILD_EXIT_CODE);
            }
            result => {
                return Err(format!(
                    "expected external foreign-key RESTRICT after restart, received {result:?}"
                )
                .into());
            }
        }
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    let database = initialize(&database_path)?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(ACTOR_ID, 1, 1, "rejected_drop_process_admin"),
        object_write_with_name(ACTOR_ID, 8, "rejected_drop_process_admin"),
        membership_write(ACTOR_ID, admin_role),
        dependency_write(ACTOR_ID, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xb0, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb1), admin_role, 9_999, &setup_writes, &[setup_audit])?;
    let before_drop_sequence = commit_sequence(&database)?;
    drop(storage);
    drop(database);

    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("process_exit_after_rejected_table_drop_preserves_dependencies")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .status()?;
    assert_eq!(
        child_status.code(),
        Some(CHILD_EXIT_CODE),
        "restricted DROP TABLE child did not exit after durable rejection: {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    assert_eq!(commit_sequence(&reopened)?, before_drop_sequence);
    assert_eq!(
        lookup_outcome(&reopened, DROP_INVOCATION_ID, ACTOR_ID, 1)?
            .ok_or("missing rejected DROP TABLE outcome after process exit")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = reopened.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(5, id(81)),
        id_catalog_key(5, id(82)),
        id_catalog_key(8, id(85)),
        id_catalog_key(8, id(86)),
        dependency_write(id(86), id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_some(),
            "process exit removed restricted-drop dependency {key:?}"
        );
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&reopened)?;
    Ok(())
}

pub(super) fn process_crash_during_foreign_key_creation_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateForeignKeyRequest, CreatePrimaryKeyRequest, CreateTableRequest, TableColumnDefinition,
    };
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_FOREIGN_KEY_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_FOREIGN_KEY_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xb1; 16];
    const FK_INVOCATION_ID: [u8; 16] = [0xb2; 16];
    const FK_AUDIT_ID: [u8; 16] = [0xb3; 16];
    const SOURCE_ROW_ID: [u8; 16] = [0xb4; 16];
    const TARGET_ROW_ID: [u8; 16] = [0xb5; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let mut setup_audit = identity_audit_event(0xb6, admin_role, id(0xb7));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(
            id(0xb7),
            admin_role,
            i64::MAX,
            &[
                principal_write_with_name(ACTOR_ID, 1, 1, "foreign_key_crash_actor"),
                object_write_with_name(ACTOR_ID, 8, "foreign_key_crash_actor"),
                membership_write(ACTOR_ID, admin_role),
                dependency_write(ACTOR_ID, admin_role, 5),
            ],
            &[setup_audit],
        )?;
        let schema_invocation = id(0xb8);
        let mut schema_audit = identity_audit_event(0xb9, ACTOR_ID, id(0xba));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "foreign_key_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let target_table_invocation = id(0xbb);
        let mut target_table_audit = identity_audit_event(0xbc, ACTOR_ID, id(0xbd));
        target_table_audit.correlation_id = target_table_invocation;
        let target_table = storage.create_table(CreateTableRequest {
            invocation_id: target_table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "foreign_key_target".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "id".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: target_table_audit,
        })?;
        let target_pk_invocation = id(0xbe);
        let mut target_pk_audit = identity_audit_event(0xbf, ACTOR_ID, id(0xc0));
        target_pk_audit.correlation_id = target_pk_invocation;
        let target_pk = storage.create_primary_key(CreatePrimaryKeyRequest {
            invocation_id: target_pk_invocation,
            authenticated_actor_id: ACTOR_ID,
            table_id: target_table.table_id,
            column_ids: target_table.column_ids.clone(),
            retention_deadline_us: i64::MAX,
            audit_event: target_pk_audit,
        })?;
        let source_table_invocation = id(0xc1);
        let mut source_table_audit = identity_audit_event(0xc2, ACTOR_ID, id(0xc3));
        source_table_audit.correlation_id = source_table_invocation;
        let source_table = storage.create_table(CreateTableRequest {
            invocation_id: source_table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "foreign_key_source".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "target_id".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: source_table_audit,
        })?;

        let mut references_audit = identity_audit_event(0xc4, admin_role, id(0xc5));
        references_audit.action = AuditAction::AuthorityChange;
        let mut references_actor_epoch =
            principal_write_with_name(ACTOR_ID, 1, 2, "foreign_key_crash_actor");
        references_actor_epoch.expected_value =
            principal_write_with_name(ACTOR_ID, 1, 1, "foreign_key_crash_actor").payload;
        storage
            .commit_catalog_and_audit(
                id(0xc5),
                admin_role,
                i64::MAX,
                &[
                    grant_write(
                        ACTOR_ID,
                        target_table.table_id,
                        6,
                        1,
                        target_table.table_id,
                        admin_role,
                    ),
                    dependency_write(ACTOR_ID, target_table.table_id, 5),
                    references_actor_epoch,
                ],
                &[references_audit],
            )
            .map_err(|error| format!("REFERENCES grant setup failed: {error:?}"))?;

        for (invocation_id, table_id, row_id) in [
            (id(0xc6), target_table.table_id, TARGET_ROW_ID),
            (id(0xc7), source_table.table_id, SOURCE_ROW_ID),
        ] {
            let mut seed = storage.begin_invocation(invocation_id, ACTOR_ID, i64::MAX, 1, 1024)?;
            let table_schema = seed.load_catalog_table_schema(table_id)?;
            seed.load_catalog_indexes(&table_schema)?;
            seed.stage_put(
                prdb_engine::overlay::RowId {
                    table: table_id,
                    row: row_id,
                },
                &encode_tuple(
                    &table_schema.columns,
                    table_schema.schema_version,
                    &[Some(ScalarValue::Integer(37))],
                )?,
            )?;
            seed.commit()?;
        }

        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_epoch = |table_id: [u8; 16]| -> Result<u64, Box<dyn std::error::Error>> {
            let record = catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("foreign-key participant table missing")?;
            let record = decode_registered_envelope(record.value())?;
            Ok(u64::from_le_bytes(
                record
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("foreign-key participant epoch missing")?
                    .payload
                    .as_slice()
                    .try_into()?,
            ))
        };
        let source_epoch = table_epoch(source_table.table_id)?;
        let target_epoch = table_epoch(target_table.table_id)?;
        drop(catalog);
        drop(read);
        let before_fk_sequence = commit_sequence(&database)?;
        let mut source_schema = storage.begin_invocation(id(0xc8), ACTOR_ID, i64::MAX, 1, 1024)?;
        let source_table_schema = source_schema.load_catalog_table_schema(source_table.table_id)?;
        let source_schema_version = source_table_schema.schema_version;
        drop(source_schema);
        let mut target_schema = storage.begin_invocation(id(0xc9), ACTOR_ID, i64::MAX, 1, 1024)?;
        let target_table_schema = target_schema.load_catalog_table_schema(target_table.table_id)?;
        let target_schema_version = target_table_schema.schema_version;
        drop(target_schema);
        let mut metadata = Vec::with_capacity(152);
        metadata.extend_from_slice(&source_table.table_id);
        metadata.extend_from_slice(&target_table.table_id);
        metadata.extend_from_slice(&source_table.column_ids[0]);
        metadata.extend_from_slice(&target_table.column_ids[0]);
        metadata.extend_from_slice(&target_pk.constraint_id);
        metadata.extend_from_slice(&SOURCE_ROW_ID);
        metadata.extend_from_slice(&TARGET_ROW_ID);
        metadata.extend_from_slice(&source_schema_version.to_le_bytes());
        metadata.extend_from_slice(&target_schema_version.to_le_bytes());
        metadata.extend_from_slice(&source_epoch.to_le_bytes());
        metadata.extend_from_slice(&target_epoch.to_le_bytes());
        metadata.extend_from_slice(&before_fk_sequence.to_le_bytes());
        std::fs::write(
            database_path.with_extension("foreign-key-crash-metadata"),
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
        let mut fk_audit = identity_audit_event(FK_AUDIT_ID[0], ACTOR_ID, id(0xca));
        fk_audit.event_id = FK_AUDIT_ID;
        fk_audit.correlation_id = FK_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown FOREIGN KEY crash boundary {boundary}").into());
        }
        let create_result = storage.create_foreign_key(CreateForeignKeyRequest {
            invocation_id: FK_INVOCATION_ID,
            authenticated_actor_id: ACTOR_ID,
            source_table_id: source_table.table_id,
            source_column_ids: source_table.column_ids,
            target_constraint_id: target_pk.constraint_id,
            retention_deadline_us: i64::MAX,
            audit_event: fk_audit,
        });
        if boundary == "write" {
            return Err(format!(
                "FOREIGN KEY creation returned without injected backend-write exit: {create_result:?}"
            )
            .into());
        }
        create_result.map_err(|error| format!("create_foreign_key failed: {error:?}"))?;
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
            .arg("process_crash_during_foreign_key_creation_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "FOREIGN KEY child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("foreign-key-crash-metadata"))?;
        assert_eq!(metadata.len(), 152);
        let source_table_id: [u8; 16] = metadata[0..16].try_into()?;
        let target_table_id: [u8; 16] = metadata[16..32].try_into()?;
        let source_column_id: [u8; 16] = metadata[32..48].try_into()?;
        let target_column_id: [u8; 16] = metadata[48..64].try_into()?;
        let target_constraint_id: [u8; 16] = metadata[64..80].try_into()?;
        let source_row_id: [u8; 16] = metadata[80..96].try_into()?;
        let target_row_id: [u8; 16] = metadata[96..112].try_into()?;
        let source_schema_version = u64::from_le_bytes(metadata[112..120].try_into()?);
        let target_schema_version = u64::from_le_bytes(metadata[120..128].try_into()?);
        let source_epoch = u64::from_le_bytes(metadata[128..136].try_into()?);
        let target_epoch = u64::from_le_bytes(metadata[136..144].try_into()?);
        let before_fk_sequence = u64::from_le_bytes(metadata[144..152].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let fk_ids =
            prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, source_table_id, 5)?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&FK_AUDIT_ID[..])?
            .is_some();
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&FK_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let sequence = commit_sequence(&reopened)?;
        let source_row = row(&reopened, &table_row_key(source_table_id, source_row_id))?
            .ok_or("foreign-key source row missing")?;
        let target_row = row(&reopened, &table_row_key(target_table_id, target_row_id))?
            .ok_or("foreign-key target row missing")?;
        assert_eq!(
            source_row.payload,
            encode_tuple(
                &[ColumnSchema {
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                }],
                source_schema_version,
                &[Some(ScalarValue::Integer(37))],
            )?
        );
        assert_eq!(
            target_row.payload,
            encode_tuple(
                &[ColumnSchema {
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                }],
                target_schema_version,
                &[Some(ScalarValue::Integer(37))],
            )?
        );
        let catalog = read.open_table(CATALOG)?;
        let current_epoch = |table_id: [u8; 16]| -> Result<u64, Box<dyn std::error::Error>> {
            let value = catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("foreign-key participant table missing after reopen")?;
            let record = decode_registered_envelope(value.value())?;
            Ok(u64::from_le_bytes(
                record
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("foreign-key participant epoch missing after reopen")?
                    .payload
                    .as_slice()
                    .try_into()?,
            ))
        };
        if boundary == "write" {
            assert!(fk_ids.is_empty());
            assert_eq!(current_epoch(source_table_id)?, source_epoch);
            assert_eq!(current_epoch(target_table_id)?, target_epoch);
            assert_eq!(sequence, before_fk_sequence);
            assert_eq!(outcome, None);
            assert!(!audit_exists);
        } else {
            assert_eq!(fk_ids.len(), 1);
            let fk_id = fk_ids[0];
            let fk_key = id_catalog_key(8, fk_id);
            let fk_record = catalog
                .get(fk_key.as_slice())?
                .ok_or("foreign-key constraint record missing")?;
            let fk_record = decode_registered_envelope(fk_record.value())?;
            assert_eq!(
                fk_record
                    .fields
                    .iter()
                    .find(|field| field.tag == 1)
                    .ok_or("foreign-key source table missing")?
                    .payload
                    .as_slice(),
                source_table_id.as_slice()
            );
            assert_eq!(
                fk_record
                    .fields
                    .iter()
                    .find(|field| field.tag == 5)
                    .ok_or("foreign-key target constraint missing")?
                    .payload
                    .as_slice(),
                target_constraint_id.as_slice()
            );
            assert!(
                catalog
                    .get(
                        dependency_write(fk_id, target_constraint_id, 3)
                            .key
                            .as_slice()
                    )?
                    .is_some()
            );
            assert!(catalog.get(id_catalog_key(2, fk_id).as_slice())?.is_some());
            assert_eq!(current_epoch(source_table_id)?, source_epoch + 1);
            assert_eq!(current_epoch(target_table_id)?, target_epoch + 1);
            assert_eq!(sequence, before_fk_sequence + 1);
            let outcome = outcome.ok_or("committed FOREIGN KEY outcome was not recovered")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
            assert!(audit_exists);
            assert!(fk_record.fields.iter().any(|field| {
                field.tag == 3 && field.payload.as_slice().ends_with(&source_column_id)
            }));
            assert!(fk_record.fields.iter().any(|field| {
                field.tag == 6 && field.payload.as_slice().ends_with(&target_column_id)
            }));
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_foreign_key_drop_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateForeignKeyRequest, CreatePrimaryKeyRequest, CreateTableRequest, TableColumnDefinition,
    };
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_FOREIGN_KEY_DROP_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_FOREIGN_KEY_DROP_BOUNDARY";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xf1; 16];
    const DROP_INVOCATION_ID: [u8; 16] = [0xf2; 16];
    const DROP_AUDIT_ID: [u8; 16] = [0xf3; 16];
    const SOURCE_ROW_ID: [u8; 16] = [0xf4; 16];
    const TARGET_ROW_ID: [u8; 16] = [0xf5; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let metadata = std::fs::read(database_path.with_extension("foreign-key-drop-metadata"))?;
        if metadata.len() != 168 {
            return Err(format!(
                "invalid FOREIGN KEY drop metadata length: {}",
                metadata.len()
            )
            .into());
        }
        let foreign_key_id: [u8; 16] = metadata[32..48].try_into()?;
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
        let mut audit = identity_audit_event(DROP_AUDIT_ID[0], ACTOR_ID, foreign_key_id);
        audit.event_id = DROP_AUDIT_ID;
        audit.correlation_id = DROP_INVOCATION_ID;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown FOREIGN KEY drop crash boundary {boundary}").into());
        }
        let result = storage.drop_foreign_key(
            DROP_INVOCATION_ID,
            ACTOR_ID,
            foreign_key_id,
            i64::MAX,
            audit,
        );
        if boundary == "write" {
            return Err(format!(
                "FOREIGN KEY drop returned without injected backend-write exit: {result:?}"
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
        let mut setup_audit = identity_audit_event(0xf6, admin_role, id(0xf7));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(
            id(0xf7),
            admin_role,
            i64::MAX,
            &[
                principal_write_with_name(ACTOR_ID, 1, 1, "foreign_key_drop_actor"),
                object_write_with_name(ACTOR_ID, 8, "foreign_key_drop_actor"),
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
            "foreign_key_drop_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let target_table_invocation = id(0xfb);
        let mut target_table_audit = identity_audit_event(0xfc, ACTOR_ID, id(0xfd));
        target_table_audit.correlation_id = target_table_invocation;
        let target_table = storage.create_table(CreateTableRequest {
            invocation_id: target_table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "foreign_key_drop_target".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "id".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: target_table_audit,
        })?;
        let target_pk_invocation = id(0x91);
        let mut target_pk_audit = identity_audit_event(0x92, ACTOR_ID, id(0x93));
        target_pk_audit.correlation_id = target_pk_invocation;
        let target_pk = storage.create_primary_key(CreatePrimaryKeyRequest {
            invocation_id: target_pk_invocation,
            authenticated_actor_id: ACTOR_ID,
            table_id: target_table.table_id,
            column_ids: target_table.column_ids.clone(),
            retention_deadline_us: i64::MAX,
            audit_event: target_pk_audit,
        })?;
        let source_table_invocation = id(0x94);
        let mut source_table_audit = identity_audit_event(0x95, ACTOR_ID, id(0x96));
        source_table_audit.correlation_id = source_table_invocation;
        let source_table = storage.create_table(CreateTableRequest {
            invocation_id: source_table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "foreign_key_drop_source".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "target_id".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: source_table_audit,
        })?;
        let mut references_audit = identity_audit_event(0x97, admin_role, id(0x98));
        references_audit.action = AuditAction::AuthorityChange;
        let references_actor_epoch =
            principal_write_with_name(ACTOR_ID, 1, 2, "foreign_key_drop_actor");
        let mut references_actor_epoch = references_actor_epoch;
        references_actor_epoch.expected_value =
            principal_write_with_name(ACTOR_ID, 1, 1, "foreign_key_drop_actor").payload;
        storage.commit_catalog_and_audit(
            id(0x98),
            admin_role,
            i64::MAX,
            &[
                grant_write(
                    ACTOR_ID,
                    target_table.table_id,
                    6,
                    1,
                    target_table.table_id,
                    admin_role,
                ),
                dependency_write(ACTOR_ID, target_table.table_id, 5),
                references_actor_epoch,
            ],
            &[references_audit],
        )?;

        for (invocation_id, table_id, row_id) in [
            (id(0x99), target_table.table_id, TARGET_ROW_ID),
            (id(0x9a), source_table.table_id, SOURCE_ROW_ID),
        ] {
            let mut seed = storage.begin_invocation(invocation_id, ACTOR_ID, i64::MAX, 1, 1024)?;
            let table_schema = seed.load_catalog_table_schema(table_id)?;
            seed.load_catalog_indexes(&table_schema)?;
            let row_payload = encode_tuple(
                &table_schema.columns,
                table_schema.schema_version,
                &[Some(ScalarValue::Integer(37))],
            )?;
            seed.stage_put(
                prdb_engine::overlay::RowId {
                    table: table_id,
                    row: row_id,
                },
                &row_payload,
            )?;
            seed.commit()?;
        }
        let mut create_audit = identity_audit_event(0x9b, ACTOR_ID, source_table.table_id);
        create_audit.correlation_id = id(0x9c);
        let foreign_key = storage.create_foreign_key(CreateForeignKeyRequest {
            invocation_id: id(0x9c),
            authenticated_actor_id: ACTOR_ID,
            source_table_id: source_table.table_id,
            source_column_ids: source_table.column_ids.clone(),
            target_constraint_id: target_pk.constraint_id,
            retention_deadline_us: i64::MAX,
            audit_event: create_audit,
        })?;
        let before_sequence = commit_sequence(&database)?;
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_epoch = |table_id: [u8; 16]| -> Result<u64, Box<dyn std::error::Error>> {
            let record = catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("FOREIGN KEY participant table missing before drop")?;
            let epoch = decode_registered_envelope(record.value())?
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("FOREIGN KEY participant epoch missing before drop")?
                .payload
                .as_slice()
                .try_into()?;
            Ok(u64::from_le_bytes(epoch))
        };
        let source_epoch = table_epoch(source_table.table_id)?;
        let target_epoch = table_epoch(target_table.table_id)?;
        drop(catalog);
        drop(read);
        let mut source_schema = storage.begin_invocation(id(0x9d), ACTOR_ID, i64::MAX, 1, 1024)?;
        let source_schema_version = source_schema
            .load_catalog_table_schema(source_table.table_id)?
            .schema_version;
        drop(source_schema);
        let mut target_schema = storage.begin_invocation(id(0x9e), ACTOR_ID, i64::MAX, 1, 1024)?;
        let target_schema_version = target_schema
            .load_catalog_table_schema(target_table.table_id)?
            .schema_version;
        drop(target_schema);
        let mut metadata = Vec::with_capacity(168);
        metadata.extend_from_slice(&source_table.table_id);
        metadata.extend_from_slice(&target_table.table_id);
        metadata.extend_from_slice(&foreign_key.foreign_key_id);
        metadata.extend_from_slice(&target_pk.constraint_id);
        metadata.extend_from_slice(&source_table.column_ids[0]);
        metadata.extend_from_slice(&target_table.column_ids[0]);
        metadata.extend_from_slice(&SOURCE_ROW_ID);
        metadata.extend_from_slice(&TARGET_ROW_ID);
        metadata.extend_from_slice(&source_schema_version.to_le_bytes());
        metadata.extend_from_slice(&target_schema_version.to_le_bytes());
        metadata.extend_from_slice(&source_epoch.to_le_bytes());
        metadata.extend_from_slice(&target_epoch.to_le_bytes());
        metadata.extend_from_slice(&before_sequence.to_le_bytes());
        std::fs::write(
            database_path.with_extension("foreign-key-drop-metadata"),
            metadata,
        )?;
        drop(storage);
        drop(database);

        let child_status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("process_crash_during_foreign_key_drop_recovers_all_or_none")
            .arg("--nocapture")
            .env(CHILD_DATABASE_ENV, &database_path)
            .env(CHILD_BOUNDARY_ENV, boundary)
            .status()?;
        assert_eq!(
            child_status.code(),
            Some(expected_exit),
            "FOREIGN KEY drop child missed the {boundary} crash boundary: {child_status}"
        );

        let metadata = std::fs::read(database_path.with_extension("foreign-key-drop-metadata"))?;
        assert_eq!(metadata.len(), 168);
        let source_table_id: [u8; 16] = metadata[0..16].try_into()?;
        let target_table_id: [u8; 16] = metadata[16..32].try_into()?;
        let foreign_key_id: [u8; 16] = metadata[32..48].try_into()?;
        let target_constraint_id: [u8; 16] = metadata[48..64].try_into()?;
        let source_column_id: [u8; 16] = metadata[64..80].try_into()?;
        let target_column_id: [u8; 16] = metadata[80..96].try_into()?;
        let source_row_id: [u8; 16] = metadata[96..112].try_into()?;
        let target_row_id: [u8; 16] = metadata[112..128].try_into()?;
        let source_schema_version = u64::from_le_bytes(metadata[128..136].try_into()?);
        let target_schema_version = u64::from_le_bytes(metadata[136..144].try_into()?);
        let source_epoch = u64::from_le_bytes(metadata[144..152].try_into()?);
        let target_epoch = u64::from_le_bytes(metadata[152..160].try_into()?);
        let before_sequence = u64::from_le_bytes(metadata[160..168].try_into()?);
        let reopened = Database::open(&database_path)?;
        let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
        let read = reopened.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let constraint_ids =
            prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, source_table_id, 5)?;
        let sequence = commit_sequence(&reopened)?;
        let audit_exists = read
            .open_table(schema::AUDIT)?
            .get(&DROP_AUDIT_ID[..])?
            .is_some();
        let outcome = read
            .open_table(schema::OUTCOMES)?
            .get(&DROP_INVOCATION_ID[..])?
            .map(|value| InvocationOutcome::decode(value.value()))
            .transpose()?;
        let committed = boundary == "after_commit";
        assert_eq!(constraint_ids.contains(&foreign_key_id), !committed);
        for key in [
            id_catalog_key(8, foreign_key_id),
            id_catalog_key(2, foreign_key_id),
            dependency_write(foreign_key_id, target_constraint_id, 3).key,
        ] {
            assert_eq!(catalog.get(key.as_slice())?.is_some(), !committed);
        }
        if !committed {
            let foreign_key = catalog
                .get(id_catalog_key(8, foreign_key_id).as_slice())?
                .ok_or("FOREIGN KEY record missing before committed removal")?;
            let foreign_key = decode_registered_envelope(foreign_key.value())?;
            for (tag, expected) in [
                (1, source_table_id),
                (3, source_column_id),
                (5, target_constraint_id),
                (6, target_column_id),
            ] {
                let payload = &foreign_key
                    .fields
                    .iter()
                    .find(|field| field.tag == tag)
                    .ok_or("FOREIGN KEY endpoint missing")?
                    .payload;
                if matches!(tag, 3 | 6) {
                    assert_eq!(u32::from_le_bytes(payload[..4].try_into()?), 1);
                    assert_eq!(&payload[4..], expected.as_slice());
                } else {
                    assert_eq!(payload.as_slice(), expected.as_slice());
                }
            }
        }
        let table_epoch = |table_id: [u8; 16]| -> Result<u64, Box<dyn std::error::Error>> {
            let record = catalog
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("FOREIGN KEY participant table missing after reopen")?;
            let epoch = decode_registered_envelope(record.value())?
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("FOREIGN KEY participant epoch missing after reopen")?
                .payload
                .as_slice()
                .try_into()?;
            Ok(u64::from_le_bytes(epoch))
        };
        assert_eq!(
            table_epoch(source_table_id)?,
            source_epoch + u64::from(committed)
        );
        assert_eq!(
            table_epoch(target_table_id)?,
            target_epoch + u64::from(committed)
        );
        assert_eq!(sequence, before_sequence + u64::from(committed));
        assert_eq!(audit_exists, committed);
        if committed {
            let outcome = outcome.ok_or("FOREIGN KEY drop outcome missing")?;
            assert_eq!(outcome.status, OutcomeStatus::Committed);
            assert_eq!(outcome.commit_sequence, sequence);
        } else {
            assert_eq!(outcome, None);
        }
        for (table_id, row_id, schema_version) in [
            (source_table_id, source_row_id, source_schema_version),
            (target_table_id, target_row_id, target_schema_version),
        ] {
            let payload = encode_tuple(
                &[ColumnSchema {
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                }],
                schema_version,
                &[Some(ScalarValue::Integer(37))],
            )?;
            assert_eq!(
                row(&reopened, &table_row_key(table_id, row_id))?
                    .ok_or("FOREIGN KEY participant row missing")?
                    .payload,
                payload
            );
        }
        assert!(
            catalog
                .get(id_catalog_key(8, target_constraint_id).as_slice())?
                .is_some()
        );
        let target_index_id: [u8; 16] = decode_registered_envelope(
            catalog
                .get(id_catalog_key(8, target_constraint_id).as_slice())?
                .ok_or("referenced PRIMARY KEY disappeared")?
                .value(),
        )?
        .fields
        .iter()
        .find(|field| field.tag == 11)
        .ok_or("referenced PRIMARY KEY backing index missing")?
        .payload
        .as_slice()
        .try_into()?;
        let target_payload = encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            }],
            target_schema_version,
            &[Some(ScalarValue::Integer(37))],
        )?;
        let index_key = prdb_engine::index_key::derive_index_key(
            target_index_id,
            target_row_id,
            target_schema_version,
            &[target_column_id],
            &[ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            }],
            &target_payload,
            &[target_column_id],
        )?;
        assert!(
            read.open_table(schema::INDEXES)?
                .get(index_key.key.as_slice())?
                .is_some()
        );
        let mut reverse_key = table_row_key(target_table_id, target_row_id).to_vec();
        reverse_key.extend_from_slice(&index_key.key[..index_key.key.len() - 16]);
        assert!(
            read.open_table(schema::ROW_INDEXES)?
                .get(reverse_key.as_slice())?
                .is_some()
        );
        drop(catalog);
        drop(read);
        prdb_engine::startup::validate_persisted_records(&reopened)?;
    }
    Ok(())
}

pub(super) fn supervisor_drains_both_worker_channels_before_durable_finalize()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::ResolveRelationRequest;
    use prdb_engine::ipc::{IpcFrame, Operation, encode_frame, read_frame, write_frame};
    use prdb_engine::planner_ipc::DescribeRelationResponse;
    use prdb_engine::storage_manager::SupervisorInvocationError;
    use prdb_engine::supervisor_ipc::SUPERVISOR_PAYLOAD_VERSION;
    use std::io::Cursor;
    use std::os::unix::net::UnixStream;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "worker_user"),
            object_write_with_name(user_id, 8, "worker_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let mut invocation = storage.admit_supervisor_invocation(
        id(0x73),
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;

    let (mut planner_worker, planner_storage) = UnixStream::pair()?;
    let (executor_worker, executor_storage) = UnixStream::pair()?;
    let (_optimizer_worker, optimizer_storage) = UnixStream::pair()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let serving = scope.spawn(|| {
            invocation.serve_worker_channels(planner_storage, executor_storage, optimizer_storage)
        });
        write_frame(
            &mut planner_worker,
            &IpcFrame {
                operation: Operation::DescribeRelation as u16,
                request_id: 1,
                payload: ResolveRelationRequest {
                    schema_name: "information_schema".to_owned(),
                    relation_name: "tables".to_owned(),
                }
                .encode()?,
            },
        )?;
        let response = read_frame(&mut planner_worker)?.ok_or("planner channel closed")?;
        assert_eq!(response.operation, Operation::DescribeRelation as u16);
        assert_eq!(response.request_id, 1);
        assert!(
            DescribeRelationResponse::decode(&response.payload)?
                .table
                .is_some()
        );
        planner_worker.shutdown(std::net::Shutdown::Write)?;
        drop(planner_worker);
        drop(executor_worker);
        drop(_optimizer_worker);
        serving
            .join()
            .expect("worker-channel server thread panicked")
            .map_err(|error| format!("worker-channel server failed: {error:?}"))?;
        Ok(())
    })?;

    assert!(matches!(
        invocation.serve_terminal(&mut Cursor::new(Vec::new())),
        Err(SupervisorInvocationError::WorkerExitNotObserved)
    ));
    let mut worker = Command::new("true").spawn()?;
    assert!(invocation.observe_worker_exit(&mut worker)?.success());
    let (_executor_worker, mut executor_storage) = UnixStream::pair()?;
    assert!(matches!(
        invocation.serve_executor(&mut executor_storage),
        Err(SupervisorInvocationError::Closed)
    ));

    let terminal = IpcFrame {
        operation: Operation::FinalizeTransaction as u16,
        request_id: 1,
        payload: SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec(),
    };
    let request_bytes = encode_frame(&terminal)?;
    let response_start = request_bytes.len() as u64;
    let mut terminal_stream = Cursor::new(request_bytes);
    invocation.serve_terminal(&mut terminal_stream)?;
    terminal_stream.set_position(response_start);
    let response = read_frame(&mut terminal_stream)?.ok_or("missing terminal response")?;
    assert_eq!(response.operation, Operation::FinalizeTransaction as u16);
    assert_eq!(response.request_id, 1);
    assert_eq!(response.payload[2], 0);
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    Ok(())
}

pub(super) fn supervisor_worker_exit_failure_is_reaped_and_durably_aborted()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::SupervisorInvocationError;
    use prdb_engine::supervisor_ipc::{
        GatewayAbortReason, GatewayTerminalDecision, send_gateway_result_data_event,
        send_gateway_terminal_for_confirmed_outcome,
    };
    use std::io::Cursor;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "worker_exit_user"),
            object_write_with_name(user_id, 8, "worker_exit_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let invocation = storage.admit_supervisor_invocation(
        id(0x73),
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;
    let mut terminal_stream = Cursor::new(Vec::new());
    assert!(
        send_gateway_terminal_for_confirmed_outcome(
            &storage,
            &invocation,
            &mut terminal_stream,
            9,
            GatewayAbortReason::WorkerFailure,
        )
        .is_err()
    );
    assert!(terminal_stream.get_ref().is_empty());

    let mut result_and_terminal_stream = Cursor::new(Vec::new());
    send_gateway_result_data_event(
        &mut result_and_terminal_stream,
        9,
        b"partial SQL output".to_vec(),
    )?;

    let mut worker = Command::new("sh").args(["-c", "exit 7"]).spawn()?;
    assert!(matches!(
        invocation.observe_worker_exit(&mut worker),
        Err(SupervisorInvocationError::WorkerExited(_))
    ));
    assert_eq!(worker.try_wait()?.unwrap().code(), Some(7));
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        send_gateway_terminal_for_confirmed_outcome(
            &storage,
            &invocation,
            &mut result_and_terminal_stream,
            9,
            GatewayAbortReason::WorkerFailure,
        )?,
        GatewayTerminalDecision::Abort(GatewayAbortReason::WorkerFailure)
    );
    result_and_terminal_stream.set_position(0);
    let provisional =
        read_frame(&mut result_and_terminal_stream)?.ok_or("missing provisional result data")?;
    assert_eq!(provisional.operation, Operation::GatewayResultData as u16);
    assert_eq!(provisional.request_id, 9);
    assert_eq!(provisional.payload, b"partial SQL output");
    let terminal =
        read_frame(&mut result_and_terminal_stream)?.ok_or("missing aborted terminal")?;
    assert_eq!(terminal.operation, Operation::GatewayTerminal as u16);
    assert_eq!(terminal.request_id, 9);
    assert!(String::from_utf8(terminal.payload)?.contains("\"outcome\":\"aborted\""));
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output()
-> Result<(), Box<dyn std::error::Error>> {
    run_sandboxed_v3_sql_error_fixture("PRDB_TEST_QUERY_ARTIFACT", false, None, false, false)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_v3_shared_sql_library_resolves_loads_and_durably_aborts_on_sql_error()
-> Result<(), Box<dyn std::error::Error>> {
    run_sandboxed_v3_sql_error_fixture(
        "PRDB_TEST_SHARED_SQL_QUERY_ARTIFACT",
        false,
        Some("PRDB_TEST_SDK_RELEASE_DIR"),
        false,
        false,
    )
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_repository_sdk_library_loads_and_stays_pinned_until_worker_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    run_sandboxed_v3_sql_error_fixture(
        "PRDB_TEST_SHARED_SQL_QUERY_ARTIFACT",
        false,
        Some("PRDB_TEST_SDK_RELEASE_DIR"),
        false,
        false,
    )
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_shared_sql_missing_dependency_aborts_inside_executor()
-> Result<(), Box<dyn std::error::Error>> {
    run_sandboxed_v3_sql_error_fixture(
        "PRDB_TEST_SHARED_SQL_LOAD_FAILURE_ARTIFACT",
        false,
        Some("PRDB_TEST_SDK_RELEASE_DIR"),
        true,
        false,
    )
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_v3_constructor_cannot_connect_to_host_loopback()
-> Result<(), Box<dyn std::error::Error>> {
    run_sandboxed_v3_sql_error_fixture(
        "PRDB_TEST_NETWORK_CONSTRUCTOR_ARTIFACT",
        true,
        None,
        false,
        false,
    )
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_direct_executor_ipc_rejects_forged_relation_handle()
-> Result<(), Box<dyn std::error::Error>> {
    run_sandboxed_v3_sql_error_fixture(
        "PRDB_TEST_FORGED_HANDLE_QUERY_ARTIFACT",
        false,
        None,
        true,
        true,
    )
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_direct_executor_ipc_rejects_forged_row_handle_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    run_sandboxed_v3_sql_error_fixture(
        "PRDB_TEST_FORGED_ROW_HANDLE_QUERY_ARTIFACT",
        false,
        None,
        true,
        true,
    )
}
