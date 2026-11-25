use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::bootstrap::{BootstrapError, ReadOnlyStorageInstance, StorageInstance};
use prdb_engine::client_credentials::read_client_credentials;
use prdb_engine::schema::{AUDIT, STATS};
use redb::{Database, ReadOnlyDatabase, ReadableDatabase, ReadableTable, ReadableTableMetadata};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::process::Command;

use super::helpers::*;

pub(super) fn migration_apply_requires_complete_history_ack_and_publishes_validated_v2() {
    let fixture = Fixture::new();
    let mut session = prdb_engine::bootstrap::initialize(
        fixture.instance_dir(),
        "migration_apply_admin",
        i64::MAX,
    )
    .expect("initialize migration source");
    let admin_id = session.admin_user_id();
    let token_id = session.token_id();
    let secret = zeroize::Zeroizing::new(session.take_secret().expect("bootstrap secret"));
    drop(session.acknowledge().expect("publish source READY"));

    let source_path = fixture.instance_dir().join("storage.redb");
    let source_database = Database::open(&source_path).expect("open migration source");
    let source_storage = prdb_engine::storage_manager::StorageManager::new(
        &source_database,
        std::time::Duration::from_millis(100),
    )
    .expect("create migration source storage manager");
    let migration_audit = |event_id: u8,
                           invocation_id: [u8; 16],
                           action: AuditAction,
                           target_ids: Vec<[u8; 16]>| AuditEvent {
        event_id: [event_id; 16],
        actor_id: Some(admin_id),
        correlation_id: invocation_id,
        occurred_at_us: 1,
        action,
        result: AuditResult::Succeeded,
        target_ids,
    };
    let schema = source_storage
        .create_schema(
            [0xa1; 16],
            admin_id,
            "migration_access",
            admin_id,
            i64::MAX,
            migration_audit(0xa1, [0xa1; 16], AuditAction::Ddl, Vec::new()),
        )
        .expect("create migration test schema");
    let table = source_storage
        .create_table(prdb_engine::storage_manager::CreateTableRequest {
            invocation_id: [0xa2; 16],
            authenticated_actor_id: admin_id,
            schema_id: schema.schema_id,
            name: "visible_records".to_owned(),
            columns: vec![prdb_engine::storage_manager::TableColumnDefinition {
                name: "value".to_owned(),
                scalar_type: prdb_engine::tuple::ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: migration_audit(0xa2, [0xa2; 16], AuditAction::Ddl, Vec::new()),
        })
        .expect("create migration test table");
    let reader = source_storage
        .create_user(
            [0xa3; 16],
            admin_id,
            "migration_reader",
            i64::MAX,
            migration_audit(0xa3, [0xa3; 16], AuditAction::Ddl, Vec::new()),
        )
        .expect("create migration test user");
    let mut reader_token = source_storage
        .issue_token(prdb_engine::storage_manager::TokenIssueRequest {
            invocation_id: [0xa4; 16],
            authenticated_actor_id: admin_id,
            user_id: reader.user_id,
            label: "migration reader",
            expires_at_us: i64::MAX,
            retention_deadline_us: i64::MAX,
            audit_event: migration_audit(
                0xa4,
                [0xa4; 16],
                AuditAction::TokenLifecycle,
                vec![reader.user_id],
            ),
        })
        .expect("issue migration test token");
    let reader_token_id = reader_token.token_id;
    let reader_secret = zeroize::Zeroizing::new(
        reader_token
            .take_secret()
            .expect("one-time migration reader secret"),
    );
    for (event_id, invocation_id, object_id, privilege) in [
        (
            0xa5,
            [0xa5; 16],
            schema.schema_id,
            prdb_engine::storage_manager::ObjectGrantPrivilege::Usage,
        ),
        (
            0xa6,
            [0xa6; 16],
            table.table_id,
            prdb_engine::storage_manager::ObjectGrantPrivilege::Select,
        ),
    ] {
        source_storage
            .set_object_grant(prdb_engine::storage_manager::SetObjectGrantRequest {
                invocation_id,
                authenticated_actor_id: admin_id,
                grantee_id: reader.user_id,
                object_id,
                privilege,
                target: prdb_engine::storage_manager::ObjectGrantTarget::Object,
                grant: true,
                retention_deadline_us: i64::MAX,
                audit_event: migration_audit(
                    event_id,
                    invocation_id,
                    AuditAction::AuthorityChange,
                    vec![reader.user_id, object_id],
                ),
            })
            .expect("grant migration reader access");
    }
    source_storage
        .set_object_grant(prdb_engine::storage_manager::SetObjectGrantRequest {
            invocation_id: [0xac; 16],
            authenticated_actor_id: admin_id,
            grantee_id: reader.user_id,
            object_id: table.table_id,
            privilege: prdb_engine::storage_manager::ObjectGrantPrivilege::Select,
            target: prdb_engine::storage_manager::ObjectGrantTarget::AllColumns,
            grant: true,
            retention_deadline_us: i64::MAX,
            audit_event: migration_audit(
                0xac,
                [0xac; 16],
                AuditAction::AuthorityChange,
                vec![reader.user_id, table.table_id],
            ),
        })
        .expect("grant migration reader column access");
    source_storage
        .create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
            invocation_id: [0xa7; 16],
            authenticated_actor_id: admin_id,
            table_id: table.table_id,
            name: "migration_visible_values".to_owned(),
            kind: prdb_engine::storage_manager::PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients: vec![prdb_engine::authorization::Subject::Principal(
                reader.user_id,
            )],
            target_column_ids: Vec::new(),
            using_expression: Some(prdb_engine::authorization::Expr::Equal(
                Box::new(prdb_engine::authorization::Expr::Field(0)),
                Box::new(prdb_engine::authorization::Expr::Literal(Some(
                    prdb_engine::tuple::ScalarValue::Text("visible".to_owned()),
                ))),
            )),
            with_check_expression: None,
            retention_deadline_us: i64::MAX,
            audit_event: migration_audit(0xa7, [0xa7; 16], AuditAction::Ddl, Vec::new()),
        })
        .expect("create migration row policy");
    source_storage
        .create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
            invocation_id: [0xa8; 16],
            authenticated_actor_id: admin_id,
            table_id: table.table_id,
            name: "migration_restrict_values".to_owned(),
            kind: prdb_engine::storage_manager::PolicyKind::Cell,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Restrict,
            recipients: vec![prdb_engine::authorization::Subject::Principal(
                reader.user_id,
            )],
            target_column_ids: vec![table.column_ids[0]],
            using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                prdb_engine::tuple::ScalarValue::Boolean(false),
            ))),
            with_check_expression: None,
            retention_deadline_us: i64::MAX,
            audit_event: migration_audit(0xa8, [0xa8; 16], AuditAction::Ddl, Vec::new()),
        })
        .expect("create migration cell restriction");
    let mut seed = source_storage
        .begin_invocation([0xab; 16], admin_id, i64::MAX, 4, 4096)
        .expect("begin migration fixture row seed");
    for (row_id, value) in [([0xb1; 16], "visible"), ([0xb2; 16], "hidden")] {
        let payload = prdb_engine::tuple::encode_tuple(
            &[prdb_engine::tuple::ColumnSchema {
                scalar_type: prdb_engine::tuple::ScalarType::Text,
                nullable: false,
            }],
            1,
            &[Some(prdb_engine::tuple::ScalarValue::Text(
                value.to_owned(),
            ))],
        )
        .expect("encode migration fixture row");
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: table.table_id,
                row: row_id,
            },
            &payload,
        )
        .expect("stage migration fixture row");
    }
    seed.commit().expect("commit migration fixture rows");
    let visible_relations = source_storage
        .discover_token_authorized_relations(reader_token_id, &reader_secret, schema.schema_id)
        .expect("discover migration reader relations")
        .expect("migration reader can use the schema");
    assert!(
        visible_relations
            .iter()
            .any(|relation| relation.relation_id == table.table_id),
        "table grant should make the migration fixture relation discoverable"
    );
    let (mut source_reader, source_roles) = source_storage
        .begin_token_authenticated_invocation(
            [0xaa; 16],
            reader_token_id,
            &reader_secret,
            i64::MAX,
            4,
            4096,
        )
        .expect("admit migration reader before conversion");
    assert!(source_roles.is_empty());
    let raw_visible = source_reader
        .read(prdb_engine::overlay::RowId {
            table: table.table_id,
            row: [0xb1; 16],
        })
        .expect("read source visible row")
        .expect("source visible row is stored");
    assert_eq!(
        prdb_engine::tuple::decode_tuple(
            &[prdb_engine::tuple::ColumnSchema {
                scalar_type: prdb_engine::tuple::ScalarType::Text,
                nullable: false,
            }],
            1,
            &raw_visible,
        )
        .expect("decode source visible row")
        .values,
        [Some(prdb_engine::tuple::ScalarValue::Text(
            "visible".to_owned()
        ))]
    );
    assert_eq!(
        source_reader
            .read_catalog_authorized_row(table.table_id, [0xb1; 16])
            .expect("read source row permitted by policy"),
        Some(vec![prdb_engine::authorization::SanitizedCell::Redacted])
    );
    assert_eq!(
        source_reader
            .read_catalog_authorized_row(table.table_id, [0xb2; 16])
            .expect("read hidden source row"),
        None
    );
    source_reader
        .commit()
        .expect("commit source authorization check");
    drop(source_storage);
    drop(source_database);

    let metadata = downgrade_ready_fixture_to_v1(&source_path);
    let backup_path = fixture.0.join("verified-v1-backup");
    prdb_engine::offline_backup::create_verified_migration_backup(
        fixture.instance_dir(),
        &backup_path,
    )
    .expect("create verified v1 backup");
    let history_path = fixture.0.join("identity-history.txt");
    let history = prdb_engine::identity_history::IdentityHistory {
        database_id: metadata.database_id,
        commit_sequence: metadata.commit_sequence,
        tombstone_ids: vec![[0xfe; 16]],
    };
    std::fs::write(
        &history_path,
        history.encode().expect("encode identity history"),
    )
    .expect("write identity history");
    std::fs::set_permissions(&history_path, std::fs::Permissions::from_mode(0o600))
        .expect("restrict identity history permissions");

    let rejected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-apply", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--identity-history")
        .arg(&history_path)
        .output()
        .expect("run migration without explicit complete-history acknowledgment");
    assert!(!rejected.status.success());
    assert!(!fixture.instance_dir().join(".prdb-migration.next").exists());

    let interrupted_stage = fixture.instance_dir().join(".prdb-migration.next");
    std::fs::copy(&source_path, &interrupted_stage).expect("copy v1 source into staging");
    let interrupted_target = Database::open(&interrupted_stage).expect("open v1 staging copy");
    prdb_engine::migration::upgrade_v1_staging_database(&interrupted_target, &history)
        .expect("convert interrupted target before publication");
    {
        let write = interrupted_target
            .begin_write()
            .expect("begin target corruption fixture");
        write
            .open_table(STATS)
            .expect("open statistics table")
            .insert(&b"unexpected-migration-state"[..], &b"not-from-source"[..])
            .expect("add data not present in the active source");
        write.commit().expect("commit target-only change");
    }
    drop(interrupted_target);

    let applied = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-apply", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--identity-history")
        .arg(&history_path)
        .arg("--confirm-complete-identity-history")
        .output()
        .expect("run v1-to-v2 migration");
    assert!(
        applied.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&applied.stdout),
        String::from_utf8_lossy(&applied.stderr)
    );
    assert!(String::from_utf8_lossy(&applied.stdout).contains("migration=published"));

    let active = StorageInstance::open(fixture.instance_dir()).expect("open migrated instance");
    let active_report =
        prdb_engine::startup::validate_persisted_records(active.database()).expect("validate v2");
    assert_eq!(active_report.metadata.catalog_version, 2);
    assert_eq!(active_report.metadata.database_id, metadata.database_id);
    assert_eq!(
        active_report.metadata.commit_sequence,
        metadata.commit_sequence
    );
    let migrated_storage = prdb_engine::storage_manager::StorageManager::new(
        active.database(),
        std::time::Duration::from_millis(100),
    )
    .expect("create storage manager for migrated catalog");
    let authenticated = migrated_storage
        .authenticate_query_session(token_id, &secret)
        .expect("pre-migration bootstrap token remains valid after migration");
    assert_eq!(authenticated.principal_id(), admin_id);
    let (mut reader_invocation, reader_roles) = migrated_storage
        .begin_token_authenticated_invocation(
            [0xa9; 16],
            reader_token_id,
            &reader_secret,
            i64::MAX,
            4,
            4096,
        )
        .expect("migrated reader token retains access");
    assert!(reader_roles.is_empty());
    assert_eq!(
        reader_invocation
            .read_catalog_authorized_row(table.table_id, [0xb1; 16])
            .expect("read row permitted by migrated policy"),
        Some(vec![prdb_engine::authorization::SanitizedCell::Redacted])
    );
    assert_eq!(
        reader_invocation
            .read_catalog_authorized_row(table.table_id, [0xb2; 16])
            .expect("read hidden row through migrated policy"),
        None
    );
    reader_invocation
        .commit()
        .expect("commit read-only migration authorization check");
    drop(migrated_storage);
    let read = active
        .database()
        .begin_read()
        .expect("read published stats");
    assert!(
        read.open_table(STATS)
            .expect("open published stats")
            .get(&b"unexpected-migration-state"[..])
            .expect("check target-only state")
            .is_none()
    );
    drop(read);
    drop(active);

    let post_upgrade_schema_id;
    {
        let instance = StorageInstance::open(fixture.instance_dir())
            .expect("reopen migrated instance for post-upgrade write");
        let storage = prdb_engine::storage_manager::StorageManager::new(
            instance.database(),
            std::time::Duration::from_millis(100),
        )
        .expect("create migrated storage manager");
        post_upgrade_schema_id = storage
            .create_schema(
                [0x91; 16],
                admin_id,
                "post_upgrade_data",
                admin_id,
                i64::MAX,
                AuditEvent {
                    event_id: [0x92; 16],
                    actor_id: Some(admin_id),
                    correlation_id: [0x91; 16],
                    occurred_at_us: 1,
                    action: AuditAction::Ddl,
                    result: AuditResult::Succeeded,
                    target_ids: Vec::new(),
                },
            )
            .expect("commit schema after migration")
            .schema_id;
    }
    let active_after_write =
        ReadOnlyDatabase::open(&source_path).expect("open post-migration active image read-only");
    let post_upgrade_report =
        prdb_engine::startup::validate_persisted_records_read_only(&active_after_write)
            .expect("validate active image after post-upgrade write");
    assert_eq!(post_upgrade_report.metadata.catalog_version, 2);
    assert!(post_upgrade_report.metadata.commit_sequence > metadata.commit_sequence);
    assert_eq!(
        prdb_engine::catalog_index::lookup_schema_id_by_name(
            &active_after_write
                .begin_read()
                .expect("read upgraded catalog"),
            "post_upgrade_data",
        )
        .expect("lookup post-upgrade schema"),
        Some(post_upgrade_schema_id)
    );
    let post_upgrade_sequence = post_upgrade_report.metadata.commit_sequence;
    drop(active_after_write);

    let repeated_migration = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-apply", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--identity-history")
        .arg(&history_path)
        .arg("--confirm-complete-identity-history")
        .output()
        .expect("attempt migration again after a post-upgrade write");
    assert!(
        !repeated_migration.status.success(),
        "migration unexpectedly replaced a newer active generation: stdout={} stderr={}",
        String::from_utf8_lossy(&repeated_migration.stdout),
        String::from_utf8_lossy(&repeated_migration.stderr)
    );
    let active_after_retry = ReadOnlyDatabase::open(&source_path)
        .expect("reopen active image after rejected repeat migration");
    let retry_report =
        prdb_engine::startup::validate_persisted_records_read_only(&active_after_retry)
            .expect("validate active image after rejected repeat migration");
    assert_eq!(retry_report.metadata.catalog_version, 2);
    assert_eq!(retry_report.metadata.commit_sequence, post_upgrade_sequence);
    assert_eq!(
        prdb_engine::catalog_index::lookup_schema_id_by_name(
            &active_after_retry
                .begin_read()
                .expect("read post-retry catalog"),
            "post_upgrade_data",
        )
        .expect("verify post-upgrade schema survived retry"),
        Some(post_upgrade_schema_id)
    );
    drop(active_after_retry);

    let retained = ReadOnlyDatabase::open(fixture.instance_dir().join(".prdb-migration.next"))
        .expect("open retained v1 source");
    let retained_report = prdb_engine::startup::validate_v1_migration_source_read_only(&retained)
        .expect("validate retained source generation");
    assert_eq!(retained_report.metadata.database_id, metadata.database_id);
    assert_eq!(
        retained_report.metadata.commit_sequence,
        metadata.commit_sequence
    );
    drop(retained);
    assert!(
        prdb_engine::offline_backup::database_image_matches_backup(
            fixture.instance_dir().join(".prdb-migration.next"),
            &prdb_engine::offline_backup::verify_backup(&backup_path).expect("verify backup")
        )
        .expect("compare retained source with backup")
    );
    let inspected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-inspect", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .output()
        .expect("inspect migration generations after publication");
    assert!(
        inspected.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let inspection = String::from_utf8(inspected.stdout).expect("UTF-8 inspection output");
    assert!(inspection.contains("active=READY"), "{inspection}");
    assert!(inspection.contains("catalog_version=2"), "{inspection}");
    assert!(inspection.contains("staging=READY"), "{inspection}");
    assert!(inspection.contains("catalog_version=1"), "{inspection}");
    assert!(
        inspection.contains("backup=VERIFIED active_matches=false staging_matches=true"),
        "{inspection}"
    );
    let retained_bytes = std::fs::read(fixture.instance_dir().join(".prdb-migration.next"))
        .expect("read retained source before discard attempt");
    let discard_after_publication = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-discard-stage", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .output()
        .expect("attempt to discard retained source after publication");
    assert!(!discard_after_publication.status.success());
    assert_eq!(
        std::fs::read(fixture.instance_dir().join(".prdb-migration.next"))
            .expect("reread retained source"),
        retained_bytes,
        "post-publication cleanup must preserve the rollback evidence"
    );
}

pub(super) fn migration_apply_resumes_a_fully_validated_v2_staging_image() {
    let fixture = Fixture::new();
    let mut session = prdb_engine::bootstrap::initialize(
        fixture.instance_dir(),
        "migration_resume_admin",
        i64::MAX,
    )
    .expect("initialize migration source");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish source READY"));

    let source_path = fixture.instance_dir().join("storage.redb");
    let metadata = downgrade_ready_fixture_to_v1(&source_path);
    let backup_path = fixture.0.join("resume-v1-backup");
    prdb_engine::offline_backup::create_verified_migration_backup(
        fixture.instance_dir(),
        &backup_path,
    )
    .expect("create verified v1 backup");
    let history_path = fixture.0.join("resume-identity-history.txt");
    let history = prdb_engine::identity_history::IdentityHistory {
        database_id: metadata.database_id,
        commit_sequence: metadata.commit_sequence,
        tombstone_ids: vec![[0xfc; 16]],
    };
    std::fs::write(
        &history_path,
        history.encode().expect("encode identity history"),
    )
    .expect("write identity history");
    std::fs::set_permissions(&history_path, std::fs::Permissions::from_mode(0o600))
        .expect("restrict identity history permissions");

    let source_bytes = std::fs::read(&source_path).expect("read exact v1 source bytes");
    let staging_path = fixture.instance_dir().join(".prdb-migration.next");
    std::fs::copy(&source_path, &staging_path).expect("copy source into staging");
    let staging = Database::open(&staging_path).expect("open staging source copy");
    prdb_engine::migration::upgrade_v1_staging_database(&staging, &history)
        .expect("prepare fully converted staging image");
    drop(staging);
    let prepared = ReadOnlyDatabase::open(&staging_path).expect("open prepared v2 staging image");
    let prepared_report = prdb_engine::migration::validate_v2_staging_history(
        &ReadOnlyDatabase::open(&source_path).expect("open locked v1 source image"),
        &prepared,
        &history,
    )
    .expect("validate resumable staging image");
    assert_eq!(prepared_report.metadata.catalog_version, 2);
    drop(prepared);

    let interrupted = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-apply", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--identity-history")
        .arg(&history_path)
        .arg("--confirm-complete-identity-history")
        .env("PRDB_TEST_MIGRATION_EXIT_BEFORE_EXCHANGE", "1")
        .output()
        .expect("interrupt migration before publication exchange");
    assert_eq!(interrupted.status.code(), Some(85));
    assert_eq!(
        std::fs::read(&source_path).expect("read active source after process exit"),
        source_bytes,
        "the v1 source must remain untouched before publication"
    );
    let resumable = ReadOnlyDatabase::open(&staging_path)
        .expect("open converted staging image after process exit");
    let resumable_report = prdb_engine::startup::validate_persisted_records_read_only(&resumable)
        .expect("validate converted staging image after process exit");
    assert_eq!(resumable_report.metadata.catalog_version, 2);
    assert_eq!(resumable_report.metadata.database_id, metadata.database_id);
    assert_eq!(
        resumable_report.metadata.commit_sequence,
        metadata.commit_sequence
    );
    drop(resumable);

    let applied = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-apply", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--identity-history")
        .arg(&history_path)
        .arg("--confirm-complete-identity-history")
        .output()
        .expect("resume and publish v2 staging image");
    assert!(
        applied.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&applied.stdout),
        String::from_utf8_lossy(&applied.stderr)
    );
    let migrated = StorageInstance::open(fixture.instance_dir())
        .expect("open instance published from resumed staging");
    let report = prdb_engine::startup::validate_persisted_records(migrated.database())
        .expect("validate resumed v2 image");
    assert_eq!(report.metadata.catalog_version, 2);
    assert_eq!(report.metadata.database_id, metadata.database_id);
    assert_eq!(report.metadata.commit_sequence, metadata.commit_sequence);
    drop(migrated);
}

pub(super) fn migration_apply_keeps_the_published_target_after_process_exit_at_exchange() {
    let fixture = Fixture::new();
    let mut session = prdb_engine::bootstrap::initialize(
        fixture.instance_dir(),
        "migration_exchange_admin",
        i64::MAX,
    )
    .expect("initialize migration source");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish source READY"));

    let source_path = fixture.instance_dir().join("storage.redb");
    let metadata = downgrade_ready_fixture_to_v1(&source_path);
    let source_bytes = std::fs::read(&source_path).expect("read exact v1 source bytes");
    let backup_path = fixture.0.join("exchange-v1-backup");
    prdb_engine::offline_backup::create_verified_migration_backup(
        fixture.instance_dir(),
        &backup_path,
    )
    .expect("create verified source backup");
    let history_path = fixture.0.join("exchange-identity-history.txt");
    let history = prdb_engine::identity_history::IdentityHistory {
        database_id: metadata.database_id,
        commit_sequence: metadata.commit_sequence,
        tombstone_ids: vec![[0xfb; 16]],
    };
    std::fs::write(
        &history_path,
        history.encode().expect("encode identity history"),
    )
    .expect("write identity history");
    std::fs::set_permissions(&history_path, std::fs::Permissions::from_mode(0o600))
        .expect("restrict identity history permissions");

    let interrupted = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-apply", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--identity-history")
        .arg(&history_path)
        .arg("--confirm-complete-identity-history")
        .env("PRDB_TEST_MIGRATION_EXIT_AFTER_EXCHANGE", "1")
        .output()
        .expect("interrupt migration after publication exchange");
    assert_eq!(interrupted.status.code(), Some(86));

    let active = ReadOnlyStorageInstance::open(fixture.instance_dir())
        .expect("open published target after process exit");
    let active_report =
        prdb_engine::startup::validate_persisted_records_read_only(active.database())
            .expect("validate active target after process exit");
    assert_eq!(active_report.metadata.catalog_version, 2);
    assert_eq!(active_report.metadata.database_id, metadata.database_id);
    assert_eq!(
        active_report.metadata.commit_sequence,
        metadata.commit_sequence
    );
    drop(active);

    let retained_source_path = fixture.instance_dir().join(".prdb-migration.next");
    assert_eq!(
        std::fs::read(&retained_source_path).expect("read retained v1 source"),
        source_bytes,
        "the pre-migration source must remain byte-identical after exchange"
    );
    let retained_source = ReadOnlyDatabase::open(&retained_source_path)
        .expect("open retained source after process exit");
    let retained_report =
        prdb_engine::startup::validate_v1_migration_source_read_only(&retained_source)
            .expect("validate retained catalog-v1 source");
    assert_eq!(retained_report.metadata.database_id, metadata.database_id);
    assert_eq!(
        retained_report.metadata.commit_sequence,
        metadata.commit_sequence
    );
}

pub(super) fn migration_apply_recovers_after_directory_sync_failure_at_exchange() {
    let fixture = Fixture::new();
    let mut session = prdb_engine::bootstrap::initialize(
        fixture.instance_dir(),
        "migration_sync_admin",
        i64::MAX,
    )
    .expect("initialize migration source");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish source READY"));

    let source_path = fixture.instance_dir().join("storage.redb");
    let metadata = downgrade_ready_fixture_to_v1(&source_path);
    let source_bytes = std::fs::read(&source_path).expect("read exact v1 source bytes");
    let backup_path = fixture.0.join("sync-v1-backup");
    prdb_engine::offline_backup::create_verified_migration_backup(
        fixture.instance_dir(),
        &backup_path,
    )
    .expect("create verified source backup");
    let history_path = fixture.0.join("sync-identity-history.txt");
    let history = prdb_engine::identity_history::IdentityHistory {
        database_id: metadata.database_id,
        commit_sequence: metadata.commit_sequence,
        tombstone_ids: vec![[0xfa; 16]],
    };
    std::fs::write(
        &history_path,
        history.encode().expect("encode identity history"),
    )
    .expect("write identity history");
    std::fs::set_permissions(&history_path, std::fs::Permissions::from_mode(0o600))
        .expect("restrict identity history permissions");

    let failed = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-apply", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--identity-history")
        .arg(&history_path)
        .arg("--confirm-complete-identity-history")
        .env("PRDB_TEST_MIGRATION_FAIL_SYNC_AFTER_EXCHANGE", "1")
        .output()
        .expect("inject migration directory-sync failure after exchange");
    assert!(!failed.status.success());
    assert!(
        String::from_utf8_lossy(&failed.stderr)
            .contains("injected migration directory sync failure after exchange"),
        "stderr: {}",
        String::from_utf8_lossy(&failed.stderr)
    );

    let active = ReadOnlyStorageInstance::open(fixture.instance_dir())
        .expect("open published target after directory-sync failure");
    let active_report =
        prdb_engine::startup::validate_persisted_records_read_only(active.database())
            .expect("validate active target after directory-sync failure");
    assert_eq!(active_report.metadata.catalog_version, 2);
    assert_eq!(active_report.metadata.database_id, metadata.database_id);
    assert_eq!(
        active_report.metadata.commit_sequence,
        metadata.commit_sequence
    );
    drop(active);

    let retained_path = fixture.instance_dir().join(".prdb-migration.next");
    assert_eq!(
        std::fs::read(&retained_path).expect("read retained v1 source"),
        source_bytes,
        "the pre-migration source must remain byte-identical after the sync error"
    );
    let retained = ReadOnlyDatabase::open(&retained_path).expect("open retained v1 source");
    let retained_report = prdb_engine::startup::validate_v1_migration_source_read_only(&retained)
        .expect("validate retained catalog-v1 source");
    assert_eq!(retained_report.metadata.database_id, metadata.database_id);
    assert_eq!(
        retained_report.metadata.commit_sequence,
        metadata.commit_sequence
    );
}

pub(super) fn restore_requires_data_loss_ack_and_recovers_an_unreadable_newer_active_image() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "restore_admin", i64::MAX)
            .expect("initialize restore instance");
    let admin_id = session.admin_user_id();
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish READY"));

    let active_path = fixture.instance_dir().join("storage.redb");
    let backup_path = fixture.0.join("restore-backup");
    let backup =
        prdb_engine::offline_backup::create_verified_backup(fixture.instance_dir(), &backup_path)
            .expect("create verified restore backup");
    let expected_bytes = std::fs::read(&active_path).expect("read original active image");
    {
        let instance = StorageInstance::open(fixture.instance_dir()).expect("open active instance");
        let storage = prdb_engine::storage_manager::StorageManager::new(
            instance.database(),
            std::time::Duration::from_millis(100),
        )
        .expect("create storage manager");
        storage
            .create_schema(
                [0x51; 16],
                admin_id,
                "restore_newer_generation",
                admin_id,
                i64::MAX,
                AuditEvent {
                    event_id: [0x52; 16],
                    actor_id: Some(admin_id),
                    correlation_id: [0x51; 16],
                    occurred_at_us: 81,
                    action: AuditAction::Ddl,
                    result: AuditResult::Succeeded,
                    target_ids: Vec::new(),
                },
            )
            .expect("commit newer schema generation");
    }
    let newer_report = {
        let current = ReadOnlyDatabase::open(&active_path).expect("open newer image read-only");
        prdb_engine::startup::validate_persisted_records_read_only(&current)
            .expect("validate newer image")
    };
    assert!(newer_report.metadata.commit_sequence > backup.commit_sequence);
    let newer_generation = std::fs::read(&active_path).expect("read newer active image");
    assert_ne!(newer_generation, expected_bytes);
    let mut corrupted_bytes = newer_generation;
    corrupted_bytes[0] ^= 0xff;
    std::fs::write(&active_path, &corrupted_bytes).expect("corrupt active redb header");
    let interrupted_copy = fixture.instance_dir().join(".prdb-restore.copying");
    std::fs::write(&interrupted_copy, b"interrupted backup copy")
        .expect("write interrupted restore copy");
    std::fs::set_permissions(&interrupted_copy, std::fs::Permissions::from_mode(0o600))
        .expect("restrict interrupted restore copy");
    assert!(StorageInstance::open(fixture.instance_dir()).is_err());

    let missing_ack = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["restore", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .output()
        .expect("run restore without data-loss acknowledgment");
    assert!(!missing_ack.status.success());
    assert_eq!(
        std::fs::read(&active_path).expect("read still-corrupt active image"),
        corrupted_bytes
    );

    let failed_after_exchange = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["restore", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--confirm-data-loss")
        .env("PRDB_TEST_RESTORE_FAIL_SYNC_AFTER_EXCHANGE", "1")
        .output()
        .expect("inject restore directory-sync failure after exchange");
    assert!(!failed_after_exchange.status.success());
    assert!(
        String::from_utf8_lossy(&failed_after_exchange.stderr)
            .contains("injected restore directory sync failure after exchange"),
        "stderr: {}",
        String::from_utf8_lossy(&failed_after_exchange.stderr)
    );
    assert_eq!(
        std::fs::read(&active_path).expect("read restored active bytes after sync failure"),
        expected_bytes
    );
    assert_eq!(
        std::fs::read(fixture.instance_dir().join(".prdb-migration.next"))
            .expect("read retained displaced image after sync failure"),
        corrupted_bytes
    );
    drop(
        ReadOnlyStorageInstance::open(fixture.instance_dir())
            .expect("open validated restored generation after sync failure"),
    );

    let retry = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["restore", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--confirm-data-loss")
        .output()
        .expect("retry restore after directory-sync failure");
    assert!(
        retry.status.success(),
        "restore retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(String::from_utf8_lossy(&retry.stdout).contains("restore=already-active"));
    assert_eq!(
        std::fs::read(&active_path).expect("verify active restore after retry"),
        expected_bytes
    );
    assert_eq!(
        std::fs::read(fixture.instance_dir().join(".prdb-migration.next"))
            .expect("verify retained displaced image after retry"),
        corrupted_bytes
    );
    assert!(!interrupted_copy.exists());
    let restored_instance =
        StorageInstance::open(fixture.instance_dir()).expect("open restored READY instance");
    let report = prdb_engine::startup::validate_persisted_records(restored_instance.database())
        .expect("validate restored backup generation");
    assert_eq!(report.metadata.catalog_version, 2);
    drop(restored_instance);
}

pub(super) fn restore_refuses_to_overwrite_an_unrelated_staging_image() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "restore_admin", i64::MAX)
            .expect("initialize restore instance");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish READY"));

    let active_path = fixture.instance_dir().join("storage.redb");
    let backup_path = fixture.0.join("restore-backup");
    prdb_engine::offline_backup::create_verified_backup(fixture.instance_dir(), &backup_path)
        .expect("create verified restore backup");
    let mut active_bytes = std::fs::read(&active_path).expect("read active image");
    active_bytes[0] ^= 0xff;
    std::fs::write(&active_path, &active_bytes).expect("make active image differ from backup");
    let staging_path = fixture.instance_dir().join(".prdb-migration.next");
    let staging_bytes = b"unrelated private staging data";
    std::fs::write(&staging_path, staging_bytes).expect("write unrelated staging image");
    std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o600))
        .expect("restrict unrelated staging image");

    let rejected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["restore", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .arg("--confirm-data-loss")
        .output()
        .expect("refuse unrelated staging image");
    assert!(!rejected.status.success());
    assert_eq!(
        std::fs::read(&active_path).expect("read unchanged active image"),
        active_bytes
    );
    assert_eq!(
        std::fs::read(&staging_path).expect("read preserved staging image"),
        staging_bytes
    );
}

pub(super) fn startup_uses_active_image_and_tolerates_only_known_private_generation_files() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "migration_admin", i64::MAX)
            .expect("initialize migration fixture");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish READY"));

    let staging = fixture.instance_dir().join(".prdb-migration.next");
    std::fs::write(&staging, b"interrupted target generation")
        .expect("write interrupted migration target");
    std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o600))
        .expect("make migration target private");
    drop(StorageInstance::open(fixture.instance_dir()).expect("start from active source image"));

    std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o640))
        .expect("broaden staging file permissions");
    assert!(matches!(
        StorageInstance::open(fixture.instance_dir()),
        Err(BootstrapError::NotPrivate)
    ));
    std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o600))
        .expect("restore staging permissions");

    let restore_copy = fixture.instance_dir().join(".prdb-restore.copying");
    std::fs::write(&restore_copy, b"interrupted restore copy")
        .expect("write interrupted restore copy");
    std::fs::set_permissions(&restore_copy, std::fs::Permissions::from_mode(0o600))
        .expect("make restore copy private");
    drop(
        StorageInstance::open(fixture.instance_dir()).expect("start with interrupted restore copy"),
    );
    std::fs::set_permissions(&restore_copy, std::fs::Permissions::from_mode(0o640))
        .expect("broaden restore copy permissions");
    assert!(matches!(
        StorageInstance::open(fixture.instance_dir()),
        Err(BootstrapError::NotPrivate)
    ));
    std::fs::set_permissions(&restore_copy, std::fs::Permissions::from_mode(0o600))
        .expect("restore restore-copy permissions");

    let unexpected = fixture.instance_dir().join(".prdb-migration.extra");
    std::fs::write(&unexpected, b"unknown file").expect("write unexpected file");
    std::fs::set_permissions(&unexpected, std::fs::Permissions::from_mode(0o600))
        .expect("make unexpected file private");
    assert!(matches!(
        StorageInstance::open(fixture.instance_dir()),
        Err(BootstrapError::NotPrivate)
    ));
}

pub(super) fn staged_generation_publication_atomically_exchanges_names_and_retains_source() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "publish_admin", i64::MAX)
            .expect("initialize publication fixture");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish READY"));

    let active_path = fixture.instance_dir().join("storage.redb");
    let staging_path = fixture.instance_dir().join(".prdb-migration.next");
    let backup_path = fixture.0.join("verified-backup");
    prdb_engine::offline_backup::create_verified_backup(fixture.instance_dir(), &backup_path)
        .expect("create independently verified source backup");
    std::fs::copy(&active_path, &staging_path).expect("prepare staged database image");
    std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o600))
        .expect("make staged database private");
    let active_inode = std::fs::metadata(&active_path).unwrap().ino();
    let staged_inode = std::fs::metadata(&staging_path).unwrap().ino();

    let active = StorageInstance::open(fixture.instance_dir()).expect("open active generation");
    let published = active
        .publish_staged_generation(&backup_path)
        .expect("atomically publish staged generation");
    assert_eq!(std::fs::metadata(&active_path).unwrap().ino(), staged_inode);
    assert_eq!(
        std::fs::metadata(&staging_path).unwrap().ino(),
        active_inode
    );
    drop(published);
    drop(StorageInstance::open(fixture.instance_dir()).expect("reopen published generation"));
}

pub(super) fn staged_generation_publication_rejects_different_database_identity() {
    let source = Fixture::new();
    let other = Fixture::new();
    for (fixture, name) in [(&source, "source_admin"), (&other, "other_admin")] {
        let mut session =
            prdb_engine::bootstrap::initialize(fixture.instance_dir(), name, i64::MAX)
                .expect("initialize publication fixture");
        let mut secret = session.take_secret().expect("bootstrap secret");
        use zeroize::Zeroize;
        secret.zeroize();
        drop(session.acknowledge().expect("publish READY"));
    }

    let active_path = source.instance_dir().join("storage.redb");
    let staging_path = source.instance_dir().join(".prdb-migration.next");
    let backup_path = source.0.join("verified-backup");
    prdb_engine::offline_backup::create_verified_backup(source.instance_dir(), &backup_path)
        .expect("create independently verified source backup");
    std::fs::copy(other.instance_dir().join("storage.redb"), &staging_path)
        .expect("copy a different READY database");
    std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o600))
        .expect("make staged database private");
    let active_inode = std::fs::metadata(&active_path).unwrap().ino();
    let staged_inode = std::fs::metadata(&staging_path).unwrap().ino();

    let error = match StorageInstance::open(source.instance_dir())
        .expect("open source generation")
        .publish_staged_generation(&backup_path)
    {
        Ok(_) => panic!("reject different database identity"),
        Err(error) => error,
    };
    assert!(matches!(error, BootstrapError::InvalidRequest));
    assert_eq!(std::fs::metadata(&active_path).unwrap().ino(), active_inode);
    assert_eq!(
        std::fs::metadata(&staging_path).unwrap().ino(),
        staged_inode
    );
    drop(StorageInstance::open(source.instance_dir()).expect("source generation remains active"));
}

pub(super) fn staged_generation_publication_requires_a_matching_verified_backup() {
    let source = Fixture::new();
    let other = Fixture::new();
    for (fixture, name) in [(&source, "source_backup_admin"), (&other, "backup_admin")] {
        let mut session =
            prdb_engine::bootstrap::initialize(fixture.instance_dir(), name, i64::MAX)
                .expect("initialize publication fixture");
        let mut secret = session.take_secret().expect("bootstrap secret");
        use zeroize::Zeroize;
        secret.zeroize();
        drop(session.acknowledge().expect("publish READY"));
    }

    let backup_path = other.0.join("verified-backup");
    prdb_engine::offline_backup::create_verified_backup(other.instance_dir(), &backup_path)
        .expect("create an unrelated verified backup");
    let active_path = source.instance_dir().join("storage.redb");
    let staging_path = source.instance_dir().join(".prdb-migration.next");
    std::fs::copy(&active_path, &staging_path).expect("prepare staged source image");
    std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o600))
        .expect("make staged database private");
    let active_inode = std::fs::metadata(&active_path).unwrap().ino();
    let staged_inode = std::fs::metadata(&staging_path).unwrap().ino();

    let error = match StorageInstance::open(source.instance_dir())
        .expect("open source generation")
        .publish_staged_generation(&backup_path)
    {
        Ok(_) => panic!("reject unrelated verified backup"),
        Err(error) => error,
    };
    assert!(matches!(error, BootstrapError::InvalidRequest));
    assert_eq!(std::fs::metadata(&active_path).unwrap().ino(), active_inode);
    assert_eq!(
        std::fs::metadata(&staging_path).unwrap().ino(),
        staged_inode
    );
    drop(StorageInstance::open(source.instance_dir()).expect("source generation remains active"));
}

pub(super) fn maintenance_verifies_pra1_archives_and_rejects_corruption()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let event = AuditEvent {
        event_id: [0x41; 16],
        actor_id: Some([0x42; 16]),
        correlation_id: [0x43; 16],
        occurred_at_us: 1,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[0x44; 16]],
    };
    let archive = prdb_engine::audit_archive::encode(&[(event.event_id, event.encode()?)])?;
    let archive_path = fixture.0.join("audit.archive");
    std::fs::write(&archive_path, &archive)?;
    std::fs::set_permissions(&archive_path, std::fs::Permissions::from_mode(0o600))?;

    let verified = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["audit-verify", "--archive"])
        .arg(&archive_path)
        .output()?;
    assert!(
        verified.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&verified.stderr)
    );
    assert!(String::from_utf8_lossy(&verified.stdout).contains("events=1"));

    let corrupted_path = fixture.0.join("corrupted.archive");
    let mut corrupted = archive;
    let last = corrupted.len() - 1;
    corrupted[last] ^= 1;
    std::fs::write(&corrupted_path, &corrupted)?;
    std::fs::set_permissions(&corrupted_path, std::fs::Permissions::from_mode(0o600))?;
    let rejected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["audit-verify", "--archive"])
        .arg(&corrupted_path)
        .output()?;
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("InvalidDigest"));
    assert_eq!(std::fs::read(corrupted_path)?, corrupted);
    Ok(())
}

pub(super) fn init_delivers_and_persists_one_time_credential_before_ready() {
    let fixture = Fixture::new();
    let output = initialize_command(&fixture, false);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("database is READY"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("token_secret"));
    let metadata = std::fs::metadata(fixture.credential_file()).expect("credential metadata");
    assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
    let credentials = read_client_credentials(fixture.credential_file())
        .expect("read saved bootstrap credential");
    assert_ne!(credentials.token_id, [0; 16]);
    drop(credentials);
    drop(StorageInstance::open(fixture.instance_dir()).expect("open READY instance"));
}

pub(super) fn init_resumes_interrupted_bootstrap_and_rotates_the_credential() {
    let fixture = Fixture::new();
    let session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "first_admin", i64::MAX)
            .expect("create interrupted bootstrap");
    let original_token = session.token_id();
    drop(session);

    let output = initialize_command(&fixture, true);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let credentials = read_client_credentials(fixture.credential_file())
        .expect("read replacement bootstrap credential");
    assert_ne!(credentials.token_id, original_token);
    drop(credentials);
    drop(StorageInstance::open(fixture.instance_dir()).expect("open resumed READY instance"));
}

pub(super) fn maintenance_compaction_requires_exclusive_instance_lock() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "maint_admin", i64::MAX)
            .expect("initialize maintenance fixture");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    let ready = session.acknowledge().expect("publish READY instance");

    let blocked = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["compact", "--instance-dir"])
        .arg(fixture.instance_dir())
        .output()
        .expect("run maintenance while instance is held");
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("InstanceLocked"));
    drop(ready);

    let compacted = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["compact", "--instance-dir"])
        .arg(fixture.instance_dir())
        .output()
        .expect("run offline maintenance");
    assert!(
        compacted.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&compacted.stderr)
    );
    assert!(String::from_utf8_lossy(&compacted.stdout).contains("compaction performed="));
    drop(StorageInstance::open(fixture.instance_dir()).expect("reopen READY instance"));
}

pub(super) fn audit_archive_is_durable_before_events_are_removed_and_refuses_overwrite()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "archive_admin", i64::MAX)?;
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge()?);

    let instance = StorageInstance::open(fixture.instance_dir())?;
    let write = instance.database().begin_write()?;
    {
        let mut audit = write.open_table(AUDIT)?;
        for byte in [0x21u8, 0x42] {
            let event = AuditEvent {
                event_id: [byte; 16],
                actor_id: Some([0x10; 16]),
                correlation_id: [byte.wrapping_add(1); 16],
                occurred_at_us: i64::from(byte),
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: vec![[0x30; 16]],
            };
            audit.insert(event.event_id.as_slice(), event.encode()?.as_slice())?;
        }
    }
    write.commit()?;
    let sink_path = fixture.0.join("audit.jsonl");
    assert_eq!(
        prdb_engine::audit_sink::deliver_jsonl_batch(instance.database(), &sink_path, 8)?,
        3
    );
    drop(instance);

    let archive_path = fixture.0.join("audit.archive");
    let archived = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["audit-archive", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output")
        .arg(&archive_path)
        .output()?;
    assert!(
        archived.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&archived.stderr)
    );
    assert!(String::from_utf8_lossy(&archived.stdout).contains("events=3"));
    assert_eq!(
        std::fs::metadata(&archive_path)?.permissions().mode() & 0o7777,
        0o600
    );
    let archive = std::fs::read(&archive_path)?;
    assert!(archive.starts_with(b"PRA1"));
    let mut reopened = StorageInstance::open(fixture.instance_dir())?;
    assert_eq!(
        reopened.database().begin_read()?.open_table(AUDIT)?.len()?,
        0
    );
    let read = reopened.database().begin_read()?;
    let stats = read.open_table(prdb_engine::schema::STATS)?;
    assert!(!stats.iter()?.any(|entry| {
        entry
            .expect("valid stats row")
            .0
            .value()
            .starts_with(b"audit_sink_delivered_v1:")
    }));
    drop(stats);
    drop(read);
    assert!(matches!(
        reopened.archive_audit_events(fixture.instance_dir().join("inside.archive")),
        Err(prdb_engine::bootstrap::AuditArchiveError::InvalidOutputPath)
    ));
    drop(reopened);

    let repeated = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["audit-archive", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output")
        .arg(&archive_path)
        .output()?;
    assert!(repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stdout).contains("events=0"));
    assert_eq!(std::fs::read(&archive_path)?, archive);
    Ok(())
}

pub(super) fn audit_archive_requires_the_exclusive_instance_lock_and_preserves_sentinel()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "archive_admin", i64::MAX)?;
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    let ready = session.acknowledge()?;
    let archive_path = fixture.0.join("audit.archive");
    std::fs::write(&archive_path, b"sentinel")?;
    std::fs::set_permissions(&archive_path, std::fs::Permissions::from_mode(0o600))?;

    let blocked = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["audit-archive", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output")
        .arg(&archive_path)
        .output()?;
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("InstanceLocked"));
    drop(ready);

    let refused = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["audit-archive", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output")
        .arg(&archive_path)
        .output()?;
    assert!(!refused.status.success());
    assert_eq!(std::fs::read(&archive_path)?, b"sentinel");
    let reopened = StorageInstance::open(fixture.instance_dir())?;
    assert_eq!(
        reopened.database().begin_read()?.open_table(AUDIT)?.len()?,
        1
    );
    Ok(())
}

pub(super) fn audit_archive_completes_pruning_from_a_matching_preexisting_archive()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "archive_admin", i64::MAX)?;
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge()?);

    let instance = StorageInstance::open(fixture.instance_dir())?;
    let read = instance.database().begin_read()?;
    let table = read.open_table(AUDIT)?;
    let events = table
        .iter()?
        .map(|entry| {
            let (key, value) = entry?;
            Ok::<_, redb::StorageError>((
                <[u8; 16]>::try_from(key.value()).expect("audit key width"),
                value.value().to_vec(),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert!(!events.is_empty());
    drop(table);
    drop(read);
    drop(instance);

    let archive_path = fixture.0.join("recovery.archive");
    std::fs::write(&archive_path, prdb_engine::audit_archive::encode(&events)?)?;
    std::fs::set_permissions(&archive_path, std::fs::Permissions::from_mode(0o600))?;
    std::fs::File::open(&archive_path)?.sync_all()?;
    std::fs::File::open(&fixture.0)?.sync_all()?;

    let archived = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["audit-archive", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output")
        .arg(&archive_path)
        .output()?;
    assert!(
        archived.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&archived.stderr)
    );
    assert!(
        String::from_utf8_lossy(&archived.stdout).contains(&format!("events={}", events.len()))
    );
    let reopened = StorageInstance::open(fixture.instance_dir())?;
    assert_eq!(
        reopened.database().begin_read()?.open_table(AUDIT)?.len()?,
        0
    );
    Ok(())
}

pub(super) fn init_refuses_to_overwrite_credential_files_before_creating_database() {
    let fixture = Fixture::new();
    std::fs::write(fixture.credential_file(), b"existing").expect("write sentinel");
    let output = initialize_command(&fixture, false);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("refusing to overwrite"));
    assert!(!fixture.instance_dir().exists());
    assert_eq!(
        std::fs::read(fixture.credential_file()).expect("read sentinel"),
        b"existing"
    );
}
