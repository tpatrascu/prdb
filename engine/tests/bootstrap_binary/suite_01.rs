use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::bootstrap::{BootstrapError, ReadOnlyStorageInstance, StorageInstance};
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::metadata::EngineMetadata;
use prdb_engine::schema::{CATALOG, ENGINE_META, POLICIES, ROWS};
use prdb_engine::startup::StartupError;
use redb::{
    Database, ReadOnlyDatabase, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition, TableHandle,
};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::process::Command;

use super::helpers::*;

pub(super) fn init_help_lists_required_bootstrap_inputs_cleanly() {
    let output = Command::new(env!("CARGO_BIN_EXE_prdb-init"))
        .arg("--help")
        .output()
        .expect("run prdb-init help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    for option in [
        "--instance-dir",
        "--administrator",
        "--credential-file",
        "--expires-in-seconds",
        "--resume",
    ] {
        assert!(help.contains(option), "missing {option} from help");
    }
    assert!(!help.contains('+'));
}

pub(super) fn maintenance_help_lists_offline_audit_archival() {
    let output = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("--help")
        .output()
        .expect("run maintenance help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(help.contains("audit-archive --instance-dir PATH --output PATH"));
    assert!(help.contains("audit-verify --archive PATH"));
    assert!(help.contains("backup --instance-dir PATH --output-dir PATH"));
    assert!(help.contains("backup-verify --backup-dir PATH"));
    assert!(help.contains("migration-backup --instance-dir PATH --output-dir PATH"));
    assert!(help.contains("migration-apply --instance-dir PATH --backup-dir PATH"));
    assert!(help.contains("restore --instance-dir PATH --backup-dir PATH --confirm-data-loss"));
    assert!(
        help.contains(
            "redb-format-upgrade --instance-dir PATH --backup-file PATH --confirm-redb-v2"
        )
    );
    assert!(help.contains("inspect --instance-dir PATH"));
    assert!(help.contains("migration-inspect --instance-dir PATH"));
    assert!(help.contains("migration-discard-stage --instance-dir PATH --backup-dir PATH"));
}

pub(super) fn maintenance_inspection_is_read_only_bounded_and_requires_offline_lock() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "inspect_admin", i64::MAX)
            .expect("initialize inspect fixture");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish READY"));

    let database_path = fixture.instance_dir().join("storage.redb");
    let before = std::fs::read(&database_path).expect("read database before inspection");
    let inspected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["inspect", "--instance-dir"])
        .arg(fixture.instance_dir())
        .output()
        .expect("run read-only inspect");
    assert!(
        inspected.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let output = String::from_utf8(inspected.stdout).expect("UTF-8 inspection result");
    for expected in [
        "status=READY",
        "catalog_version=2",
        "expression_version=1",
        "builtin_semantics_version=1",
        "catalog_records=",
        "rows=",
        "index_entries=",
    ] {
        assert!(output.contains(expected), "missing {expected}: {output}");
    }
    assert_eq!(
        std::fs::read(&database_path).expect("read database after inspection"),
        before,
        "inspection must not write to the redb image"
    );

    let live = StorageInstance::open(fixture.instance_dir()).expect("lock instance");
    let rejected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["inspect", "--instance-dir"])
        .arg(fixture.instance_dir())
        .output()
        .expect("attempt inspection during service ownership");
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("InstanceLocked"));
    drop(live);
}

pub(super) fn migration_inspection_validates_active_and_staged_images_without_writing_them() {
    let fixture = Fixture::new();
    let mut session = prdb_engine::bootstrap::initialize(
        fixture.instance_dir(),
        "migration_inspect_admin",
        i64::MAX,
    )
    .expect("initialize migration inspection fixture");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish READY"));

    let active_path = fixture.instance_dir().join("storage.redb");
    let staging_path = fixture.instance_dir().join(".prdb-migration.next");
    let backup_path = fixture.0.join("verified-backup");
    prdb_engine::offline_backup::create_verified_backup(fixture.instance_dir(), &backup_path)
        .expect("create source backup before inspection");
    std::fs::copy(&active_path, &staging_path).expect("prepare staged image");
    std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o600))
        .expect("make staged image private");
    let active_before = std::fs::read(&active_path).expect("read active image");
    let staging_before = std::fs::read(&staging_path).expect("read staged image");

    let inspected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-inspect", "--instance-dir"])
        .arg(fixture.instance_dir())
        .args(["--backup-dir"])
        .arg(&backup_path)
        .output()
        .expect("run migration inspection");
    assert!(
        inspected.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let output = String::from_utf8(inspected.stdout).expect("UTF-8 migration inspection result");
    assert!(output.contains("active=READY"), "{output}");
    assert!(
        output.contains("staging=READY relationship=same_frontier"),
        "{output}"
    );
    assert!(
        output.contains("backup=VERIFIED active_matches=true staging_matches=true"),
        "{output}"
    );
    assert_eq!(
        std::fs::read(&active_path).expect("reread active image"),
        active_before
    );
    assert_eq!(
        std::fs::read(&staging_path).expect("reread staged image"),
        staging_before
    );

    let live = StorageInstance::open(fixture.instance_dir()).expect("hold instance lock");
    let rejected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-inspect", "--instance-dir"])
        .arg(fixture.instance_dir())
        .output()
        .expect("attempt migration inspection while instance is owned");
    assert!(!rejected.status.success());
    drop(live);

    let discard_backup_path = fixture.0.join("discard-verified-backup");
    prdb_engine::offline_backup::create_verified_backup(
        fixture.instance_dir(),
        &discard_backup_path,
    )
    .expect("create source backup after closing competing writer");
    let active_before_discard =
        std::fs::read(&active_path).expect("read active source before discard");

    let discarded = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-discard-stage", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&discard_backup_path)
        .output()
        .expect("discard verified unpublished stage");
    assert!(
        discarded.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&discarded.stderr)
    );
    assert!(
        !staging_path.exists(),
        "verified unpublished staging image should be removed"
    );
    assert_eq!(
        std::fs::read(&active_path).expect("reread active image"),
        active_before_discard,
        "discard must preserve active source bytes"
    );
}

pub(super) fn migration_stage_discard_rejects_an_unrelated_verified_backup() {
    let active_fixture = Fixture::new();
    let other_fixture = Fixture::new();
    for (fixture, name) in [
        (&active_fixture, "discard_source_admin"),
        (&other_fixture, "discard_other_admin"),
    ] {
        let mut session =
            prdb_engine::bootstrap::initialize(fixture.instance_dir(), name, i64::MAX)
                .expect("initialize discard fixture");
        let mut secret = session.take_secret().expect("bootstrap secret");
        use zeroize::Zeroize;
        secret.zeroize();
        drop(session.acknowledge().expect("publish READY"));
    }
    let active_path = active_fixture.instance_dir().join("storage.redb");
    let staging_path = active_fixture.instance_dir().join(".prdb-migration.next");
    std::fs::copy(&active_path, &staging_path).expect("prepare staged image");
    std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o600))
        .expect("make staged image private");
    let backup_path = other_fixture.0.join("unrelated-backup");
    prdb_engine::offline_backup::create_verified_backup(other_fixture.instance_dir(), &backup_path)
        .expect("create unrelated source backup");

    let live = StorageInstance::open(active_fixture.instance_dir()).expect("hold instance lock");
    let locked = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-discard-stage", "--instance-dir"])
        .arg(active_fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .output()
        .expect("attempt discard while instance is owned");
    assert!(!locked.status.success());
    drop(live);

    let rejected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-discard-stage", "--instance-dir"])
        .arg(active_fixture.instance_dir())
        .arg("--backup-dir")
        .arg(&backup_path)
        .output()
        .expect("attempt discard with unrelated backup");
    assert!(!rejected.status.success());
    assert!(staging_path.exists(), "failed discard must retain staging");
    drop(
        StorageInstance::open(active_fixture.instance_dir()).expect("active source remains READY"),
    );
}

pub(super) fn migration_inspection_reports_database_identity_mismatch() {
    let active_fixture = Fixture::new();
    let staged_fixture = Fixture::new();
    for (fixture, name) in [
        (&active_fixture, "active_migration_inspect_admin"),
        (&staged_fixture, "staged_migration_inspect_admin"),
    ] {
        let mut session =
            prdb_engine::bootstrap::initialize(fixture.instance_dir(), name, i64::MAX)
                .expect("initialize migration inspection fixture");
        let mut secret = session.take_secret().expect("bootstrap secret");
        use zeroize::Zeroize;
        secret.zeroize();
        drop(session.acknowledge().expect("publish READY"));
    }
    let staging_path = active_fixture.instance_dir().join(".prdb-migration.next");
    std::fs::copy(
        staged_fixture.instance_dir().join("storage.redb"),
        &staging_path,
    )
    .expect("prepare unrelated staged image");
    std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o600))
        .expect("make staged image private");

    let inspected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-inspect", "--instance-dir"])
        .arg(active_fixture.instance_dir())
        .output()
        .expect("inspect unrelated generations");
    assert!(
        inspected.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let output = String::from_utf8(inspected.stdout).expect("UTF-8 migration inspection result");
    assert!(
        output.contains("staging=READY relationship=database_id_mismatch"),
        "{output}"
    );
}

pub(super) fn maintenance_backup_is_private_validated_and_checksum_verified() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "backup_admin", i64::MAX)
            .expect("initialize backup source");
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish source READY"));

    let backup_dir = fixture.0.join("backup");
    let create = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["backup", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output-dir")
        .arg(&backup_dir)
        .output()
        .expect("create verified offline backup");
    assert!(
        create.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert!(String::from_utf8_lossy(&create.stdout).contains("backup verified"));
    assert_eq!(
        std::fs::metadata(&backup_dir)
            .expect("backup directory")
            .permissions()
            .mode()
            & 0o7777,
        0o700
    );
    for name in [".prdb-bootstrap", "storage.redb"] {
        assert_eq!(
            std::fs::metadata(backup_dir.join(name))
                .expect("backup file")
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        assert_eq!(
            std::fs::read(fixture.instance_dir().join(name)).unwrap(),
            std::fs::read(backup_dir.join(name)).unwrap()
        );
    }

    let manifest_path = fixture.0.join("backup.prdb-backup");
    let original_manifest = std::fs::read(&manifest_path).expect("backup manifest");
    assert_eq!(
        std::fs::metadata(&manifest_path)
            .expect("manifest metadata")
            .permissions()
            .mode()
            & 0o7777,
        0o600
    );
    let verified = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["backup-verify", "--backup-dir"])
        .arg(&backup_dir)
        .output()
        .expect("verify offline backup");
    assert!(
        verified.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&verified.stderr)
    );
    let stable_database = std::fs::read(backup_dir.join("storage.redb")).unwrap();
    let verified_again = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["backup-verify", "--backup-dir"])
        .arg(&backup_dir)
        .output()
        .expect("repeat offline backup verification");
    assert!(
        verified_again.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&verified_again.stderr)
    );
    assert_eq!(
        std::fs::read(backup_dir.join("storage.redb")).unwrap(),
        stable_database,
        "read-only verification must preserve backup bytes"
    );

    let mut corrupted_manifest = original_manifest.clone();
    let last = corrupted_manifest.len() - 2;
    corrupted_manifest[last] = if corrupted_manifest[last] == b'0' {
        b'1'
    } else {
        b'0'
    };
    std::fs::write(&manifest_path, &corrupted_manifest).unwrap();
    let rejected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["backup-verify", "--backup-dir"])
        .arg(&backup_dir)
        .output()
        .expect("verify corrupted backup manifest");
    assert!(!rejected.status.success());
    std::fs::write(&manifest_path, original_manifest).unwrap();

    let repeated = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["backup", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output-dir")
        .arg(&backup_dir)
        .output()
        .expect("refuse to overwrite an existing backup");
    assert!(!repeated.status.success());

    let live = StorageInstance::open(fixture.instance_dir()).expect("lock source instance");
    let locked = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["backup", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output-dir")
        .arg(fixture.0.join("blocked-backup"))
        .output()
        .expect("reject backup while source is live");
    assert!(!locked.status.success());
    assert!(String::from_utf8_lossy(&locked.stderr).contains("InstanceLocked"));
    drop(live);
}

pub(super) fn unsupported_catalog_versions_are_rejected_before_derived_index_rebuild() {
    for unsupported_version in [1, 3] {
        let fixture = Fixture::new();
        let mut session =
            prdb_engine::bootstrap::initialize(fixture.instance_dir(), "version_admin", i64::MAX)
                .expect("initialize version fixture");
        let mut secret = session.take_secret().expect("bootstrap secret");
        use zeroize::Zeroize;
        secret.zeroize();
        drop(session.acknowledge().expect("publish READY"));

        let database_path = fixture.instance_dir().join("storage.redb");
        let database = Database::open(&database_path).expect("open version fixture");
        let mut metadata = {
            let read = database.begin_read().expect("read engine metadata");
            let table = read.open_table(ENGINE_META).expect("open metadata table");
            EngineMetadata::decode(
                table
                    .get(&b"format"[..])
                    .expect("read metadata")
                    .expect("metadata exists")
                    .value(),
            )
            .expect("decode metadata")
        };
        metadata.catalog_version = unsupported_version;
        let encoded = metadata.encode().expect("encode unsupported metadata");
        {
            let write = database.begin_write().expect("begin metadata update");
            write
                .open_table(ENGINE_META)
                .expect("open metadata table")
                .insert(&b"format"[..], &encoded[..])
                .expect("write unsupported version");
            write.delete_table(POLICIES).expect("remove derived index");
            write.commit().expect("commit unsupported metadata");
        }
        drop(database);

        let open_error = match StorageInstance::open(fixture.instance_dir()) {
            Ok(_) => panic!("catalog version {unsupported_version} must be rejected"),
            Err(error) => error,
        };
        assert!(
            matches!(
                &open_error,
                BootstrapError::Startup(StartupError::UnsupportedFormat)
            ),
            "unexpected startup error for catalog version {unsupported_version}: {open_error:?}"
        );

        let database = Database::open(&database_path).expect("reopen unsupported image");
        let read = database.begin_read().expect("read unsupported image");
        assert!(
            !read
                .list_tables()
                .expect("list tables")
                .any(|table| table.name() == POLICIES.name()),
            "catalog version {unsupported_version} must not have disposable indexes rebuilt"
        );
    }
}

pub(super) fn pinned_redb_rejects_file_format_v2_read_only_without_changing_the_fixture() {
    let fixture = Fixture::new();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/redb-2.6.4-v2.redb");
    let database_path = fixture.0.join("redb-v2.redb");
    std::fs::copy(source, &database_path).expect("copy archived redb v2 fixture");
    let before = std::fs::read(&database_path).expect("read original v2 fixture bytes");

    let open_error = match ReadOnlyDatabase::open(&database_path) {
        Ok(_) => panic!("redb 4.4 must reject the old physical file format"),
        Err(error) => error,
    };
    assert!(
        matches!(open_error, redb::DatabaseError::UpgradeRequired(2)),
        "unexpected physical-format compatibility result: {open_error:?}"
    );
    assert_eq!(
        std::fs::read(&database_path).expect("read v2 fixture after rejected open"),
        before,
        "read-only rejection must leave the unsupported file unchanged"
    );
}

pub(super) fn pinned_redb_opens_and_reads_fixture_after_redb_v2_to_v3_upgrade() {
    const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("compatibility_fixture_v1");

    let fixture = Fixture::new();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/redb-2.6.4-v3-upgraded.redb");
    let database_path = fixture.0.join("redb-v3-upgraded.redb");
    std::fs::copy(source, &database_path).expect("copy upgraded redb v3 fixture");

    {
        let database = Database::open(&database_path).expect("recover upgraded v3 fixture");
        let read = database.begin_read().expect("read upgraded v3 fixture");
        let table = read.open_table(TABLE).expect("open fixture table");
        assert_eq!(table.len().expect("count fixture rows"), 2);
        assert_eq!(
            table
                .get(&b"catalog-version"[..])
                .expect("read catalog version")
                .expect("catalog version exists")
                .value(),
            b"1"
        );
        assert_eq!(
            table
                .get(&b"payload"[..])
                .expect("read payload")
                .expect("payload exists")
                .value(),
            b"redb-file-format-v2-fixture"
        );
    }

    let database =
        ReadOnlyDatabase::open(&database_path).expect("open recovered upgraded fixture read-only");
    let read = database.begin_read().expect("read recovered fixture");
    let table = read.open_table(TABLE).expect("reopen fixture table");
    assert_eq!(table.len().expect("count recovered fixture rows"), 2);
}

pub(super) fn pinned_redb_rejects_populated_prdb_v2_without_mutating_the_file() {
    let fixture = Fixture::new();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/prdb-redb-2.6.4-v2.redb");
    let database_path = fixture.0.join("populated-prdb-v2.redb");
    std::fs::copy(source, &database_path).expect("copy populated PRDB v2 fixture");
    let before = std::fs::read(&database_path).expect("read populated PRDB v2 bytes");

    let error = match ReadOnlyDatabase::open(&database_path) {
        Ok(_) => panic!("redb 4.4 must reject the populated v2 physical format"),
        Err(error) => error,
    };
    assert!(
        matches!(error, redb::DatabaseError::UpgradeRequired(2)),
        "unexpected populated PRDB physical-format result: {error:?}"
    );
    assert_eq!(
        std::fs::read(&database_path).expect("read populated v2 bytes after rejection"),
        before,
        "rejected open must leave the populated source unchanged"
    );
}

pub(super) fn populated_prdb_logical_records_survive_redb_v2_to_v3_upgrade() {
    let fixture = Fixture::new();
    let instance_dir = fixture.instance_dir();
    std::fs::create_dir(&instance_dir).expect("create upgraded fixture instance");
    std::fs::set_permissions(&instance_dir, std::fs::Permissions::from_mode(0o700))
        .expect("set fixture directory permissions");
    let marker_path = instance_dir.join(".prdb-bootstrap");
    std::fs::write(&marker_path, b"PRDB-LOCAL-SETUP-1\n").expect("write managed instance marker");
    std::fs::set_permissions(&marker_path, std::fs::Permissions::from_mode(0o600))
        .expect("set marker permissions");
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/prdb-redb-2.6.4-v3-upgraded.redb");
    let database_path = instance_dir.join("storage.redb");
    std::fs::copy(source, &database_path).expect("copy upgraded populated PRDB fixture");
    std::fs::set_permissions(&database_path, std::fs::Permissions::from_mode(0o600))
        .expect("set fixture file permissions");
    drop(Database::open(&database_path).expect("recover upgraded populated redb image"));

    let instance = ReadOnlyStorageInstance::open(&instance_dir)
        .expect("validate populated PRDB after physical redb upgrade");
    let report = prdb_engine::startup::validate_persisted_records_read_only(instance.database())
        .expect("validate migrated logical PRDB records");
    assert_eq!(report.metadata.catalog_version, 2);
    assert!(report.catalog_records > 31);
    assert_eq!(report.rows, 1);

    let read = instance
        .database()
        .begin_read()
        .expect("read upgraded application rows");
    let catalog = read.open_table(CATALOG).expect("open upgraded catalog");
    let mut principals = 0;
    let mut grants = 0;
    let mut policies = 0;
    for entry in catalog.iter().expect("iterate upgraded catalog") {
        let (key, _) = entry.expect("read upgraded catalog record");
        let key = key.value();
        assert!(key.len() >= 2, "catalog key contains a family prefix");
        match u16::from_be_bytes([key[0], key[1]]) {
            10 => principals += 1,
            12 => grants += 1,
            13 => policies += 1,
            _ => {}
        }
    }
    assert_eq!(
        principals, 3,
        "fixture retains admin, admin role, and reader"
    );
    assert_eq!(grants, 2, "fixture retains schema USAGE and table SELECT");
    assert_eq!(policies, 1, "fixture retains the reader row policy");
    drop(catalog);
    let rows = read.open_table(ROWS).expect("open upgraded rows table");
    let (_, value) = rows
        .iter()
        .expect("iterate upgraded rows")
        .next()
        .expect("one application row")
        .expect("decode application row");
    let row = prdb_engine::row_record::RowRecord::decode(value.value())
        .expect("decode upgraded application row");
    assert!(
        row.payload
            .windows(b"persisted redb v2 row".len())
            .any(|window| window == b"persisted redb v2 row"),
        "upgraded row retains its application payload"
    );
}

pub(super) fn maintenance_redb_v2_upgrade_backs_up_and_atomically_publishes_ready_instance() {
    let fixture = Fixture::new();
    let instance_dir = fixture.instance_dir();
    std::fs::create_dir(&instance_dir).expect("create source instance directory");
    std::fs::set_permissions(&instance_dir, std::fs::Permissions::from_mode(0o700))
        .expect("set source directory permissions");
    let marker_path = instance_dir.join(".prdb-bootstrap");
    std::fs::write(&marker_path, b"PRDB-LOCAL-SETUP-1\n").expect("write source marker");
    std::fs::set_permissions(&marker_path, std::fs::Permissions::from_mode(0o600))
        .expect("set source marker permissions");
    let source_fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/prdb-redb-2.6.4-v2.redb");
    let active_path = instance_dir.join("storage.redb");
    std::fs::copy(source_fixture, &active_path).expect("copy populated physical v2 source");
    std::fs::set_permissions(&active_path, std::fs::Permissions::from_mode(0o600))
        .expect("set active database permissions");
    let source_bytes = std::fs::read(&active_path).expect("read source database bytes");
    let backup_path = fixture.0.join("redb-v2-independent-backup.redb");
    let missing_ack = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .output()
        .expect("check upgrade acknowledgment");
    assert!(!missing_ack.status.success());
    assert_eq!(std::fs::read(&active_path).unwrap(), source_bytes);
    assert!(!backup_path.exists());

    let internal_backup = instance_dir.join("unsafe-backup.redb");
    let internal_backup = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&internal_backup)
        .arg("--confirm-redb-v2")
        .output()
        .expect("check backup path restriction");
    assert!(!internal_backup.status.success());
    assert_eq!(std::fs::read(&active_path).unwrap(), source_bytes);
    assert!(!instance_dir.join("unsafe-backup.redb").exists());

    let staging_path = instance_dir.join(".prdb-redb-upgrade.next");
    std::fs::write(&staging_path, b"interrupted physical-format staging")
        .expect("seed interrupted staging image");
    std::fs::set_permissions(&staging_path, std::fs::Permissions::from_mode(0o600))
        .expect("set interrupted staging permissions");

    let output = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .output()
        .expect("run redb physical-format upgrade");
    assert!(
        output.status.success(),
        "redb physical upgrade failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(backup_path.is_file(), "independent byte backup exists");
    assert_eq!(
        std::fs::read(&backup_path).expect("read independent source backup"),
        source_bytes,
        "backup preserves the exact physical-v2 source"
    );
    assert_eq!(
        std::fs::metadata(&backup_path)
            .expect("stat source backup")
            .mode()
            & 0o777,
        0o600,
        "source backup is private"
    );
    assert_eq!(
        std::fs::read(&staging_path).expect("read retained v2 source generation"),
        source_bytes,
        "atomic publication retains the original physical-v2 generation"
    );
    let instance = ReadOnlyStorageInstance::open(&instance_dir)
        .expect("open published physical-v3 READY instance");
    let report = prdb_engine::startup::validate_persisted_records_read_only(instance.database())
        .expect("validate published PRDB image");
    assert_eq!(report.rows, 1);
    assert!(report.catalog_records > 31);
    drop(instance);

    let active_bytes = std::fs::read(&active_path).expect("read published database");
    let retry = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .output()
        .expect("retry physical-format upgrade");
    assert!(
        retry.status.success(),
        "idempotent retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(String::from_utf8_lossy(&retry.stdout).contains("physical_format=already-current"));
    assert_eq!(
        std::fs::read(&active_path).expect("read database after idempotent retry"),
        active_bytes,
        "retry leaves the published database unchanged"
    );
}

pub(super) fn maintenance_redb_physical_upgrade_refuses_a_live_instance_lock() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "upgrade_lock_admin", i64::MAX)
            .expect("initialize locked instance");
    if let Some(mut secret) = session.take_secret() {
        use zeroize::Zeroize;
        secret.zeroize();
    }
    drop(session.acknowledge().expect("publish locked instance"));
    let instance = StorageInstance::open(fixture.instance_dir()).expect("hold instance lock");
    let backup_path = fixture.0.join("must-not-exist.redb");

    let output = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(fixture.instance_dir())
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .output()
        .expect("run physical upgrade while service lock is held");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("InstanceLocked"));
    assert!(!backup_path.exists());
    drop(instance);
}

pub(super) fn maintenance_redb_physical_upgrade_recovers_after_process_exit_at_exchange() {
    let fixture = Fixture::new();
    let instance_dir = fixture.instance_dir();
    std::fs::create_dir(&instance_dir).expect("create source instance directory");
    std::fs::set_permissions(&instance_dir, std::fs::Permissions::from_mode(0o700))
        .expect("set source directory permissions");
    let marker_path = instance_dir.join(".prdb-bootstrap");
    std::fs::write(&marker_path, b"PRDB-LOCAL-SETUP-1\n").expect("write source marker");
    std::fs::set_permissions(&marker_path, std::fs::Permissions::from_mode(0o600))
        .expect("set source marker permissions");
    let source_fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/prdb-redb-2.6.4-v2.redb");
    let active_path = instance_dir.join("storage.redb");
    std::fs::copy(source_fixture, &active_path).expect("copy populated physical v2 source");
    std::fs::set_permissions(&active_path, std::fs::Permissions::from_mode(0o600))
        .expect("set active database permissions");
    let source_bytes = std::fs::read(&active_path).expect("read source database bytes");
    let backup_path = fixture.0.join("redb-v2-independent-backup.redb");
    let crash = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .env("PRDB_TEST_REDB_UPGRADE_EXIT_AFTER_EXCHANGE", "1")
        .output()
        .expect("run crash-injected physical upgrade");
    assert_eq!(crash.status.code(), Some(86));
    assert_eq!(
        std::fs::read(instance_dir.join(".prdb-redb-upgrade.next"))
            .expect("read retained v2 source"),
        source_bytes
    );
    let instance = ReadOnlyStorageInstance::open(&instance_dir)
        .expect("open READY generation after process exit");
    drop(instance);

    let retry = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .output()
        .expect("retry physical upgrade after process exit");
    assert!(
        retry.status.success(),
        "upgrade retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(String::from_utf8_lossy(&retry.stdout).contains("physical_format=already-current"));
}

pub(super) fn maintenance_redb_physical_upgrade_recovers_after_directory_sync_failure() {
    let fixture = Fixture::new();
    let instance_dir = fixture.instance_dir();
    std::fs::create_dir(&instance_dir).expect("create source instance directory");
    std::fs::set_permissions(&instance_dir, std::fs::Permissions::from_mode(0o700))
        .expect("set source directory permissions");
    let marker_path = instance_dir.join(".prdb-bootstrap");
    std::fs::write(&marker_path, b"PRDB-LOCAL-SETUP-1\n").expect("write source marker");
    std::fs::set_permissions(&marker_path, std::fs::Permissions::from_mode(0o600))
        .expect("set source marker permissions");
    let source_fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/prdb-redb-2.6.4-v2.redb");
    let active_path = instance_dir.join("storage.redb");
    std::fs::copy(source_fixture, &active_path).expect("copy populated physical v2 source");
    std::fs::set_permissions(&active_path, std::fs::Permissions::from_mode(0o600))
        .expect("set active database permissions");
    let source_bytes = std::fs::read(&active_path).expect("read source database bytes");
    let backup_path = fixture.0.join("redb-v2-independent-backup.redb");
    let failed = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .env("PRDB_TEST_REDB_UPGRADE_FAIL_SYNC_AFTER_EXCHANGE", "1")
        .output()
        .expect("run sync-failure-injected physical upgrade");
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("injected directory sync failure"));
    assert_eq!(
        std::fs::read(instance_dir.join(".prdb-redb-upgrade.next"))
            .expect("read retained source generation after sync failure"),
        source_bytes
    );
    drop(
        ReadOnlyStorageInstance::open(&instance_dir)
            .expect("open validated READY generation after sync failure"),
    );

    let retry = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .output()
        .expect("retry physical upgrade after directory sync failure");
    assert!(
        retry.status.success(),
        "upgrade retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(String::from_utf8_lossy(&retry.stdout).contains("physical_format=already-current"));
}

pub(super) fn maintenance_redb_physical_upgrade_recovers_after_backup_directory_sync_failure() {
    let fixture = Fixture::new();
    let instance_dir = fixture.instance_dir();
    std::fs::create_dir(&instance_dir).expect("create source instance directory");
    std::fs::set_permissions(&instance_dir, std::fs::Permissions::from_mode(0o700))
        .expect("set source directory permissions");
    let marker_path = instance_dir.join(".prdb-bootstrap");
    std::fs::write(&marker_path, b"PRDB-LOCAL-SETUP-1\n").expect("write source marker");
    std::fs::set_permissions(&marker_path, std::fs::Permissions::from_mode(0o600))
        .expect("set source marker permissions");
    let source_fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/prdb-redb-2.6.4-v2.redb");
    let active_path = instance_dir.join("storage.redb");
    std::fs::copy(source_fixture, &active_path).expect("copy populated physical v2 source");
    std::fs::set_permissions(&active_path, std::fs::Permissions::from_mode(0o600))
        .expect("set active database permissions");
    let source_bytes = std::fs::read(&active_path).expect("read source database bytes");
    let backup_path = fixture.0.join("redb-v2-independent-backup.redb");
    let failed = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .env("PRDB_TEST_REDB_UPGRADE_FAIL_SYNC_AFTER_BACKUP_PUBLISH", "1")
        .output()
        .expect("run backup-directory-sync-failure-injected physical upgrade");
    assert!(!failed.status.success());
    assert!(
        String::from_utf8_lossy(&failed.stderr)
            .contains("injected directory sync failure after backup publication")
    );
    assert_eq!(
        std::fs::read(&active_path).expect("read active v2 source after backup sync failure"),
        source_bytes,
        "failure before staging must leave the active source byte-identical"
    );
    assert_eq!(
        std::fs::read(&backup_path).expect("read published independent backup"),
        source_bytes,
        "the published backup must be reusable after retry"
    );
    assert!(
        !instance_dir.join(".prdb-redb-upgrade.next").exists(),
        "staging must not begin until the independent backup directory is synced"
    );

    let retry = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .output()
        .expect("retry physical upgrade after backup directory sync failure");
    assert!(
        retry.status.success(),
        "upgrade retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(String::from_utf8_lossy(&retry.stdout).contains("physical_format=upgraded"));
    assert_eq!(
        std::fs::read(instance_dir.join(".prdb-redb-upgrade.next"))
            .expect("read retained v2 source after successful retry"),
        source_bytes
    );
    drop(
        ReadOnlyStorageInstance::open(&instance_dir)
            .expect("open READY generation after successful retry"),
    );
}

pub(super) fn maintenance_redb_physical_upgrade_rebuilds_stage_after_conversion_failure() {
    let fixture = Fixture::new();
    let instance_dir = fixture.instance_dir();
    std::fs::create_dir(&instance_dir).expect("create source instance directory");
    std::fs::set_permissions(&instance_dir, std::fs::Permissions::from_mode(0o700))
        .expect("set source directory permissions");
    let marker_path = instance_dir.join(".prdb-bootstrap");
    std::fs::write(&marker_path, b"PRDB-LOCAL-SETUP-1\n").expect("write source marker");
    std::fs::set_permissions(&marker_path, std::fs::Permissions::from_mode(0o600))
        .expect("set source marker permissions");
    let source_fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/prdb-redb-2.6.4-v2.redb");
    let active_path = instance_dir.join("storage.redb");
    std::fs::copy(source_fixture, &active_path).expect("copy populated physical v2 source");
    std::fs::set_permissions(&active_path, std::fs::Permissions::from_mode(0o600))
        .expect("set active database permissions");
    let source_bytes = std::fs::read(&active_path).expect("read source database bytes");
    let backup_path = fixture.0.join("redb-v2-independent-backup.redb");

    let failed = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .env("PRDB_TEST_REDB_UPGRADE_FAIL_AFTER_STAGE_CONVERSION", "1")
        .output()
        .expect("run conversion-failure-injected physical upgrade");
    assert!(!failed.status.success());
    assert!(
        String::from_utf8_lossy(&failed.stderr)
            .contains("injected failure after physical stage conversion")
    );
    assert_eq!(
        std::fs::read(&active_path).expect("read active v2 source after failure"),
        source_bytes
    );
    assert_eq!(
        std::fs::read(&backup_path).expect("read independent backup after failure"),
        source_bytes
    );
    assert!(
        instance_dir.join(".prdb-redb-upgrade.next").exists(),
        "converted stage remains available for the retry cleanup path"
    );

    let retry = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .output()
        .expect("retry physical upgrade after stage conversion failure");
    assert!(
        retry.status.success(),
        "upgrade retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(String::from_utf8_lossy(&retry.stdout).contains("physical_format=upgraded"));
    assert_eq!(
        std::fs::read(instance_dir.join(".prdb-redb-upgrade.next"))
            .expect("read retained source generation after retry"),
        source_bytes
    );
    drop(
        ReadOnlyStorageInstance::open(&instance_dir)
            .expect("open READY generation after recovered stage conversion"),
    );
}

pub(super) fn maintenance_redb_physical_upgrade_rebuilds_valid_stage_after_process_exit() {
    let fixture = Fixture::new();
    let instance_dir = fixture.instance_dir();
    std::fs::create_dir(&instance_dir).expect("create source instance directory");
    std::fs::set_permissions(&instance_dir, std::fs::Permissions::from_mode(0o700))
        .expect("set source directory permissions");
    let marker_path = instance_dir.join(".prdb-bootstrap");
    std::fs::write(&marker_path, b"PRDB-LOCAL-SETUP-1\n").expect("write source marker");
    std::fs::set_permissions(&marker_path, std::fs::Permissions::from_mode(0o600))
        .expect("set source marker permissions");
    let source_fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/redb-file-formats/prdb-redb-2.6.4-v2.redb");
    let active_path = instance_dir.join("storage.redb");
    std::fs::copy(source_fixture, &active_path).expect("copy populated physical v2 source");
    std::fs::set_permissions(&active_path, std::fs::Permissions::from_mode(0o600))
        .expect("set active database permissions");
    let source_bytes = std::fs::read(&active_path).expect("read source database bytes");
    let backup_path = fixture.0.join("redb-v2-independent-backup.redb");
    let crash = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .env("PRDB_TEST_REDB_UPGRADE_EXIT_BEFORE_EXCHANGE", "1")
        .output()
        .expect("run crash-injected physical upgrade");
    assert_eq!(crash.status.code(), Some(85));
    assert_eq!(
        std::fs::read(&active_path).expect("read active v2 source after exit"),
        source_bytes
    );
    assert_eq!(
        std::fs::read(&backup_path).expect("read independent v2 backup after exit"),
        source_bytes
    );
    let staged = ReadOnlyDatabase::open(instance_dir.join(".prdb-redb-upgrade.next"))
        .expect("open complete staged v3 image after process exit");
    prdb_engine::startup::validate_persisted_records_read_only(&staged)
        .expect("validate staged READY records");
    drop(staged);

    let retry = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .arg("redb-format-upgrade")
        .arg("--instance-dir")
        .arg(&instance_dir)
        .arg("--backup-file")
        .arg(&backup_path)
        .arg("--confirm-redb-v2")
        .output()
        .expect("retry physical upgrade after pre-publication process exit");
    assert!(
        retry.status.success(),
        "upgrade retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(String::from_utf8_lossy(&retry.stdout).contains("physical_format=upgraded"));
    assert_eq!(
        std::fs::read(instance_dir.join(".prdb-redb-upgrade.next"))
            .expect("read retained v2 source after retry"),
        source_bytes
    );
}

pub(super) fn v1_source_validator_checks_legacy_ready_images_without_admitting_them() {
    let fixture = Fixture::new();
    let mut session =
        prdb_engine::bootstrap::initialize(fixture.instance_dir(), "v1_source_admin", i64::MAX)
            .expect("initialize source fixture");
    let admin_id = session.admin_user_id();
    let mut secret = session.take_secret().expect("bootstrap secret");
    use zeroize::Zeroize;
    secret.zeroize();
    drop(session.acknowledge().expect("publish source READY"));

    let database_path = fixture.instance_dir().join("storage.redb");
    let database = Database::open(&database_path).expect("open fixture for populated records");
    let storage = prdb_engine::storage_manager::StorageManager::new(
        &database,
        std::time::Duration::from_millis(100),
    )
    .expect("create fixture storage manager");
    let make_ddl_audit = |event_id, correlation_id| AuditEvent {
        event_id,
        actor_id: Some(admin_id),
        correlation_id,
        occurred_at_us: 1,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: Vec::new(),
    };
    let schema = storage
        .create_schema(
            [0x71; 16],
            admin_id,
            "legacy_fixture",
            admin_id,
            i64::MAX,
            make_ddl_audit([0x72; 16], [0x71; 16]),
        )
        .expect("create populated legacy schema");
    let table = storage
        .create_table(prdb_engine::storage_manager::CreateTableRequest {
            invocation_id: [0x73; 16],
            authenticated_actor_id: admin_id,
            schema_id: schema.schema_id,
            name: "records".to_owned(),
            columns: vec![prdb_engine::storage_manager::TableColumnDefinition {
                name: "payload".to_owned(),
                scalar_type: prdb_engine::tuple::ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: Some(prdb_engine::storage_manager::TableColumnDefault::Literal(
                    Some(prdb_engine::tuple::ScalarValue::Text(
                        "legacy value".to_owned(),
                    )),
                )),
            }],
            retention_deadline_us: i64::MAX,
            audit_event: make_ddl_audit([0x74; 16], [0x73; 16]),
        })
        .expect("create populated legacy table and default expression");
    let mut row_key = Vec::with_capacity(32);
    row_key.extend_from_slice(&table.table_id);
    row_key.extend_from_slice(&[0x75; 16]);
    let row_payload = prdb_engine::tuple::encode_tuple(
        &[prdb_engine::tuple::ColumnSchema {
            scalar_type: prdb_engine::tuple::ScalarType::Text,
            nullable: false,
        }],
        1,
        &[Some(prdb_engine::tuple::ScalarValue::Text(
            "persisted legacy row".to_owned(),
        ))],
    )
    .expect("encode populated legacy row");
    let row_record = prdb_engine::row_record::RowRecord {
        row_version: 1,
        payload: row_payload,
    }
    .encode()
    .expect("encode legacy row record");
    {
        let write = database.begin_write().expect("begin populated row write");
        write
            .open_table(ROWS)
            .expect("open rows table")
            .insert(row_key.as_slice(), row_record.as_slice())
            .expect("write populated legacy row");
        write.commit().expect("commit populated legacy row");
    }
    drop(storage);
    drop(database);

    let current = ReadOnlyDatabase::open(&database_path).expect("open populated v2 image");
    let current_report = prdb_engine::startup::validate_persisted_records_read_only(&current)
        .expect("validate populated current catalog");
    assert_eq!(current_report.metadata.catalog_version, 2);
    assert!(current_report.catalog_records > 31);
    assert_eq!(current_report.rows, 1);
    drop(current);

    let database = Database::open(&database_path).expect("open fixture for legacy conversion");
    let mut metadata = {
        let read = database.begin_read().expect("read fixture metadata");
        let table = read.open_table(ENGINE_META).expect("open metadata table");
        let encoded = table.get(&b"format"[..]).expect("read metadata").unwrap();
        EngineMetadata::decode(encoded.value()).expect("decode current metadata")
    };
    metadata.catalog_version = 1;
    let encoded_metadata = metadata.encode().expect("encode v1 metadata");
    let mut database_key = 1u16.to_be_bytes().to_vec();
    database_key.push(1);
    database_key.extend_from_slice(&metadata.database_id);
    let mut fields = {
        let read = database.begin_read().expect("read fixture catalog");
        let table = read.open_table(CATALOG).expect("open catalog table");
        let encoded = table
            .get(database_key.as_slice())
            .expect("read database record")
            .unwrap();
        decode_registered_envelope(encoded.value())
            .expect("decode database record")
            .fields
    };
    for field in &mut fields {
        if field.tag == 2 {
            field.payload = 1u32.to_le_bytes().to_vec();
        }
        if field.tag == 3 {
            assert_eq!(
                u32::from_le_bytes(field.payload[..4].try_into().unwrap()),
                31
            );
            field.payload[..4].copy_from_slice(&30u32.to_le_bytes());
            field.payload.truncate(4 + 30 * 20);
        }
    }
    let database_record = encode_envelope(
        &family_schema(1).expect("database family schema"),
        &database_key,
        &fields,
    )
    .expect("encode v1 database record");
    {
        let write = database.begin_write().expect("begin fixture downgrade");
        write
            .open_table(CATALOG)
            .expect("open catalog for downgrade")
            .insert(database_key.as_slice(), database_record.as_slice())
            .expect("write v1 database record");
        write
            .open_table(ENGINE_META)
            .expect("open metadata for downgrade")
            .insert(&b"format"[..], &encoded_metadata[..])
            .expect("write v1 metadata");
        write.commit().expect("commit v1 source fixture");
    }
    drop(database);

    let source = ReadOnlyDatabase::open(&database_path).expect("open legacy source read-only");
    let report = prdb_engine::startup::validate_v1_migration_source_read_only(&source)
        .expect("validate populated v1 source for offline conversion");
    assert_eq!(report.metadata.catalog_version, 1);
    assert_eq!(report.metadata.database_id, metadata.database_id);
    assert!(report.catalog_records > 30);
    assert_eq!(report.rows, 1);
    assert!(matches!(
        prdb_engine::startup::validate_persisted_records_read_only(&source),
        Err(StartupError::UnsupportedFormat)
    ));
    drop(source);

    let staging_path = fixture.0.join("v1-staging.redb");
    std::fs::copy(&database_path, &staging_path).expect("copy source to private staging image");
    let staging = Database::open(&staging_path).expect("open staging image");
    let history = prdb_engine::identity_history::IdentityHistory {
        database_id: report.metadata.database_id,
        commit_sequence: report.metadata.commit_sequence,
        tombstone_ids: vec![[0xfc; 16], [0xfd; 16], [0xfe; 16]],
    };
    prdb_engine::migration::upgrade_v1_staging_database(&staging, &history)
        .expect("upgrade copied v1 staging image");
    drop(staging);
    let upgraded = ReadOnlyDatabase::open(&staging_path).expect("open upgraded staging image");
    let upgraded_report = prdb_engine::startup::validate_persisted_records_read_only(&upgraded)
        .expect("fully validate upgraded staging image");
    assert_eq!(upgraded_report.metadata.catalog_version, 2);
    assert_eq!(
        upgraded_report.metadata.database_id,
        report.metadata.database_id
    );
    assert_eq!(
        upgraded_report.metadata.commit_sequence,
        report.metadata.commit_sequence
    );
    assert_eq!(upgraded_report.rows, report.rows);
    assert_eq!(
        upgraded_report.catalog_records,
        report.catalog_records + history.tombstone_ids.len()
    );
    let read = upgraded.begin_read().expect("read upgraded catalog");
    let catalog = read.open_table(CATALOG).expect("open upgraded catalog");
    for tombstone_id in &history.tombstone_ids {
        let tombstone_key = [17u16.to_be_bytes().as_slice(), &[1], tombstone_id].concat();
        assert!(
            catalog
                .get(tombstone_key.as_slice())
                .expect("read tombstone")
                .is_some(),
            "migrated tombstone {tombstone_id:02x?} is missing"
        );
    }
    let database_record_key = [
        1u16.to_be_bytes().as_slice(),
        &[1],
        &report.metadata.database_id,
    ]
    .concat();
    let database_record = catalog
        .get(database_record_key.as_slice())
        .expect("read upgraded database descriptor")
        .expect("database descriptor exists");
    let descriptor =
        decode_registered_envelope(database_record.value()).expect("decode upgraded descriptor");
    let builtins = descriptor
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .expect("built-in mapping field");
    assert_eq!(
        u32::from_le_bytes(builtins.payload[..4].try_into().unwrap()),
        31
    );
    assert_eq!(
        u16::from_le_bytes(
            builtins.payload[builtins.payload.len() - 20..][..2]
                .try_into()
                .unwrap()
        ),
        217
    );
    drop(database_record);
    drop(catalog);
    drop(read);
    drop(upgraded);

    let colliding_staging_path = fixture.0.join("v1-staging-collision.redb");
    std::fs::copy(&database_path, &colliding_staging_path)
        .expect("copy source to collision staging image");
    let colliding_staging =
        Database::open(&colliding_staging_path).expect("open collision staging image");
    let colliding_history = prdb_engine::identity_history::IdentityHistory {
        database_id: report.metadata.database_id,
        commit_sequence: report.metadata.commit_sequence,
        tombstone_ids: vec![{
            let mut id = [0x80; 16];
            id[15] = 3;
            id
        }],
    };
    assert!(matches!(
        prdb_engine::migration::upgrade_v1_staging_database(&colliding_staging, &colliding_history),
        Err(prdb_engine::migration::MigrationError::InvalidHistory(
            prdb_engine::identity_history::IdentityHistoryError::TombstoneIdAlreadyUsed
        ))
    ));
    let unmodified = prdb_engine::startup::validate_v1_migration_database(&colliding_staging)
        .expect("failed conversion leaves staging image at v1");
    assert_eq!(unmodified.metadata.catalog_version, 1);
    drop(colliding_staging);

    let inspected = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-inspect", "--instance-dir"])
        .arg(fixture.instance_dir())
        .output()
        .expect("inspect legacy migration source");
    assert!(
        inspected.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let output = String::from_utf8(inspected.stdout).expect("UTF-8 migration inspection result");
    assert!(output.contains("active=READY"), "{output}");
    assert!(output.contains("catalog_version=1"), "{output}");
    assert!(output.contains("staging=absent"), "{output}");
    let source_before_backup = std::fs::read(&database_path).expect("read legacy source bytes");
    let backup_path = fixture.0.join("v1-migration-backup");
    let backed_up = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-backup", "--instance-dir"])
        .arg(fixture.instance_dir())
        .arg("--output-dir")
        .arg(&backup_path)
        .output()
        .expect("create legacy migration backup");
    assert!(
        backed_up.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&backed_up.stderr)
    );
    let verified = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["backup-verify", "--backup-dir"])
        .arg(&backup_path)
        .output()
        .expect("verify legacy migration backup");
    assert!(
        verified.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&verified.stderr)
    );
    assert!(String::from_utf8_lossy(&verified.stdout).contains("verified database_bytes="));
    assert_eq!(
        std::fs::read(&database_path).expect("reread legacy source bytes"),
        source_before_backup,
        "migration backup must not modify the legacy source"
    );

    let populated_instance = fixture.0.join("populated-v1-instance");
    std::fs::create_dir(&populated_instance).expect("create populated migration instance");
    std::fs::set_permissions(&populated_instance, std::fs::Permissions::from_mode(0o700))
        .expect("restrict populated migration instance");
    for file_name in [".prdb-bootstrap", ".prdb.lock", "storage.redb"] {
        let source_file = fixture.instance_dir().join(file_name);
        let copied_file = populated_instance.join(file_name);
        std::fs::copy(source_file, &copied_file).expect("copy populated migration file");
        std::fs::set_permissions(&copied_file, std::fs::Permissions::from_mode(0o600))
            .expect("restrict copied migration file");
    }
    let populated_backup = fixture.0.join("populated-v1-backup");
    prdb_engine::offline_backup::create_verified_migration_backup(
        &populated_instance,
        &populated_backup,
    )
    .expect("back up populated v1 migration instance");
    let populated_history_path = fixture.0.join("populated-identity-history.txt");
    let populated_history = prdb_engine::identity_history::IdentityHistory {
        database_id: report.metadata.database_id,
        commit_sequence: report.metadata.commit_sequence,
        tombstone_ids: vec![[0xfd; 16]],
    };
    std::fs::write(
        &populated_history_path,
        populated_history
            .encode()
            .expect("encode populated history"),
    )
    .expect("write populated identity history");
    std::fs::set_permissions(
        &populated_history_path,
        std::fs::Permissions::from_mode(0o600),
    )
    .expect("restrict populated identity history");
    let populated_apply = Command::new(env!("CARGO_BIN_EXE_prdb-maintain"))
        .args(["migration-apply", "--instance-dir"])
        .arg(&populated_instance)
        .arg("--backup-dir")
        .arg(&populated_backup)
        .arg("--identity-history")
        .arg(&populated_history_path)
        .arg("--confirm-complete-identity-history")
        .output()
        .expect("migrate populated v1 instance");
    assert!(
        populated_apply.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&populated_apply.stdout),
        String::from_utf8_lossy(&populated_apply.stderr)
    );
    let populated_migrated =
        StorageInstance::open(&populated_instance).expect("open populated migrated instance");
    let populated_report =
        prdb_engine::startup::validate_persisted_records(populated_migrated.database())
            .expect("validate populated v2 migration result");
    assert_eq!(populated_report.metadata.catalog_version, 2);
    assert_eq!(populated_report.rows, report.rows);
    assert_eq!(populated_report.audit_events, report.audit_events);
    assert_eq!(
        populated_report.catalog_records,
        report.catalog_records + populated_history.tombstone_ids.len()
    );
    drop(populated_migrated);

    let orphan_user_id = [0x61; 16];
    let token_id = [0x62; 16];
    let mut token_key = 16u16.to_be_bytes().to_vec();
    token_key.push(1);
    token_key.extend_from_slice(&token_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: orphan_user_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bytes,
            payload: vec![0; 32],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::I64,
            payload: 1i64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::I64,
            payload: 2i64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Bool,
            payload: vec![1],
        },
        CatalogField {
            tag: 8,
            wire_type: WireType::I64,
            payload: 2i64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::Text,
            payload: b"retired".to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U64,
            payload: 2u64.to_le_bytes().to_vec(),
        },
    ];
    let orphan_verifier = encode_envelope(
        &family_schema(16).expect("verifier family schema"),
        &token_key,
        &fields,
    )
    .expect("encode revoked orphan verifier");
    let database = Database::open(&database_path).expect("open v1 source to corrupt it");
    {
        let write = database.begin_write().expect("begin orphan verifier write");
        write
            .open_table(CATALOG)
            .expect("open v1 catalog")
            .insert(token_key.as_slice(), orphan_verifier.as_slice())
            .expect("insert orphan verifier");
        write.commit().expect("commit orphan verifier");
    }
    drop(database);
    let invalid_source = ReadOnlyDatabase::open(&database_path).expect("open modified source");
    assert!(matches!(
        prdb_engine::startup::validate_v1_migration_source_read_only(&invalid_source),
        Err(StartupError::InvalidCatalog)
    ));
    drop(invalid_source);
    assert!(matches!(
        StorageInstance::open(fixture.instance_dir()),
        Err(BootstrapError::Startup(StartupError::UnsupportedFormat))
    ));
}
