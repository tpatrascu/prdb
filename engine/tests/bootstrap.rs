use prdb_engine::bootstrap::{
    BootstrapError, StorageInstance, initialize, receive_bootstrap_handoff, resume,
};
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::credentials::hash_token_secret;
use prdb_engine::ipc::{IpcFrame, read_frame, write_frame};
use prdb_engine::schema::CATALOG;
use prdb_engine::startup::{StartupError, validate_persisted_records};
use prdb_engine::storage_manager::{StorageManager, StorageManagerConfig, StorageManagerInitError};
use redb::{Database, ReadableDatabase};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zeroize::Zeroize;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("prdb-bootstrap-{}-{id}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        Self(root.join("data"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let root = self.0.parent().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn expiry() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    now + 86_400_000_000
}

fn verifier_revoked(database: &Database, token_id: [u8; 16]) -> bool {
    let key = [16u16.to_be_bytes().as_slice(), &[1], &token_id].concat();
    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let value = catalog.get(key.as_slice()).unwrap().unwrap();
    let record = decode_registered_envelope(value.value()).unwrap();
    record
        .fields
        .iter()
        .find(|field| field.tag == 7)
        .is_some_and(|field| field.payload == [1])
}

fn verifier_hash(database: &Database, token_id: [u8; 16]) -> Vec<u8> {
    let key = [16u16.to_be_bytes().as_slice(), &[1], &token_id].concat();
    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let value = catalog.get(key.as_slice()).unwrap().unwrap();
    decode_registered_envelope(value.value())
        .unwrap()
        .fields
        .into_iter()
        .find(|field| field.tag == 4)
        .unwrap()
        .payload
}

#[test]
fn bootstrap_handoff_stays_initializing_and_resume_rotates_secret() {
    let fixture = Fixture::new();
    let mut first = initialize(&fixture.0, "first_admin", expiry()).unwrap();
    assert_eq!(
        std::fs::metadata(&fixture.0).unwrap().permissions().mode() & 0o7777,
        0o700
    );
    for filename in [".prdb.lock", ".prdb-bootstrap", "storage.redb"] {
        assert_eq!(
            std::fs::metadata(fixture.0.join(filename))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
    }
    let admin_id = first.admin_user_id();
    let first_token = first.token_id();
    let first_secret = first.take_secret().unwrap();
    assert_eq!(first_secret.len(), 32);
    assert_eq!(
        verifier_hash(first.database(), first_token),
        hash_token_secret(&first_secret)
    );
    assert!(first.take_secret().is_none());
    assert_eq!(
        validate_persisted_records(first.database()),
        Err(StartupError::NotReady)
    );
    assert!(matches!(
        StorageManager::new(first.database(), Duration::from_secs(1)),
        Err(StorageManagerInitError::Startup(StartupError::NotReady))
    ));
    assert!(initialize(&fixture.0, "first_admin", expiry()).is_err());
    assert!(matches!(
        StorageInstance::open(&fixture.0),
        Err(BootstrapError::InstanceLocked)
    ));
    assert!(resume(&fixture.0, "first_admin", expiry()).is_err());
    let mut first_secret = first_secret;
    first_secret.zeroize();
    drop(first);

    let mut resumed = resume(&fixture.0, "first_admin", expiry()).unwrap();
    assert_eq!(resumed.admin_user_id(), admin_id);
    assert_ne!(resumed.token_id(), first_token);
    assert!(verifier_revoked(resumed.database(), first_token));
    let mut replacement_secret = resumed.take_secret().unwrap();
    replacement_secret.zeroize();
    let ready = resumed.acknowledge().unwrap();
    assert!(validate_persisted_records(ready.database()).is_ok());
    assert!(matches!(
        StorageInstance::open(&fixture.0),
        Err(BootstrapError::InstanceLocked)
    ));
    drop(ready);

    let reopened = StorageInstance::open(&fixture.0).unwrap();
    assert!(validate_persisted_records(reopened.database()).is_ok());
    let manager = reopened
        .storage_manager(StorageManagerConfig::default())
        .unwrap();
    assert!(!manager.requires_reopen_after_commit_error());
    drop(manager);
    let reopened = reopened.reopen().unwrap();
    assert!(validate_persisted_records(reopened.database()).is_ok());
    assert!(matches!(
        StorageInstance::open(&fixture.0),
        Err(BootstrapError::InstanceLocked)
    ));
    let manager = reopened
        .storage_manager(StorageManagerConfig::default())
        .unwrap();
    assert!(!manager.requires_reopen_after_commit_error());
    drop(manager);
    drop(reopened);
    assert!(matches!(
        resume(&fixture.0, "first_admin", expiry()),
        Err(BootstrapError::NotInitializing)
    ));
}

#[test]
fn storage_open_rejects_a_truncated_redb_image_and_releases_the_instance_lock() {
    let fixture = Fixture::new();
    let mut session = initialize(&fixture.0, "corruption_admin", expiry()).unwrap();
    let mut secret = session.take_secret().unwrap();
    secret.zeroize();
    drop(session.acknowledge().unwrap());

    let database_path = fixture.0.join("storage.redb");
    let valid_image = std::fs::read(&database_path).unwrap();
    assert!(valid_image.len() > 1);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&database_path)
        .unwrap();
    file.set_len(1).unwrap();
    file.sync_all().unwrap();
    drop(file);

    assert!(
        StorageInstance::open(&fixture.0).is_err(),
        "a truncated redb image must not admit a storage instance"
    );

    std::fs::write(&database_path, valid_image).unwrap();
    let reopened = StorageInstance::open(&fixture.0)
        .expect("failed open must release the lock so a restored image can be reopened");
    assert!(validate_persisted_records(reopened.database()).is_ok());
}

#[test]
fn bootstrap_cannot_publish_ready_before_secret_transfer() {
    let fixture = Fixture::new();
    let session = initialize(&fixture.0, "first_admin", expiry()).unwrap();
    assert!(session.acknowledge().is_err());

    let mut resumed = resume(&fixture.0, "first_admin", expiry()).unwrap();
    assert_eq!(
        validate_persisted_records(resumed.database()),
        Err(StartupError::NotReady)
    );
    let mut secret = resumed.take_secret().unwrap();
    secret.zeroize();
    let ready = resumed.acknowledge().unwrap();
    assert!(validate_persisted_records(ready.database()).is_ok());
}

#[test]
fn resume_recovers_a_private_directory_before_the_marker_was_written() {
    let fixture = Fixture::new();
    std::fs::create_dir(&fixture.0).unwrap();
    std::fs::set_permissions(&fixture.0, std::fs::Permissions::from_mode(0o700)).unwrap();

    let mut session = resume(&fixture.0, "first_admin", expiry()).unwrap();
    let mut secret = session.take_secret().unwrap();
    secret.zeroize();
    let ready = session.acknowledge().unwrap();
    assert!(validate_persisted_records(ready.database()).is_ok());
}

#[test]
fn resume_recovers_a_redb_file_created_before_the_first_commit() {
    let fixture = Fixture::new();
    std::fs::create_dir(&fixture.0).unwrap();
    std::fs::set_permissions(&fixture.0, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(fixture.0.join(".prdb-bootstrap"), b"PRDB-LOCAL-SETUP-1\n").unwrap();
    std::fs::set_permissions(
        fixture.0.join(".prdb-bootstrap"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    drop(Database::create(fixture.0.join("storage.redb")).unwrap());
    std::fs::set_permissions(
        fixture.0.join("storage.redb"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();

    let mut session = resume(&fixture.0, "first_admin", expiry()).unwrap();
    let mut secret = session.take_secret().unwrap();
    secret.zeroize();
    let ready = session.acknowledge().unwrap();
    assert!(validate_persisted_records(ready.database()).is_ok());
}

#[test]
fn bootstrap_refuses_unrecognized_files_in_a_selected_directory() {
    let fixture = Fixture::new();
    std::fs::create_dir(&fixture.0).unwrap();
    std::fs::set_permissions(&fixture.0, std::fs::Permissions::from_mode(0o700)).unwrap();
    let note = fixture.0.join("keep.txt");
    std::fs::write(&note, b"existing user data").unwrap();

    assert!(matches!(
        resume(&fixture.0, "first_admin", expiry()),
        Err(BootstrapError::NotPrivate)
    ));
    assert_eq!(std::fs::read(note).unwrap(), b"existing user data");
}

#[test]
fn local_setup_socket_delivers_once_and_publishes_ready_after_acknowledgement() {
    let fixture = Fixture::new();
    let session = initialize(&fixture.0, "first_admin", expiry()).unwrap();
    let expected_token_id = session.token_id();
    let listener = session.bind_setup_listener().unwrap();
    let socket_path = listener.socket_path().to_path_buf();
    assert_eq!(
        std::fs::metadata(&socket_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o600
    );

    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(socket_path).unwrap();
        let mut handoff = receive_bootstrap_handoff(&mut stream).unwrap();
        assert_eq!(handoff.token_id(), expected_token_id);
        let secret = handoff.take_secret().unwrap();
        handoff.acknowledge_delivery(&mut stream).unwrap();
        secret
    });
    let ready = listener.accept_and_acknowledge(session).unwrap();
    let mut secret = client.join().unwrap();
    assert_eq!(
        verifier_hash(ready.database(), expected_token_id),
        hash_token_secret(&secret)
    );
    secret.zeroize();
    assert!(validate_persisted_records(ready.database()).is_ok());
    assert!(!fixture.0.join(".prdb-setup.sock").exists());
}

#[test]
fn local_setup_socket_disconnect_leaves_initializing_and_resume_rotates_token() {
    let fixture = Fixture::new();
    let session = initialize(&fixture.0, "first_admin", expiry()).unwrap();
    let original_token_id = session.token_id();
    let listener = session.bind_setup_listener().unwrap();
    let socket_path = listener.socket_path().to_path_buf();
    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(socket_path).unwrap();
        let handoff = receive_bootstrap_handoff(&mut stream).unwrap();
        drop(handoff);
    });

    assert!(listener.accept_and_acknowledge(session).is_err());
    client.join().unwrap();
    assert!(!fixture.0.join(".prdb-setup.sock").exists());
    let mut resumed = resume(&fixture.0, "first_admin", expiry()).unwrap();
    assert_ne!(resumed.token_id(), original_token_id);
    let mut secret = resumed.take_secret().unwrap();
    secret.zeroize();
    let ready = resumed.acknowledge().unwrap();
    assert!(validate_persisted_records(ready.database()).is_ok());
}

#[test]
fn resume_removes_a_private_stale_setup_socket_before_rebinding() {
    let fixture = Fixture::new();
    let session = initialize(&fixture.0, "first_admin", expiry()).unwrap();
    drop(session);

    let socket_path = fixture.0.join(".prdb-setup.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(listener);
    assert!(socket_path.exists());

    let mut resumed = resume(&fixture.0, "first_admin", expiry()).unwrap();
    assert!(!socket_path.exists());
    let mut secret = resumed.take_secret().unwrap();
    secret.zeroize();
    let ready = resumed.acknowledge().unwrap();
    assert!(validate_persisted_records(ready.database()).is_ok());
}

#[test]
fn local_setup_socket_rejects_an_acknowledgement_for_another_request() {
    use prdb_engine::bootstrap::SETUP_ACK_OPERATION;

    let fixture = Fixture::new();
    let session = initialize(&fixture.0, "first_admin", expiry()).unwrap();
    let original_token_id = session.token_id();
    let listener = session.bind_setup_listener().unwrap();
    let socket_path = listener.socket_path().to_path_buf();
    let client = std::thread::spawn(move || {
        let mut stream = UnixStream::connect(socket_path).unwrap();
        let mut handoff = read_frame(&mut stream).unwrap().unwrap();
        let request_id = handoff.request_id;
        handoff.payload.zeroize();
        let wrong_request_id = if request_id == u64::MAX {
            request_id - 1
        } else {
            request_id + 1
        };
        write_frame(
            &mut stream,
            &IpcFrame {
                operation: SETUP_ACK_OPERATION,
                request_id: wrong_request_id,
                payload: Vec::new(),
            },
        )
        .unwrap();
    });
    assert!(matches!(
        listener.accept_and_acknowledge(session),
        Err(BootstrapError::InvalidHandoff)
    ));
    client.join().unwrap();

    let mut resumed = resume(&fixture.0, "first_admin", expiry()).unwrap();
    assert_ne!(resumed.token_id(), original_token_id);
    let mut secret = resumed.take_secret().unwrap();
    secret.zeroize();
    let ready = resumed.acknowledge().unwrap();
    assert!(validate_persisted_records(ready.database()).is_ok());
}
