use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::commit::{
    CommitError, IndexWrite, RowWrite, commit_catalog_and_audit, commit_rows_and_indexes,
};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, AUDIT, CATALOG, ROWS};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::{Database, ReadableDatabase, ReadableTable};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn token_admission_checks_expiry_secret_revocation_and_commit_guards()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{
        AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Effect,
        Expr, Grant, Operation, Policy, Subject,
    };
    use prdb_engine::transaction::TransactionError;
    use prdb_engine::tuple::FieldPatch;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(70);
    let admin_user_id = id(0x82);
    let admin_role = builtin_owner_id();
    let active_secret = [0x31; 32];
    let expired_secret = [0x32; 32];
    let revoked_secret = [0x33; 32];
    let login_disable_secret = [0x35; 32];
    let active = credential_verifier_write(
        id(71),
        user_id,
        &active_secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    let expired = credential_verifier_write(
        id(72),
        user_id,
        &expired_secret,
        now - 2_000_000,
        now - 1_000_000,
        None,
        1,
    );
    let revoked = credential_verifier_write(
        id(73),
        user_id,
        &revoked_secret,
        now - 2_000_000,
        now + 60_000_000,
        Some(now - 1_000_000),
        1,
    );
    let live_admin_credential = credential_verifier_write(
        id(0x83),
        admin_user_id,
        &[0x34; 32],
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    let login_disable_credential = credential_verifier_write(
        id(0x85),
        user_id,
        &login_disable_secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    commit_catalog_and_audit(
        &database,
        id(74),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(admin_user_id, 1, 1, "credential_admin"),
            object_write_with_name(admin_user_id, 8, "credential_admin"),
            membership_write(admin_user_id, admin_role),
            dependency_write(admin_user_id, admin_role, 5),
            principal_write_with_name(user_id, 1, 1, "credential_user"),
            object_write_with_name(user_id, 8, "credential_user"),
            grant_write(user_id, id(16), 2, 1, id(16), admin_role),
            grant_write(user_id, id(16), 4, 1, id(16), admin_role),
            dependency_write(user_id, id(16), 5),
            active,
            expired,
            revoked,
            live_admin_credential,
            login_disable_credential,
        ],
        &[audit_event(74)],
    )
    .map_err(|error| format!("seed credential records: {error:?}"))?;

    let storage = StorageManager::new(&database, Duration::from_millis(10))
        .map_err(|error| format!("validate seeded credential records: {error:?}"))?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: false,
    }];
    let row_id = id(0x7a);
    let row_key = table_row_key(id(16), row_id);
    let original_value = ScalarValue::Bytea(b"before token revoke".to_vec());
    let original_payload = encode_tuple(&columns, 1, &[Some(original_value.clone())])?;
    let original_index = typed_bytea_index_key([0x44; 16], &row_key, &original_payload);
    let original_index_key = original_index.key.clone();
    commit_rows_and_indexes(
        &database,
        id(0x84),
        user_id,
        now + 3_600_000_000,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(original_payload),
        }],
        &[IndexWrite {
            key: original_index.key,
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: original_index.has_null,
        }],
    )?;
    let discovered = storage.discover_token_authorized_schemas(id(71), &active_secret)?;
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].name, "information_schema");
    assert!(matches!(
        storage.discover_token_authorized_schemas(id(71), &[0xFF; 32]),
        Err(TransactionError::AuthenticationFailed)
    ));
    let (mut admitted, roles) = storage.begin_token_authenticated_invocation(
        id(75),
        id(71),
        &active_secret,
        now + 3_600_000_000,
        2,
        1024,
    )?;
    assert!(roles.is_empty());
    let schema = admitted.load_catalog_table_schema(id(16))?;
    admitted.load_catalog_indexes(&schema)?;
    let select_grant_key = grant_write(user_id, id(16), 2, 1, id(16), admin_role).key;
    let update_grant_key = grant_write(user_id, id(16), 4, 1, id(16), admin_role).key;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let select_grant_value = catalog
        .get(select_grant_key.as_slice())?
        .ok_or("missing persisted SELECT grant")?
        .value()
        .to_vec();
    let update_grant_value = catalog
        .get(update_grant_key.as_slice())?
        .ok_or("missing persisted UPDATE grant")?
        .value()
        .to_vec();
    drop(catalog);
    drop(read);
    let column_id = schema.column_ids[0];
    let context = AuthorizationContext {
        principal_id: user_id,
        role_ids: roles,
        scope: [
            (Operation::Usage, AccessTarget::Schema(schema.schema_id)),
            (Operation::Select, AccessTarget::Table(schema.table_id)),
            (Operation::Update, AccessTarget::Table(schema.table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: schema.table_id,
                    column_id,
                },
            ),
            (
                Operation::Update,
                AccessTarget::Column {
                    table_id: schema.table_id,
                    column_id,
                },
            ),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema.schema_id),
        },
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Select,
            target: AccessTarget::Table(schema.table_id),
        },
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Update,
            target: AccessTarget::Table(schema.table_id),
        },
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: schema.table_id,
                column_id,
            },
        },
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: schema.table_id,
                column_id,
            },
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal(user_id)],
            operation: Operation::Select,
            target: AccessTarget::Table(schema.table_id),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(user_id)],
            operation: Operation::Update,
            target: AccessTarget::Table(schema.table_id),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Principal(user_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: schema.table_id,
                column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(user_id)],
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: schema.table_id,
                column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let dependencies = [
        AuthorityDependency {
            key: select_grant_key,
            expected_value: Some(select_grant_value),
        },
        AuthorityDependency {
            key: update_grant_key,
            expected_value: Some(update_grant_value),
        },
    ];
    let projection = AuthorizedProjection {
        columns: &schema.columns,
        column_ids: &schema.column_ids,
        schema_version: schema.schema_version,
        schema_id: schema.schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &dependencies,
    };
    admitted.stage_patch_authorized(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(original_value.clone()),
        }],
    )?;
    let original_index_key_for_assert = original_index_key.clone();

    for (invocation_id, token_id, secret) in [
        (id(76), id(71), [0xFF; 32]),
        (id(77), id(72), expired_secret),
        (id(78), id(73), revoked_secret),
    ] {
        assert!(matches!(
            storage.begin_token_authenticated_invocation(
                invocation_id,
                token_id,
                &secret,
                now + 3_600_000_000,
                2,
                64,
            ),
            Err(TransactionError::AuthenticationFailed)
        ));
        assert_eq!(
            lookup_outcome(&database, invocation_id, user_id, now)?,
            None
        );
    }

    let mut revoke_event = audit_event(79);
    revoke_event.actor_id = Some(admin_user_id);
    revoke_event.correlation_id = id(79);
    revoke_event.action = AuditAction::TokenLifecycle;
    revoke_event.target_ids = vec![id(71)];
    let token_revoke_barrier = Arc::new(Barrier::new(2));
    let (write_result, revoke_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&token_revoke_barrier);
        let storage = &storage;
        let revoke = scope.spawn(move || {
            barrier.wait();
            storage.revoke_token(
                id(79),
                admin_user_id,
                id(71),
                now + 3_600_000_000,
                revoke_event,
            )
        });
        token_revoke_barrier.wait();
        let write = admitted.commit();
        let revoke = revoke.join().expect("token revocation thread panicked");
        (write, revoke)
    });
    let revoke_sequence = revoke_result?;
    let (write_sequence, write_status) = match write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(TransactionError::Commit(CommitError::Conflict)) => (None, OutcomeStatus::Aborted),
        Err(error) => panic!("unexpected token revoke race write result: {error:?}"),
    };
    if let Some(sequence) = write_sequence {
        assert!(sequence < revoke_sequence);
    }
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &RowRecord::decode(
                database
                    .begin_read()?
                    .open_table(ROWS)?
                    .get(&row_key[..])?
                    .ok_or("row disappeared after token revocation")?
                    .value(),
            )?
            .payload,
        )?
        .values,
        [Some(original_value.clone())],
        "token revocation must not alter the row value"
    );
    assert_eq!(
        lookup_outcome(&database, id(75), user_id, now)?
            .unwrap()
            .status,
        write_status
    );

    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(80),
            id(71),
            &active_secret,
            now + 3_600_000_000,
            2,
            64,
        ),
        Err(TransactionError::AuthenticationFailed)
    ));
    assert_eq!(lookup_outcome(&database, id(80), user_id, now)?, None);

    let (mut rename_writer, rename_roles) = storage.begin_token_authenticated_invocation(
        id(0x8a),
        id(0x85),
        &login_disable_secret,
        now + 3_600_000_000,
        2,
        1024,
    )?;
    assert!(rename_roles.is_empty());
    rename_writer.stage_patch_authorized(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Bytea(
                b"must not publish after identity rename".to_vec(),
            )),
        }],
    )?;
    let renamed_payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"must not publish after identity rename".to_vec(),
        ))],
    )?;
    let renamed_index = typed_bytea_index_key([0x44; 16], &row_key, &renamed_payload);
    let renamed_index_key = renamed_index.key.clone();
    rename_writer.stage_index_membership(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        original_index_key_for_assert.clone(),
        false,
    )?;
    rename_writer.stage_index_membership(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        renamed_index.key,
        true,
    )?;
    let rename_invocation = id(0x8b);
    let mut rename_audit = identity_audit_event(0x8c, admin_user_id, user_id);
    rename_audit.correlation_id = rename_invocation;
    let rename_race_barrier = Arc::new(Barrier::new(2));
    let (rename_result, write_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&rename_race_barrier);
        let storage = &storage;
        let rename = scope.spawn(move || {
            barrier.wait();
            storage.rename_identity(
                rename_invocation,
                admin_user_id,
                user_id,
                "renamed_credential_user",
                now + 3_600_000_000,
                rename_audit,
            )
        });
        rename_race_barrier.wait();
        let write_result = rename_writer.commit();
        let rename_result = rename.join().expect("identity rename thread panicked");
        (rename_result, write_result)
    });
    let rename_sequence = rename_result.unwrap_or_else(|error| {
        panic!("identity rename failed during staged-write race: {error:?}; write={write_result:?}")
    });
    let (write_sequence, write_status) = match write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(TransactionError::Commit(CommitError::Conflict)) => (None, OutcomeStatus::Aborted),
        Err(error) => panic!("unexpected staged-write result in identity rename race: {error:?}"),
    };
    if let Some(sequence) = write_sequence {
        assert!(sequence < rename_sequence);
    }
    assert_eq!(commit_sequence(&database)?, rename_sequence);
    let expected_after_rename = if write_status == OutcomeStatus::Committed {
        ScalarValue::Bytea(b"must not publish after identity rename".to_vec())
    } else {
        ScalarValue::Bytea(b"before token revoke".to_vec())
    };
    let active_index_key = if write_status == OutcomeStatus::Committed {
        renamed_index_key.clone()
    } else {
        original_index_key_for_assert.clone()
    };
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &RowRecord::decode(
                database
                    .begin_read()?
                    .open_table(ROWS)?
                    .get(&row_key[..])?
                    .ok_or("row disappeared after identity rename")?
                    .value(),
            )?
            .payload,
        )?
        .values,
        [Some(expected_after_rename.clone())],
        "row state must reflect the serialized identity rename race"
    );
    let read = database.begin_read()?;
    let indexes = read.open_table(schema::INDEXES)?;
    assert_eq!(
        indexes
            .get(original_index_key_for_assert.as_slice())?
            .is_some(),
        write_status == OutcomeStatus::Aborted
    );
    assert_eq!(
        indexes.get(renamed_index_key.as_slice())?.is_some(),
        write_status == OutcomeStatus::Committed
    );
    drop(indexes);
    drop(read);
    assert_eq!(
        lookup_outcome(&database, id(0x8a), user_id, now)?
            .ok_or("missing identity-rename stale writer outcome")?
            .status,
        write_status
    );

    let (mut login_disable_writer, login_roles) = storage.begin_token_authenticated_invocation(
        id(0x88),
        id(0x85),
        &login_disable_secret,
        now + 3_600_000_000,
        2,
        1024,
    )?;
    assert!(login_roles.is_empty());
    login_disable_writer.stage_patch_authorized(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Bytea(
                b"must not publish after LOGIN disable".to_vec(),
            )),
        }],
    )?;
    let login_disabled_payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"must not publish after LOGIN disable".to_vec(),
        ))],
    )?;
    let login_disabled_index = typed_bytea_index_key([0x44; 16], &row_key, &login_disabled_payload);
    login_disable_writer.stage_index_membership(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        active_index_key.clone(),
        false,
    )?;
    login_disable_writer.stage_index_membership(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        login_disabled_index.key.clone(),
        true,
    )?;

    let login_disable_invocation = id(0x87);
    let mut login_disable_audit = identity_audit_event(0x86, admin_user_id, user_id);
    login_disable_audit.correlation_id = login_disable_invocation;
    let login_disable_barrier = Arc::new(Barrier::new(2));
    let (login_disable_result, staged_write_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&login_disable_barrier);
        let storage = &storage;
        let disable = scope.spawn(move || {
            barrier.wait();
            storage.set_user_login_enabled(
                login_disable_invocation,
                admin_user_id,
                user_id,
                false,
                now + 3_600_000_000,
                login_disable_audit,
            )
        });
        login_disable_barrier.wait();
        let staged_write_result = login_disable_writer.commit();
        let login_disable_result = disable.join().expect("LOGIN disable thread panicked");
        (login_disable_result, staged_write_result)
    });
    let login_disable_sequence = login_disable_result?;
    let (staged_write_sequence, staged_write_status) = match staged_write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(TransactionError::Commit(CommitError::Conflict)) => (None, OutcomeStatus::Aborted),
        Err(error) => panic!("unexpected staged-write result in LOGIN disable race: {error:?}"),
    };
    if let Some(sequence) = staged_write_sequence {
        assert!(sequence < login_disable_sequence);
    }
    assert_eq!(
        login_disable_sequence,
        rename_sequence
            + if staged_write_status == OutcomeStatus::Committed {
                2
            } else {
                1
            }
    );
    assert_eq!(commit_sequence(&database)?, login_disable_sequence);
    let expected_after_login_disable = if staged_write_status == OutcomeStatus::Committed {
        ScalarValue::Bytea(b"must not publish after LOGIN disable".to_vec())
    } else {
        expected_after_rename
    };
    let index_after_login_disable = if staged_write_status == OutcomeStatus::Committed {
        login_disabled_index.key.as_slice()
    } else {
        active_index_key.as_slice()
    };
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &RowRecord::decode(
                database
                    .begin_read()?
                    .open_table(ROWS)?
                    .get(&row_key[..])?
                    .ok_or("row disappeared after LOGIN disable")?
                    .value(),
            )?
            .payload,
        )?
        .values,
        [Some(expected_after_login_disable)],
        "row state must reflect the serialized LOGIN disable race"
    );
    let read = database.begin_read()?;
    let indexes = read.open_table(schema::INDEXES)?;
    assert!(indexes.get(index_after_login_disable)?.is_some());
    assert_eq!(
        indexes.get(login_disabled_index.key.as_slice())?.is_some(),
        staged_write_status == OutcomeStatus::Committed
    );
    drop(indexes);
    drop(read);
    assert_eq!(
        lookup_outcome(&database, id(0x88), user_id, now)?
            .ok_or("missing LOGIN-disable stale writer outcome")?
            .status,
        staged_write_status
    );
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0x89),
            id(0x85),
            &login_disable_secret,
            now + 3_600_000_000,
            2,
            64,
        ),
        Err(TransactionError::AuthenticationFailed)
    ));
    assert_eq!(lookup_outcome(&database, id(0x89), user_id, now)?, None);
    assert!(
        storage
            .list_tokens(admin_user_id, user_id)?
            .iter()
            .any(|token| token.token_id == id(0x85) && token.revoked)
    );
    assert!(prdb_engine::startup::validate_persisted_records(&database).is_ok());
    Ok(())
}

pub(super) fn client_session_authenticates_and_revalidates_before_operations()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientMode, ClientSessionError};
    use prdb_engine::transaction::TransactionError;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    let credential = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    let credential_value = credential.payload.clone().unwrap();
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "session_user"),
            object_write_with_name(user_id, 8, "session_user"),
            credential,
        ],
        &[audit_event(0x72)],
    )?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let mut session = ClientConnectionSession::default();
    assert!(session.permits_request_kind(1));
    assert!(!session.permits_request_kind(2));
    session.accept_hello(ClientMode::Query, "integration-test")?;
    assert!(session.permits_request_kind(2));
    assert!(!session.permits_request_kind(3));
    let authenticated = session.authenticate(&storage, token_id, &secret)?;
    let session_id = authenticated.connection_id();
    assert_ne!(session_id, [0; 16]);
    assert_eq!(authenticated.principal_id(), user_id);
    assert!(authenticated.role_ids().is_empty());
    assert!(session.staging_owner().is_ok());
    assert!(session.permits_request_kind(3));
    session.authorize_request(&storage, 3)?;
    assert_eq!(session.session_id()?, session_id);

    let mut revoked = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        Some(now),
        2,
    );
    revoked.expected_value = Some(credential_value);
    storage.commit_catalog_and_audit(
        id(0x73),
        id(9),
        now + 3_600_000_000,
        &[revoked],
        &[audit_event(0x73)],
    )?;
    assert!(matches!(
        session.authorize_request(&storage, 8),
        Err(ClientSessionError::Storage(
            TransactionError::AuthenticationFailed
        ))
    ));
    assert_eq!(session.session_id()?, session_id);
    Ok(())
}

pub(super) fn client_handshake_requires_hello_first_and_sequences_responses()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientSessionError};
    use prdb_engine::supervisor::{
        StagingLimits, StagingStore, SubmissionMetadata, SubmissionMetadataValidator,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayTerminalDecision,
        finalize_gateway_invocation_and_send_terminal, send_gateway_result_data_event,
    };
    use sha2::{Digest, Sha256};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct AcceptUpload;
    impl SubmissionMetadataValidator for AcceptUpload {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &SubmissionMetadata) -> bool {
            true
        }
    }

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "handshake_user"),
            object_write_with_name(user_id, 8, "handshake_user"),
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
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut session = ClientConnectionSession::default();
    assert!(matches!(
        session.handle_handshake_request(
            &storage,
            ClientFrame {
                kind: 11,
                request_id: 1,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            },
        ),
        Err(ClientSessionError::UnsupportedRequest)
    ));
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptUpload))?;
    let admission = GatewayAdmissionConfig {
        retention_period_us: 3_600_000_000,
        max_rows: 2,
        max_bytes: 64,
    };

    let token_id_hex = token_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let token_secret_base64 = "A".repeat(43) + "=";
    let auth_payload =
        format!("{{\"token_id\":\"{token_id_hex}\",\"token_secret\":\"{token_secret_base64}\"}}");
    let artifact = b"upload";
    let artifact_hash = Sha256::digest(artifact)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let empty_hash = Sha256::digest([])
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (mut service, mut ipc_gateway) = UnixStream::pair()?;
    service.set_read_timeout(Some(Duration::from_secs(10)))?;
    ipc_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    let tcp_listener = TcpListener::bind(("127.0.0.1", 0))?;
    let mut gateway = TcpStream::connect(tcp_listener.local_addr()?)?;
    let (mut gateway_tcp, _) = tcp_listener.accept()?;
    gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    gateway_tcp.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut session = std::thread::scope(|scope| {
        let storage_ref = &storage;
        let staging_ref = &staging;
        let service_thread = scope.spawn(move || {
            let mut service_session = session;
            let mut started = false;
            loop {
                match prdb_engine::supervisor_ipc::serve_gateway_client_session(
                    &mut service,
                    storage_ref,
                    staging_ref,
                    &mut service_session,
                    admission,
                )
                .map_err(|error| format!("gateway service failed: {error:?}"))?
                {
                    prdb_engine::supervisor_ipc::GatewayClientServiceEvent::Started {
                        request_id,
                        invocation,
                        submission,
                        executor: None,
                    } => {
                        assert_eq!(submission.artifact(), artifact);
                        let owner = service_session
                            .staging_owner()
                            .map_err(|error| format!("session owner unavailable: {error:?}"))?;
                        assert_eq!(
                            staging_ref
                                .status(owner, invocation.invocation_id())
                                .map_err(|error| format!("staging status failed: {error:?}"))?,
                            prdb_engine::supervisor::SubmissionState::Started
                        );
                        let mut invocation = invocation;
                        let mut worker = std::process::Command::new("true")
                            .spawn()
                            .map_err(|error| format!("worker spawn failed: {error:?}"))?;
                        invocation
                            .observe_worker_exit(&mut worker)
                            .map_err(|error| format!("worker exit failed: {error:?}"))?;
                        send_gateway_result_data_event(&mut service, request_id, b"rows".to_vec())
                            .map_err(|error| format!("result event failed: {error:?}"))?;
                        finalize_gateway_invocation_and_send_terminal(
                            &mut invocation,
                            &mut service,
                            request_id,
                            GatewayTerminalDecision::Commit,
                        )
                        .map_err(|error| format!("terminal relay failed: {error:?}"))?;
                        staging_ref
                            .finish_started(owner, invocation.invocation_id())
                            .map_err(|error| format!("staging cleanup failed: {error:?}"))?;
                        started = true;
                    }
                    prdb_engine::supervisor_ipc::GatewayClientServiceEvent::Started {
                        executor: Some(_),
                        ..
                    } => unreachable!("legacy session unexpectedly launched an executor"),
                    prdb_engine::supervisor_ipc::GatewayClientServiceEvent::Disconnected => {
                        if !started {
                            return Err("disconnect before START".to_owned());
                        }
                        return Ok::<_, String>(service_session);
                    }
                }
            }
        });

        let bridge_thread = scope.spawn(move || {
            prdb_engine::gateway::serve_tcp_client_connection(&mut gateway_tcp, &mut ipc_gateway)
                .map_err(|error| format!("TCP gateway bridge failed: {error:?}"))
        });

        let mut response_decoder = prdb_engine::client_protocol::ClientFrameDecoder::default();
        let mut response_buffer = [0u8; 8192];
        let mut pending_responses = std::collections::VecDeque::new();

        macro_rules! next_response {
            () => {{
                loop {
                    if let Some(response) = pending_responses.pop_front() {
                        break response;
                    }
                    let count = std::io::Read::read(&mut gateway, &mut response_buffer)?;
                    if count == 0 {
                        return Err("missing gateway response".into());
                    }
                    pending_responses.extend(response_decoder.push(&response_buffer[..count])?);
                }
            }};
        }

        macro_rules! round_trip {
            ($kind:expr, $request_id:expr, $payload:expr) => {{
                let encoded = ClientFrame {
                    kind: $kind,
                    request_id: $request_id,
                    response_sequence: 0,
                    payload: $payload,
                }
                .encode()?;
                std::io::Write::write_all(&mut gateway, &encoded)?;
                next_response!()
            }};
        }

        let hello = round_trip!(
            1,
            2,
            br#"{"mode":"query","client_name":"test","sdk_library_mode":"repository"}"#.to_vec()
        );
        assert_eq!(hello.kind, 100);
        assert!(std::str::from_utf8(&hello.payload)?.contains("\"mode\":\"query\""));
        let auth = round_trip!(2, 3, auth_payload.into_bytes());
        assert_eq!(auth.kind, 100);
        assert!(std::str::from_utf8(&auth.payload)?.contains("\"authenticated\":true"));
        let auth_response = std::str::from_utf8(&auth.payload)?;
        let session_id = auth_response
            .split("\"session_id\":\"")
            .nth(1)
            .and_then(|value| value.get(..32))
            .ok_or("AUTH response omitted its session ID")?;
        assert_eq!(session_id.len(), 32);

        let prepare = round_trip!(
            3,
            4,
            format!("{{\"manifest_version\":1,\"manifest\":\"bQ==\",\"artifact_bytes\":\"6\",\"artifact_sha256\":\"{artifact_hash}\",\"parameter_bytes\":\"0\",\"parameter_sha256\":\"{empty_hash}\",\"scope_version\":1,\"requested_scope\":\"UFNDMQABAgIBAQEBAQEBAQEBAQEBAQEB\",\"deadline_ms\":\"30000\",\"workload_class\":\"operational\",\"sdk_library_mode\":\"repository\"}}").into_bytes()
        );
        let prepare_response = std::str::from_utf8(&prepare.payload)?;
        let invocation_hex = prepare_response
            .split("\"invocation_id\":\"")
            .nth(1)
            .and_then(|value| value.get(..32))
            .ok_or("PREPARE response omitted invocation ID")?;
        let invocation_id = prdb_engine::client_protocol::decode_client_seal(
            format!("{{\"invocation_id\":\"{invocation_hex}\"}}").as_bytes(),
        )?;
        let mut chunk_payload = invocation_id.to_vec();
        chunk_payload.extend_from_slice(&0u64.to_le_bytes());
        chunk_payload.extend_from_slice(artifact);
        let chunk = round_trip!(4, 5, chunk_payload);
        assert_eq!(chunk.payload, b"{\"next_offset\":\"6\"}");
        let seal = round_trip!(
            6,
            6,
            format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes()
        );
        assert_eq!(seal.payload, b"{\"state\":\"prepared\"}");
        let status = round_trip!(
            8,
            7,
            format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes()
        );
        assert_eq!(status.payload, b"{\"state\":\"prepared\"}");
        let accepted = round_trip!(
            7,
            8,
            format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes()
        );
        assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");
        let result_data = next_response!();
        assert_eq!(result_data.kind, 102);
        assert_eq!(result_data.request_id, 8);
        assert_eq!(result_data.response_sequence, 1);
        assert_eq!(result_data.payload, b"rows");
        let terminal = next_response!();
        assert_eq!(terminal.kind, 103);
        assert_eq!(terminal.request_id, 8);
        assert_eq!(terminal.response_sequence, 2);
        assert!(std::str::from_utf8(&terminal.payload)?.contains("\"outcome\":\"committed\""));
        gateway.shutdown(std::net::Shutdown::Write)?;
        bridge_thread
            .join()
            .map_err(|_| std::io::Error::other("gateway bridge thread panicked"))?
            .map_err(std::io::Error::other)?;
        let session = service_thread
            .join()
            .map_err(|_| std::io::Error::other("gateway service thread panicked"))?
            .map_err(std::io::Error::other)?;
        Ok::<_, Box<dyn std::error::Error>>(session)
    })?;

    let ping = session.handle_handshake_request(
        &storage,
        ClientFrame {
            kind: 11,
            request_id: 9,
            response_sequence: 0,
            payload: b"{}".to_vec(),
        },
    )?;
    assert_eq!(ping.payload, b"{\"alive\":true}");
    assert!(matches!(
        session.handle_handshake_request(
            &storage,
            ClientFrame {
                kind: 1,
                request_id: 10,
                response_sequence: 0,
                payload: br#"{"mode":"query","client_name":"again"}"#.to_vec(),
            },
        ),
        Err(ClientSessionError::UnsupportedRequest)
    ));
    Ok(())
}

pub(super) fn control_cancel_over_tcp_reports_too_late_during_durable_commit()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
    use prdb_engine::supervisor::{
        StagingLimits, StagingStore, SubmissionMetadata, SubmissionMetadataValidator,
    };
    use prdb_engine::supervisor_ipc::{GatewayAdmissionConfig, serve_gateway_client_session};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;

    struct AcceptMetadata;
    impl SubmissionMetadataValidator for AcceptMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &SubmissionMetadata) -> bool {
            true
        }
    }

    let fixture = Fixture::new();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(fixture.path())?;
    let pause_state = Arc::new(PauseNextWriteState::new());
    let database = Database::builder().create_with_backend(PauseNextWriteBackend {
        inner: redb::backends::FileBackend::new(file)?,
        state: Arc::clone(&pause_state),
    })?;
    initialize_database(&database)?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_id = id(0x70);
    let token_id = id(0x71);
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(admin_id, 1, 1, "commit_control_admin"),
        object_write_with_name(admin_id, 8, "commit_control_admin"),
        membership_write_with_grantor(admin_id, admin_role, admin_role),
        dependency_write(admin_id, admin_role, 5),
        credential_verifier_write(token_id, admin_id, &[0; 32], 1, i64::MAX, None, 1),
    ];
    let mut setup_audit = audit_event(0x72);
    setup_audit.actor_id = Some(admin_role);
    storage.commit_catalog_and_audit(id(0x72), admin_role, 9_999, &records, &[setup_audit])?;

    let invocation_id = id(0x73);
    let mut invocation = storage.begin_invocation(invocation_id, admin_id, i64::MAX, 1, 64)?;
    invocation.stage_put(
        prdb_engine::overlay::RowId {
            table: id(0x74),
            row: id(0x75),
        },
        b"commit-boundary",
    )?;
    pause_state.armed.store(true, Ordering::SeqCst);
    std::thread::scope(|commit_scope| -> Result<(), Box<dyn std::error::Error>> {
        let commit_thread = commit_scope.spawn(move || invocation.commit());
        pause_state.entered.wait();

        let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptMetadata))?;
        let admission = GatewayAdmissionConfig {
            retention_period_us: i64::MAX,
            max_rows: 2,
            max_bytes: 64,
        };
        let (mut service, mut ipc_gateway) = UnixStream::pair()?;
        service.set_read_timeout(Some(Duration::from_secs(5)))?;
        ipc_gateway.set_read_timeout(Some(Duration::from_secs(5)))?;
        let tcp_listener = TcpListener::bind(("127.0.0.1", 0))?;
        let mut client = TcpStream::connect(tcp_listener.local_addr()?)?;
        let (mut gateway_tcp, _) = tcp_listener.accept()?;
        client.set_read_timeout(Some(Duration::from_secs(5)))?;
        gateway_tcp.set_read_timeout(Some(Duration::from_secs(5)))?;

        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let service_thread = scope.spawn(|| {
                let mut session = prdb_engine::client_protocol::ClientConnectionSession::default();
                match serve_gateway_client_session(
                    &mut service,
                    &storage,
                    &staging,
                    &mut session,
                    admission,
                )
                .map_err(|error| {
                    std::io::Error::other(format!("gateway service failed: {error:?}"))
                })? {
                    prdb_engine::supervisor_ipc::GatewayClientServiceEvent::Disconnected => Ok(()),
                    prdb_engine::supervisor_ipc::GatewayClientServiceEvent::Started { .. } => Err(
                        std::io::Error::other("control session unexpectedly started a query"),
                    ),
                }
            });
            let bridge_thread = scope.spawn(move || {
                prdb_engine::gateway::serve_tcp_client_connection(
                    &mut gateway_tcp,
                    &mut ipc_gateway,
                )
            });

            let mut decoder = ClientFrameDecoder::default();
            let mut next_request_id = 1;
            let mut round_trip =
                |kind: u16, payload: Vec<u8>| -> Result<ClientFrame, Box<dyn std::error::Error>> {
                    let request = ClientFrame {
                        kind,
                        request_id: next_request_id,
                        response_sequence: 0,
                        payload,
                    };
                    next_request_id += 1;
                    client.write_all(&request.encode()?)?;
                    let mut buffer = [0u8; 4096];
                    loop {
                        let count = client.read(&mut buffer)?;
                        if count == 0 {
                            return Err("gateway closed before replying".into());
                        }
                        if let Some(frame) = decoder.push(&buffer[..count])?.into_iter().next() {
                            return Ok(frame);
                        }
                    }
                };

            let hello = round_trip(
                1,
                br#"{"mode":"control","client_name":"commit-boundary"}"#.to_vec(),
            )?;
            assert_eq!(hello.kind, 100);
            let token_id_hex = token_id
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let auth = round_trip(
                2,
                format!(
                    "{{\"token_id\":\"{token_id_hex}\",\"token_secret\":\"{}\"}}",
                    "A".repeat(43) + "="
                )
                .into_bytes(),
            )?;
            assert_eq!(auth.kind, 100);
            let cancel_payload = format!(
                "{{\"invocation_id\":\"{}\"}}",
                invocation_id
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )
            .into_bytes();
            let mut cancel_stream = client.try_clone()?;
            let (sent_sender, sent_receiver) = std::sync::mpsc::sync_channel(0);
            let cancel_thread = scope.spawn(
                move || -> Result<ClientFrame, Box<dyn std::error::Error + Send + Sync>> {
                    let request = ClientFrame {
                        kind: 9,
                        request_id: next_request_id,
                        response_sequence: 0,
                        payload: cancel_payload,
                    };
                    cancel_stream.write_all(&request.encode()?)?;
                    sent_sender.send(()).map_err(std::io::Error::other)?;
                    let mut decoder = ClientFrameDecoder::default();
                    let mut buffer = [0u8; 4096];
                    loop {
                        let count = cancel_stream.read(&mut buffer)?;
                        if count == 0 {
                            return Err("gateway closed before CANCEL response".into());
                        }
                        if let Some(frame) = decoder.push(&buffer[..count])?.into_iter().next() {
                            return Ok(frame);
                        }
                    }
                },
            );
            sent_receiver.recv_timeout(Duration::from_secs(5))?;
            // Let the service dispatch CANCEL while the writer is held at its durable boundary.
            std::thread::sleep(Duration::from_millis(200));
            pause_state.release.wait();
            let cancel = cancel_thread
                .join()
                .map_err(|_| std::io::Error::other("CANCEL client thread panicked"))?
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            assert_eq!(cancel.kind, 100);
            assert_eq!(cancel.payload, br#"{"state":"too_late"}"#);

            client.shutdown(std::net::Shutdown::Write)?;
            bridge_thread
                .join()
                .map_err(|_| std::io::Error::other("TCP bridge panicked"))??;
            service_thread
                .join()
                .map_err(|_| std::io::Error::other("gateway service panicked"))??;
            Ok(())
        })?;

        assert!(
            commit_thread
                .join()
                .map_err(|_| std::io::Error::other("commit thread panicked"))??
                > 0
        );
        let status = storage
            .get_invocation_status(invocation_id, admin_id, 10_000)?
            .ok_or("committed invocation status disappeared")?;
        assert!(matches!(
            status.status,
            prdb_engine::storage_manager::InvocationStatus::Terminal(outcome)
                if outcome.status == OutcomeStatus::Committed
        ));
        let audit_read = database.begin_read()?;
        let audit_table = audit_read.open_table(AUDIT)?;
        let mut denied_cancellation_audited = false;
        for entry in audit_table.iter()? {
            let (_, value) = entry?;
            let event = AuditEvent::decode(value.value())?;
            denied_cancellation_audited |= event.correlation_id == invocation_id
                && event.action == AuditAction::AdministrativeCancellation
                && event.result == AuditResult::Denied;
        }
        assert!(denied_cancellation_audited);
        Ok(())
    })?;
    Ok(())
}

pub(super) fn gateway_ipc_status_and_cancel_keep_unknown_ids_non_enumerating()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{IpcFrame, Operation, ServiceRole};
    use prdb_engine::supervisor_ipc::{
        SUPERVISOR_PAYLOAD_VERSION, dispatch_client_control_request,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut payload = SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec();
    payload.extend_from_slice(&id(0x61));
    let status = dispatch_client_control_request(
        ServiceRole::Gateway,
        &storage,
        id(0x62),
        &IpcFrame {
            operation: Operation::ClientGetInvocationStatus as u16,
            request_id: 1,
            payload: payload.clone(),
        },
    )?;
    assert_eq!(
        status.payload,
        [SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().as_slice(), &[0]].concat()
    );
    let cancellation = dispatch_client_control_request(
        ServiceRole::Gateway,
        &storage,
        id(0x62),
        &IpcFrame {
            operation: Operation::ClientCancelInvocation as u16,
            request_id: 2,
            payload: payload.clone(),
        },
    )?;
    assert_eq!(
        cancellation.payload,
        [SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().as_slice(), &[3]].concat()
    );
    assert!(
        dispatch_client_control_request(
            ServiceRole::Executor,
            &storage,
            id(0x62),
            &IpcFrame {
                operation: Operation::ClientGetInvocationStatus as u16,
                request_id: 3,
                payload,
            },
        )
        .is_err()
    );
    Ok(())
}

pub(super) fn gateway_control_socket_serves_bounded_status_requests()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use prdb_engine::supervisor_ipc::{SUPERVISOR_PAYLOAD_VERSION, serve_client_control};
    use std::os::unix::net::UnixStream;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let (mut service, mut gateway) = UnixStream::pair()?;
    let mut payload = SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec();
    payload.extend_from_slice(&id(0x63));
    write_frame(
        &mut gateway,
        &IpcFrame {
            operation: Operation::ClientGetInvocationStatus as u16,
            request_id: 1,
            payload,
        },
    )?;
    gateway.shutdown(std::net::Shutdown::Write)?;
    serve_client_control(&mut service, &storage, id(0x64)).unwrap();
    let response = read_frame(&mut gateway)?.ok_or("missing gateway status response")?;
    assert_eq!(
        response.operation,
        Operation::ClientGetInvocationStatus as u16
    );
    assert_eq!(response.request_id, 1);
    assert_eq!(
        response.payload,
        [SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().as_slice(), &[0]].concat()
    );
    Ok(())
}
