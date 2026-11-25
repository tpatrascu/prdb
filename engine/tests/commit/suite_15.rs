use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::lookup_principal_id_by_name;
use prdb_engine::commit::{CatalogWrite, CommitError};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{CATALOG, ROWS};
use prdb_engine::storage_manager::{StorageManager, TokenIssueRequest};
use redb::ReadableDatabase;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn identity_ddl_drops_unreferenced_user_and_revokes_tokens_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0xd1);
    let dropped_user = id(0xd2);
    let admin_token = id(0xd3);
    let dropped_token = id(0xd4);
    let dropped_role = id(0xd7);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut seed_audit = identity_audit_event(0xd0, admin_role, admin_user);
    seed_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xd0),
        admin_role,
        9_999,
        &[
            principal_write_with_name(admin_user, 1, 1, "drop_admin"),
            object_write_with_name(admin_user, 8, "drop_admin"),
            principal_write_with_name(dropped_user, 1, 1, "drop_target"),
            object_write_with_parent_and_owner_and_name(
                dropped_user,
                8,
                None,
                admin_user,
                "drop_target",
            ),
            principal_write_with_name(dropped_role, 2, 1, "drop_role"),
            object_write_with_parent_and_owner_and_name(
                dropped_role,
                9,
                None,
                admin_user,
                "drop_role",
            ),
            membership_write_with_grantor(admin_user, admin_role, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(admin_token, admin_user, &[0xd3; 32], 1, i64::MAX, None, 1),
            credential_verifier_write(
                dropped_token,
                dropped_user,
                &[0xd4; 32],
                1,
                i64::MAX,
                None,
                1,
            ),
        ],
        &[seed_audit],
    )?;
    let read_catalog = || -> Result<_, Box<dyn std::error::Error>> {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        Ok((
            catalog
                .get(id_catalog_key(2, dropped_user).as_slice())?
                .map(|v| v.value().to_vec()),
            catalog
                .get(id_catalog_key(10, dropped_user).as_slice())?
                .map(|v| v.value().to_vec()),
            catalog
                .get(id_catalog_key(2, dropped_role).as_slice())?
                .map(|v| v.value().to_vec()),
            catalog
                .get(id_catalog_key(10, dropped_role).as_slice())?
                .map(|v| v.value().to_vec()),
            catalog
                .get(id_catalog_key(16, dropped_token).as_slice())?
                .map(|v| v.value().to_vec()),
        ))
    };
    let (object, principal, role_object, role_principal, token) = read_catalog()?;
    let revoked_token = catalog_expect(
        credential_verifier_write(
            dropped_token,
            dropped_user,
            &[0xd4; 32],
            1,
            i64::MAX,
            Some(2),
            2,
        ),
        token.ok_or("missing target token")?,
    );
    let drop_writes = [
        catalog_delete(
            CatalogWrite {
                key: id_catalog_key(2, dropped_user),
                expected_value: None,
                payload: None,
            },
            object.ok_or("missing user object")?,
        ),
        catalog_delete(
            CatalogWrite {
                key: id_catalog_key(10, dropped_user),
                expected_value: None,
                payload: None,
            },
            principal.ok_or("missing user principal")?,
        ),
        identity_tombstone_write(dropped_user),
        catalog_delete(
            CatalogWrite {
                key: id_catalog_key(2, dropped_role),
                expected_value: None,
                payload: None,
            },
            role_object.ok_or("missing role object")?,
        ),
        catalog_delete(
            CatalogWrite {
                key: id_catalog_key(10, dropped_role),
                expected_value: None,
                payload: None,
            },
            role_principal.ok_or("missing role principal")?,
        ),
        identity_tombstone_write(dropped_role),
        revoked_token,
    ];
    let mut incomplete_drop_audit = identity_audit_event(0xd6, admin_user, dropped_user);
    incomplete_drop_audit.target_ids = vec![dropped_user];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0xd6),
            admin_user,
            9_999,
            &drop_writes[..6],
            &[incomplete_drop_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    let mut drop_user_audit = identity_audit_event(0xd5, admin_user, dropped_user);
    drop_user_audit.correlation_id = id(0xd5);
    let drop_result =
        storage.drop_identity(id(0xd5), admin_user, dropped_user, 9_999, drop_user_audit);
    assert_eq!(drop_result, Ok(2));
    let mut drop_role_audit = identity_audit_event(0xd8, admin_user, dropped_role);
    drop_role_audit.correlation_id = id(0xd8);
    assert_eq!(
        storage.drop_identity(id(0xd8), admin_user, dropped_role, 9_999, drop_role_audit)?,
        3
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(2, dropped_user).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(10, dropped_user).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(17, dropped_user).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(id_catalog_key(2, dropped_role).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(10, dropped_role).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(17, dropped_role).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(id_catalog_key(16, dropped_token).as_slice())?
            .is_some()
    );
    drop(catalog);
    drop(read);
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0xd9),
            dropped_token,
            &[0xd4; 32],
            2,
            2,
            64,
        ),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));
    assert_eq!(lookup_outcome(&database, id(0xd9), dropped_user, 2)?, None);
    assert_eq!(commit_sequence(&database)?, 3);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn token_rotation_racing_a_staged_write_linearizes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let admin_user = id(0xfa);
    let user_id = id(0xfb);
    let token_id = id(0xfc);
    let secret = [0xa5; 32];
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_micros(),
    )?;
    let retention_deadline_us = now + 60_000_000;
    let mut setup_audit = identity_audit_event(0xf6, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xf6),
        admin_role,
        retention_deadline_us,
        &[
            principal_write_with_name(admin_user, 1, 1, "token_race_admin"),
            object_write_with_name(admin_user, 8, "token_race_admin"),
            membership_write(admin_user, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(
                id(0xfd),
                admin_user,
                &[0x5a; 32],
                now - 1,
                now + 60_000_000,
                None,
                1,
            ),
            principal_write_with_name(user_id, 1, 1, "token_race_user"),
            object_write_with_name(user_id, 8, "token_race_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[setup_audit],
    )?;

    let writer_invocation = id(0xfe);
    let (mut writer, roles) = storage.begin_token_authenticated_invocation(
        writer_invocation,
        token_id,
        &secret,
        retention_deadline_us,
        2,
        64,
    )?;
    assert!(roles.is_empty());
    let row_schema = writer.load_catalog_table_schema(id(16))?;
    writer.load_catalog_indexes(&row_schema)?;
    writer.load_catalog_check_constraints(&row_schema)?;
    let reader_invocation = id(0xff);
    let (mut reader, reader_roles) = storage.begin_token_authenticated_invocation(
        reader_invocation,
        token_id,
        &secret,
        retention_deadline_us,
        2,
        64,
    )?;
    assert!(reader_roles.is_empty());
    let pinned_schema_before_rotation = reader.load_catalog_table_schema(id(16))?;
    let stale_writer_invocation = id(0xab);
    let (mut stale_writer, _) = storage.begin_token_authenticated_invocation(
        stale_writer_invocation,
        token_id,
        &secret,
        retention_deadline_us,
        2,
        64,
    )?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(16))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    stale_writer.load_catalog_check_constraints(&stale_schema)?;
    let stale_row_id = prdb_engine::overlay::RowId {
        table: id(16),
        row: id(0xa5),
    };
    stale_writer.stage_put(stale_row_id, &bytea_tuple(b"stale before rotation"))?;
    let row_id = prdb_engine::overlay::RowId {
        table: id(16),
        row: id(0xa6),
    };
    writer.stage_put(row_id, &bytea_tuple(b"write before rotation"))?;

    let rotation_invocation = id(0xa7);
    let barrier = Arc::new(Barrier::new(2));
    let (write_result, rotation_result) = std::thread::scope(|scope| {
        let rotation_barrier = barrier.clone();
        let rotation_storage = &storage;
        let rotation = scope.spawn(move || {
            let mut audit = audit_event(0xa8);
            audit.actor_id = Some(admin_user);
            audit.correlation_id = rotation_invocation;
            audit.action = AuditAction::TokenLifecycle;
            audit.target_ids = vec![token_id];
            rotation_barrier.wait();
            rotation_storage.rotate_token(prdb_engine::storage_manager::TokenRotationRequest {
                invocation_id: rotation_invocation,
                authenticated_actor_id: admin_user,
                old_token_id: token_id,
                label: "rotated after staged write",
                expires_at_us: now + 60_000_000,
                retention_deadline_us,
                audit_event: audit,
            })
        });
        barrier.wait();
        let write = writer.commit();
        (
            write,
            rotation.join().expect("token rotation thread panicked"),
        )
    });

    let write_result_debug = format!("{write_result:?}");
    let write_status = match write_result {
        Ok(sequence) => {
            assert!(sequence >= 2);
            OutcomeStatus::Committed
        }
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        Err(error) => panic!("unexpected staged-write result in token race: {error:?}"),
    };
    let mut rotated = match rotation_result {
        Ok(rotated) => rotated,
        Err(error) => panic!("token rotation failed: {error:?}"),
    };
    let replacement_token_id = rotated.token_id;
    let replacement_secret = rotated
        .take_secret()
        .ok_or("rotated token secret missing")?;
    let rotation_sequence = rotated.commit_sequence;
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, user_id, now)?
            .ok_or("missing stale token writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(commit_sequence(&database)?, rotation_sequence);
    let pinned_schema_after_rotation = reader.load_catalog_table_schema(id(16))?;
    assert_eq!(
        pinned_schema_after_rotation.table_id,
        pinned_schema_before_rotation.table_id
    );
    assert_eq!(
        pinned_schema_after_rotation.schema_version,
        pinned_schema_before_rotation.schema_version
    );
    assert_eq!(
        pinned_schema_after_rotation.column_ids,
        pinned_schema_before_rotation.column_ids
    );
    reader.abort()?;
    assert_eq!(
        lookup_outcome(&database, reader_invocation, user_id, now)?
            .ok_or("missing admitted-reader abort outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(commit_sequence(&database)?, rotation_sequence);
    assert_eq!(
        rotation_sequence,
        if write_status == OutcomeStatus::Committed {
            3
        } else {
            2
        }
    );
    let writer_outcome = lookup_outcome(&database, writer_invocation, user_id, now)?;
    assert!(
        writer_outcome.is_some(),
        "missing staged-writer race outcome: result={write_result_debug}, rotation_sequence={rotation_sequence}, current_sequence={:?}",
        commit_sequence(&database)?
    );
    assert_eq!(writer_outcome.unwrap().status, write_status);
    assert_eq!(
        lookup_outcome(&database, rotation_invocation, admin_user, now)?
            .ok_or("missing token-rotation race outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let row_key = table_row_key(row_id.table, row_id.row);
    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    assert_eq!(
        rows.get(row_key.as_slice())?.map(|value| {
            RowRecord::decode(value.value())
                .expect("committed row record is valid")
                .payload
        }),
        (write_status == OutcomeStatus::Committed).then(|| bytea_tuple(b"write before rotation"))
    );
    assert!(
        rows.get(table_row_key(stale_row_id.table, stale_row_id.row).as_slice())?
            .is_none()
    );
    drop(rows);
    drop(read);
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0xa9),
            token_id,
            &secret,
            retention_deadline_us,
            2,
            64,
        ),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));
    assert_eq!(lookup_outcome(&database, id(0xa9), user_id, now)?, None);
    let inventory = storage.list_tokens(admin_user, user_id)?;
    assert!(
        inventory
            .iter()
            .any(|token| token.token_id == token_id && token.revoked)
    );
    assert!(
        inventory
            .iter()
            .any(|token| token.token_id == replacement_token_id && !token.revoked)
    );
    let (mut replacement_invocation, _) = storage.begin_token_authenticated_invocation(
        id(0xaa),
        replacement_token_id,
        &replacement_secret,
        retention_deadline_us,
        2,
        64,
    )?;
    replacement_invocation.abort()?;
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn identity_ddl_restricts_user_drop_when_a_grant_references_the_user()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0xe1);
    let dropped_user = id(0xe2);
    let admin_token = id(0xe3);
    let dropped_token = id(0xe4);
    let admin_role = builtin_owner_id();
    let database_id = id(0x33);
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut seed_audit = identity_audit_event(0xe0, admin_role, admin_user);
    seed_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xe0),
        admin_role,
        9_999,
        &[
            principal_write_with_name(admin_user, 1, 1, "restrict_admin"),
            object_write_with_name(admin_user, 8, "restrict_admin"),
            principal_write_with_name(dropped_user, 1, 1, "restrict_target"),
            object_write_with_parent_and_owner_and_name(
                dropped_user,
                8,
                None,
                admin_user,
                "restrict_target",
            ),
            membership_write_with_grantor(admin_user, admin_role, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(admin_token, admin_user, &[0xe3; 32], 1, i64::MAX, None, 1),
            credential_verifier_write(
                dropped_token,
                dropped_user,
                &[0xe4; 32],
                1,
                i64::MAX,
                None,
                1,
            ),
            grant_write(dropped_user, database_id, 12, 1, database_id, admin_role),
            dependency_write(dropped_user, database_id, 5),
        ],
        &[seed_audit],
    )?;
    let grant = grant_write(dropped_user, database_id, 12, 1, database_id, admin_role);
    let mut drop_audit = identity_audit_event(0xe5, admin_user, dropped_user);
    drop_audit.correlation_id = id(0xe5);
    assert_eq!(
        storage.drop_identity(id(0xe5), admin_user, dropped_user, 9_999, drop_audit,),
        Err(prdb_engine::storage_manager::IdentityLifecycleError::Unauthorized)
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(grant.key.as_slice())?.is_some());
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn storage_manager_authorizes_identity_ddl_and_preserves_a_login_admin()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0x85);
    let user_manager = id(0x86);
    let created_user = id(0x87);
    let created_role = id(0x88);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let seed_records = [
        principal_write_with_name(admin_user, 1, 1, "identity_admin"),
        object_write_with_name(admin_user, 8, "identity_admin"),
        principal_write_with_name(user_manager, 1, 1, "identity_user_manager"),
        object_write_with_name(user_manager, 8, "identity_user_manager"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        credential_verifier_write(id(0x8d), admin_user, &[0x8d; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(0x8e), admin_user, &[0x8e; 32], 1, 2, None, 1),
        grant_write(user_manager, database_id, 12, 1, database_id, admin_role),
        dependency_write(user_manager, database_id, 5),
    ];
    let mut seed_audit = identity_audit_event(0x80, admin_role, admin_user);
    seed_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x80), admin_role, 9_999, &seed_records, &[seed_audit])?;

    let user_create = [
        principal_write_with_name(created_user, 1, 1, "created_user"),
        object_write_with_parent_and_owner_and_name(
            created_user,
            8,
            None,
            user_manager,
            "created_user",
        ),
    ];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x81),
            user_manager,
            9_999,
            &user_create,
            &[identity_audit_event(0x81, user_manager, created_user)],
        )?,
        2
    );

    let role_create = [
        principal_write_with_name(created_role, 2, 1, "created_role"),
        object_write_with_parent_and_owner_and_name(
            created_role,
            9,
            None,
            user_manager,
            "created_role",
        ),
    ];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x82),
            user_manager,
            9_999,
            &role_create,
            &[identity_audit_event(0x82, user_manager, created_role)],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let current_value = |key: &[u8]| -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(database
            .begin_read()?
            .open_table(CATALOG)?
            .get(key)?
            .ok_or("missing identity record")?
            .value()
            .to_vec())
    };
    let admin_token_secret = [0x8d; 32];
    let mut revoke_admin_token = credential_verifier_write(
        id(0x8d),
        admin_user,
        &admin_token_secret,
        1,
        i64::MAX,
        Some(2),
        2,
    );
    revoke_admin_token.expected_value = Some(current_value(&revoke_admin_token.key)?);
    let mut revoke_admin_audit = identity_audit_event(0x95, user_manager, admin_user);
    revoke_admin_audit.action = AuditAction::TokenLifecycle;
    revoke_admin_audit.target_ids = vec![admin_user, id(0x8d)];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x95),
            user_manager,
            9_999,
            &[revoke_admin_token],
            &[revoke_admin_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert!(!storage.list_tokens(user_manager, admin_user)?[0].revoked);

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_micros(),
    )?;
    let mut create_no_login_audit = identity_audit_event(0xac, user_manager, user_manager);
    create_no_login_audit.correlation_id = id(0xac);
    let no_login_user = storage.create_user_no_login(
        id(0xac),
        user_manager,
        "service_loader",
        now + 3_600_000_000,
        create_no_login_audit,
    )?;
    let mut no_login_token_audit = identity_audit_event(0xad, user_manager, no_login_user.user_id);
    no_login_token_audit.action = AuditAction::TokenLifecycle;
    no_login_token_audit.target_ids = vec![no_login_user.user_id];
    let no_login_token = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0xad),
        authenticated_actor_id: user_manager,
        user_id: no_login_user.user_id,
        label: "blocked no-login token",
        expires_at_us: now + 3_600_000_000,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: no_login_token_audit,
    });
    assert!(no_login_token.is_err(), "NOLOGIN identity received a token");
    let replacement_event = AuditEvent {
        event_id: id(0x96),
        actor_id: Some(user_manager),
        correlation_id: id(0x97),
        occurred_at_us: now,
        action: AuditAction::TokenLifecycle,
        result: AuditResult::Succeeded,
        target_ids: vec![admin_user],
    };
    let _replacement = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0x96),
        authenticated_actor_id: user_manager,
        user_id: admin_user,
        label: "replacement admin token",
        expires_at_us: now + 3_600_000_000,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: replacement_event,
    })?;
    let revoke_old_event = AuditEvent {
        event_id: id(0x98),
        actor_id: Some(user_manager),
        correlation_id: id(0x99),
        occurred_at_us: now,
        action: AuditAction::TokenLifecycle,
        result: AuditResult::Succeeded,
        target_ids: vec![id(0x8d)],
    };
    storage.revoke_token(
        id(0x98),
        user_manager,
        id(0x8d),
        now + 7_200_000_000,
        revoke_old_event,
    )?;

    let user_token_secret = [0x47; 32];
    let active_user_token = credential_verifier_write(
        id(0x8b),
        created_user,
        &user_token_secret,
        1,
        i64::MAX,
        None,
        1,
    );
    let mut token_audit = identity_audit_event(0x8b, user_manager, created_user);
    token_audit.target_ids = vec![created_user, id(0x8b)];
    storage.commit_catalog_and_audit(
        id(0x8b),
        user_manager,
        9_999,
        &[active_user_token],
        &[token_audit],
    )?;

    let mut rename_audit = identity_audit_event(0x83, user_manager, created_user);
    rename_audit.correlation_id = id(0x83);
    assert_eq!(
        storage.rename_identity(
            id(0x83),
            user_manager,
            created_user,
            "renamed_user",
            9_999,
            rename_audit,
        )?,
        7
    );
    let (mut renamed_user_invocation, renamed_user_roles) = storage
        .begin_token_authenticated_invocation(
            id(0x8e),
            id(0x8b),
            &user_token_secret,
            now + 3_600_000_000,
            2,
            1024,
        )?;
    assert!(renamed_user_roles.is_empty());
    renamed_user_invocation.abort()?;

    let mut disable_user = principal_write_with_login(created_user, 1, false, 3, "renamed_user");
    disable_user.expected_value = Some(current_value(&disable_user.key)?);
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x84),
            user_manager,
            9_999,
            std::slice::from_ref(&disable_user),
            &[identity_audit_event(0x84, user_manager, created_user)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let mut disable_audit = identity_audit_event(0x8c, user_manager, created_user);
    disable_audit.correlation_id = id(0x8c);
    assert_eq!(
        storage.set_user_login_enabled(
            id(0x8c),
            user_manager,
            created_user,
            false,
            9_999,
            disable_audit,
        )?,
        8
    );
    assert!(storage.list_tokens(user_manager, created_user)?[0].revoked);
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0x8f),
            id(0x8b),
            &user_token_secret,
            now + 3_600_000_000,
            2,
            1024,
        ),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));

    let mut disable_last_admin =
        principal_write_with_login(admin_user, 1, false, 2, "identity_admin");
    disable_last_admin.expected_value = Some(current_value(&disable_last_admin.key)?);
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x89),
            user_manager,
            9_999,
            &[disable_last_admin],
            &[identity_audit_event(0x89, user_manager, admin_user)],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let admin_role_create = [
        principal_write_with_name(created_role, 2, 1, "created_role"),
        object_write_with_parent_and_owner_and_name(
            created_role,
            9,
            None,
            admin_user,
            "created_role",
        ),
    ];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x8a),
            admin_user,
            9_999,
            &admin_role_create,
            &[identity_audit_event(0x8a, admin_user, created_role)],
        )?,
        9
    );
    let mut rename_role_audit = identity_audit_event(0x9a, admin_user, created_role);
    rename_role_audit.correlation_id = id(0x9a);
    assert_eq!(
        storage.rename_identity(
            id(0x9a),
            admin_user,
            created_role,
            "renamed_created_role",
            9_999,
            rename_role_audit,
        )?,
        10
    );
    assert_eq!(commit_sequence(&database)?, 10);
    Ok(())
}

pub(super) fn login_reenable_racing_token_issue_only_publishes_after_enable()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        IdentityLifecycleError, TokenIssueRequest, TokenLifecycleError,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_user = id(0xd0);
    let admin_role = builtin_owner_id();
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let retention_deadline_us = now.saturating_add(7_200_000_000);
    let mut setup_audit = identity_audit_event(0xd0, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xd0),
        admin_role,
        retention_deadline_us,
        &[
            principal_write_with_name(admin_user, 1, 1, "reenable_race_admin"),
            object_write_with_name(admin_user, 8, "reenable_race_admin"),
            membership_write_with_grantor(admin_user, admin_role, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(
                id(0xd1),
                admin_user,
                &[0xd1; 32],
                now - 1_000_000,
                i64::MAX,
                None,
                1,
            ),
        ],
        &[setup_audit],
    )?;

    let mut create_audit = identity_audit_event(0xd2, admin_user, admin_user);
    create_audit.correlation_id = id(0xd2);
    let target = storage.create_user_no_login(
        id(0xd2),
        admin_user,
        "reenable_race_target",
        retention_deadline_us,
        create_audit,
    )?;
    let enable_invocation = id(0xd3);
    let issue_invocation = id(0xd4);
    let mut enable_audit = identity_audit_event(0xd3, admin_user, target.user_id);
    enable_audit.correlation_id = enable_invocation;
    let mut issue_audit = identity_audit_event(0xd4, admin_user, target.user_id);
    issue_audit.action = AuditAction::TokenLifecycle;
    issue_audit.correlation_id = issue_invocation;
    let expires_at_us = now.saturating_add(3_600_000_000);
    let barrier = Arc::new(Barrier::new(3));
    let (enable_result, issue_result) = std::thread::scope(|scope| {
        let enable_barrier = Arc::clone(&barrier);
        let enable_storage = &storage;
        let enable = scope.spawn(move || {
            enable_barrier.wait();
            enable_storage.set_user_login_enabled(
                enable_invocation,
                admin_user,
                target.user_id,
                true,
                retention_deadline_us,
                enable_audit,
            )
        });
        let issue_barrier = Arc::clone(&barrier);
        let issue_storage = &storage;
        let issue = scope.spawn(move || {
            issue_barrier.wait();
            issue_storage.issue_token(TokenIssueRequest {
                invocation_id: issue_invocation,
                authenticated_actor_id: admin_user,
                user_id: target.user_id,
                label: "after LOGIN enable",
                expires_at_us,
                retention_deadline_us,
                audit_event: issue_audit,
            })
        });
        barrier.wait();
        (
            enable.join().expect("LOGIN-enable thread panicked"),
            issue.join().expect("token-issue thread panicked"),
        )
    });
    let (enable_sequence, enable_committed) = match enable_result {
        Ok(sequence) => (sequence, true),
        Err(IdentityLifecycleError::Commit(CommitError::Conflict)) => {
            let mut retry_audit = identity_audit_event(0xd5, admin_user, target.user_id);
            retry_audit.correlation_id = id(0xd5);
            let sequence = storage.set_user_login_enabled(
                id(0xd5),
                admin_user,
                target.user_id,
                true,
                retention_deadline_us,
                retry_audit,
            )?;
            (sequence, false)
        }
        Err(error) => return Err(Box::new(error)),
    };
    let issued = match issue_result {
        Ok(token) => Some((token.token_id, token.commit_sequence)),
        Err(TokenLifecycleError::Commit(
            CommitError::DdlAuthorizationDenied
            | CommitError::Conflict
            | CommitError::InvalidCatalog,
        ))
        | Err(TokenLifecycleError::TokenUnavailable) => None,
        Err(error) => return Err(Box::new(error)),
    };
    if let Some((_, issue_sequence)) = issued {
        assert!(
            enable_sequence < issue_sequence,
            "token became durable before LOGIN was enabled"
        );
    }
    assert_eq!(
        lookup_outcome(&database, enable_invocation, admin_user, now)?
            .ok_or("missing LOGIN enable outcome")?
            .status,
        if enable_committed {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        },
    );
    if let Some((token_id, _)) = issued {
        assert_eq!(
            lookup_outcome(&database, issue_invocation, admin_user, now)?
                .ok_or("missing token issue outcome")?
                .status,
            OutcomeStatus::Committed,
        );
        assert!(
            storage
                .list_tokens(admin_user, target.user_id)?
                .iter()
                .any(|token| token.token_id == token_id && !token.revoked)
        );
    } else {
        assert_eq!(
            lookup_outcome(&database, issue_invocation, admin_user, now)?
                .ok_or("missing rejected token issue outcome")?
                .status,
            OutcomeStatus::Aborted,
        );
        assert!(storage.list_tokens(admin_user, target.user_id)?.is_empty());
    }
    assert!(prdb_engine::startup::validate_persisted_records(&database).is_ok());
    Ok(())
}

pub(super) fn identity_login_reenable_does_not_revive_revoked_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_user = id(0xc0);
    let admin_role = builtin_owner_id();
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_micros(),
    )?;
    let retention_deadline_us = now.saturating_add(7_200_000_000);
    let mut setup_audit = identity_audit_event(0xc0, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xc0),
        admin_role,
        retention_deadline_us,
        &[
            principal_write_with_name(admin_user, 1, 1, "login_cycle_admin"),
            object_write_with_name(admin_user, 8, "login_cycle_admin"),
            membership_write_with_grantor(admin_user, admin_role, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(
                id(0xc8),
                admin_user,
                &[0xc8; 32],
                now - 1_000_000,
                i64::MAX,
                None,
                1,
            ),
        ],
        &[setup_audit],
    )?;

    let mut create_audit = identity_audit_event(0xc1, admin_user, admin_user);
    create_audit.correlation_id = id(0xc1);
    let user = storage
        .create_user(
            id(0xc1),
            admin_user,
            "login_cycle_target",
            retention_deadline_us,
            create_audit,
        )
        .map_err(|error| format!("create target user for LOGIN cycle: {error:?}"))?;
    let expires_at_us = now.saturating_add(3_600_000_000);
    let mut first_issue_audit = identity_audit_event(0xc2, admin_user, user.user_id);
    first_issue_audit.action = AuditAction::TokenLifecycle;
    first_issue_audit.correlation_id = id(0xc2);
    let mut first_token = storage
        .issue_token(TokenIssueRequest {
            invocation_id: id(0xc2),
            authenticated_actor_id: admin_user,
            user_id: user.user_id,
            label: "before NOLOGIN",
            expires_at_us,
            retention_deadline_us,
            audit_event: first_issue_audit,
        })
        .map_err(|error| format!("issue initial token for LOGIN cycle: {error:?}"))?;
    let first_token_id = first_token.token_id;
    let first_secret = first_token
        .take_secret()
        .ok_or("initial token secret missing")?;

    let mut disable_audit = identity_audit_event(0xc3, admin_user, user.user_id);
    disable_audit.correlation_id = id(0xc3);
    let disable_sequence = storage
        .set_user_login_enabled(
            id(0xc3),
            admin_user,
            user.user_id,
            false,
            retention_deadline_us,
            disable_audit,
        )
        .map_err(|error| format!("disable target LOGIN: {error:?}"))?;
    let disabled_inventory = storage.list_tokens(admin_user, user.user_id)?;
    assert!(
        disabled_inventory
            .iter()
            .any(|token| token.token_id == first_token_id && token.revoked)
    );

    let mut enable_audit = identity_audit_event(0xc4, admin_user, user.user_id);
    enable_audit.correlation_id = id(0xc4);
    let enable_sequence = storage
        .set_user_login_enabled(
            id(0xc4),
            admin_user,
            user.user_id,
            true,
            retention_deadline_us,
            enable_audit,
        )
        .map_err(|error| format!("re-enable target LOGIN: {error:?}"))?;
    assert!(disable_sequence < enable_sequence);
    let enabled_inventory = storage.list_tokens(admin_user, user.user_id)?;
    assert!(
        enabled_inventory
            .iter()
            .any(|token| token.token_id == first_token_id && token.revoked)
    );
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0xc5),
            first_token_id,
            &first_secret,
            retention_deadline_us,
            1,
            64,
        ),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));
    assert_eq!(
        lookup_outcome(&database, id(0xc5), user.user_id, now)?,
        None
    );

    let mut replacement_audit = identity_audit_event(0xc6, admin_user, user.user_id);
    replacement_audit.action = AuditAction::TokenLifecycle;
    replacement_audit.correlation_id = id(0xc6);
    let mut replacement = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0xc6),
        authenticated_actor_id: admin_user,
        user_id: user.user_id,
        label: "after LOGIN",
        expires_at_us,
        retention_deadline_us,
        audit_event: replacement_audit,
    })?;
    assert!(enable_sequence < replacement.commit_sequence);
    let replacement_id = replacement.token_id;
    let replacement_secret = replacement
        .take_secret()
        .ok_or("replacement token secret missing")?;
    let (mut admitted, roles) = storage.begin_token_authenticated_invocation(
        id(0xc7),
        replacement_id,
        &replacement_secret,
        retention_deadline_us,
        1,
        64,
    )?;
    assert!(roles.is_empty());
    admitted.abort()?;
    let inventory = storage.list_tokens(admin_user, user.user_id)?;
    assert!(
        inventory
            .iter()
            .any(|token| token.token_id == first_token_id && token.revoked)
    );
    assert!(
        inventory
            .iter()
            .any(|token| token.token_id == replacement_id && !token.revoked)
    );
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn concurrent_user_and_role_creation_share_one_name_namespace()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let manager = id(0xd1);
    let administrator = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let records = [
        principal_write_with_name(manager, 1, 1, "shared_name_manager"),
        object_write_with_name(manager, 8, "shared_name_manager"),
        membership_write_with_grantor(manager, administrator, administrator),
        dependency_write(manager, administrator, 5),
    ];
    let mut setup_audit = identity_audit_event(0xd0, administrator, manager);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xd0), administrator, 9_999, &records, &[setup_audit])?;

    let user_invocation = id(0xd2);
    let role_invocation = id(0xd3);
    let create_barrier = Arc::new(Barrier::new(3));
    let (user_result, role_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&create_barrier);
        let storage_ref = &storage;
        let user = scope.spawn(move || {
            let mut audit = identity_audit_event(0xd4, manager, manager);
            audit.correlation_id = user_invocation;
            barrier.wait();
            storage_ref.create_user(user_invocation, manager, "shared_name", 9_999, audit)
        });
        let barrier = Arc::clone(&create_barrier);
        let storage_ref = &storage;
        let role = scope.spawn(move || {
            let mut audit = identity_audit_event(0xd5, manager, manager);
            audit.correlation_id = role_invocation;
            barrier.wait();
            storage_ref.create_role(role_invocation, manager, "shared_name", 9_999, audit)
        });
        create_barrier.wait();
        (
            user.join().expect("user creation thread panicked"),
            role.join().expect("role creation thread panicked"),
        )
    });

    assert!(
        user_result.is_ok() || role_result.is_ok(),
        "at least one authorized identity create must succeed: user={user_result:?}, role={role_result:?}"
    );
    assert_ne!(user_result.is_ok(), role_result.is_ok());
    if user_result.is_err() {
        assert!(matches!(
            &user_result,
            Err(
                prdb_engine::storage_manager::IdentityLifecycleError::Commit(
                    CommitError::InvalidCatalog | CommitError::Conflict
                )
            )
        ));
    }
    if role_result.is_err() {
        assert!(matches!(
            &role_result,
            Err(
                prdb_engine::storage_manager::IdentityLifecycleError::Commit(
                    CommitError::InvalidCatalog | CommitError::Conflict
                )
            )
        ));
    }
    let user_outcome = lookup_outcome(&database, user_invocation, manager, 1)?
        .ok_or("missing USER creation outcome")?;
    let role_outcome = lookup_outcome(&database, role_invocation, manager, 1)?
        .ok_or("missing ROLE creation outcome")?;
    assert_eq!(
        user_outcome.status,
        if user_result.is_ok() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        role_outcome.status,
        if role_result.is_ok() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );

    let snapshot = database.begin_read()?;
    let shared_name_id = lookup_principal_id_by_name(&snapshot, "shared_name")?
        .ok_or("winning shared namespace name is missing")?;
    let expected_identity_id = match (&user_result, &role_result) {
        (Ok(created), Err(_)) => created.user_id,
        (Err(_), Ok(created)) => created.role_id,
        _ => unreachable!("exactly one shared-name identity creation must commit"),
    };
    assert_eq!(shared_name_id, expected_identity_id);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn identity_rename_preserves_the_shared_user_role_name_namespace()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin = id(0xd7);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut setup_audit = identity_audit_event(0xd7, admin_role, admin);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xd7),
        admin_role,
        9_999,
        &[
            principal_write_with_name(admin, 1, 1, "rename_namespace_admin"),
            object_write_with_name(admin, 8, "rename_namespace_admin"),
            membership_write_with_grantor(admin, admin_role, admin_role),
            dependency_write(admin, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let mut create_user_audit = identity_audit_event(0xd8, admin, admin);
    create_user_audit.correlation_id = id(0xd8);
    let user_id = storage
        .create_user(
            id(0xd8),
            admin,
            "rename_namespace_user",
            9_999,
            create_user_audit,
        )?
        .user_id;
    let mut create_role_audit = identity_audit_event(0xd9, admin, admin);
    create_role_audit.correlation_id = id(0xd9);
    let role_id = storage
        .create_role(
            id(0xd9),
            admin,
            "rename_namespace_role",
            9_999,
            create_role_audit,
        )?
        .role_id;

    let mut rename_audit = identity_audit_event(0xda, admin, user_id);
    rename_audit.correlation_id = id(0xda);
    assert!(matches!(
        storage.rename_identity(
            id(0xda),
            admin,
            user_id,
            "rename_namespace_role",
            9_999,
            rename_audit,
        ),
        Err(
            prdb_engine::storage_manager::IdentityLifecycleError::Commit(
                CommitError::InvalidCatalog
            )
        )
    ));

    let read = database.begin_read()?;
    assert_eq!(
        lookup_principal_id_by_name(&read, "rename_namespace_user")?,
        Some(user_id)
    );
    assert_eq!(
        lookup_principal_id_by_name(&read, "rename_namespace_role")?,
        Some(role_id)
    );
    drop(read);
    assert_eq!(commit_sequence(&database)?, 3);
    let outcome = lookup_outcome(&database, id(0xda), admin, 1)?
        .ok_or("missing rejected cross-kind rename outcome")?;
    assert_eq!(outcome.status, OutcomeStatus::Aborted);
    Ok(())
}

pub(super) fn login_disable_racing_token_admission_linearizes_before_or_after_start()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TokenIssueRequest;
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let admin_user = id(0xA1);
    let user_id = id(0xA2);
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_micros(),
    )?;
    let retention_deadline_us = now.saturating_add(3_600_000_000);
    let mut setup_audit = identity_audit_event(0xA4, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xA4),
        admin_role,
        retention_deadline_us,
        &[
            principal_write_with_name(admin_user, 1, 1, "login_admission_admin"),
            object_write_with_name(admin_user, 8, "login_admission_admin"),
            membership_write(admin_user, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(
                id(0xA5),
                admin_user,
                &[0xA5; 32],
                now - 1,
                retention_deadline_us,
                None,
                1,
            ),
            principal_write_with_name(user_id, 1, 1, "login_admission_target"),
            object_write_with_name(user_id, 8, "login_admission_target"),
        ],
        &[setup_audit],
    )?;
    let mut token_audit = identity_audit_event(0xA6, admin_user, user_id);
    token_audit.action = AuditAction::TokenLifecycle;
    token_audit.correlation_id = id(0xA6);
    let mut issued = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0xA6),
        authenticated_actor_id: admin_user,
        user_id,
        label: "login admission race",
        expires_at_us: retention_deadline_us,
        retention_deadline_us,
        audit_event: token_audit,
    })?;
    let token_id = issued.token_id;
    let token_secret = issued.take_secret().ok_or("issued token secret missing")?;

    let retained_invocation_id = id(0xA7);
    let admission_id = id(0xAB);
    let disable_id = id(0xA8);
    let (mut retained_invocation, retained_roles) = storage.begin_token_authenticated_invocation(
        retained_invocation_id,
        token_id,
        &token_secret,
        retention_deadline_us,
        1,
        64,
    )?;
    assert!(retained_roles.is_empty());
    let pinned_schema = retained_invocation.load_catalog_table_schema(id(16))?;
    let barrier = Arc::new(Barrier::new(2));
    let (admission_result, disable_result) = std::thread::scope(|scope| {
        let admission_barrier = Arc::clone(&barrier);
        let admission_storage = &storage;
        let admission_secret = token_secret;
        let admission = scope.spawn(move || {
            admission_barrier.wait();
            admission_storage.begin_token_authenticated_invocation(
                admission_id,
                token_id,
                &admission_secret,
                retention_deadline_us,
                1,
                64,
            )
        });
        barrier.wait();
        let mut disable_audit = identity_audit_event(0xA9, admin_user, user_id);
        disable_audit.correlation_id = disable_id;
        let disable = storage.set_user_login_enabled(
            disable_id,
            admin_user,
            user_id,
            false,
            retention_deadline_us,
            disable_audit,
        );
        (
            admission.join().expect("token-admission thread panicked"),
            disable,
        )
    });

    let disable_sequence = disable_result?;
    assert!(disable_sequence > 1);
    match admission_result {
        Ok((mut invocation, roles)) => {
            assert!(roles.is_empty());
            invocation.abort()?;
            assert_eq!(
                lookup_outcome(&database, admission_id, user_id, 1)?
                    .ok_or("admitted invocation outcome missing")?
                    .status,
                OutcomeStatus::Aborted
            );
        }
        Err(TransactionError::AuthenticationFailed) => {
            // The LOGIN-disable commit linearized first, so no invocation is admitted.
        }
        Err(error) => {
            return Err(format!("unexpected LOGIN/admission race result: {error:?}").into());
        }
    }
    assert!(
        storage
            .list_tokens(admin_user, user_id)?
            .iter()
            .any(|token| token.token_id == token_id && token.revoked)
    );
    assert_eq!(
        retained_invocation.load_catalog_table_schema(id(16))?,
        pinned_schema,
        "an invocation admitted before LOGIN disable retains its bounded catalog snapshot"
    );
    retained_invocation.abort()?;
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0xAC),
            token_id,
            &token_secret,
            retention_deadline_us,
            1,
            64,
        ),
        Err(TransactionError::AuthenticationFailed)
    ));
    Ok(())
}

pub(super) fn identity_ddl_cannot_disable_the_last_credentialed_admin()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_to_disable = id(0xb1);
    let expired_admin = id(0xb2);
    let user_manager = id(0xb3);
    let active_token_id = id(0xb4);
    let expired_token_id = id(0xb5);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let active_secret = [0xb4; 32];
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let records = [
        principal_write_with_name(admin_to_disable, 1, 1, "admin_to_disable"),
        object_write_with_name(admin_to_disable, 8, "admin_to_disable"),
        principal_write_with_name(expired_admin, 1, 1, "expired_admin"),
        object_write_with_name(expired_admin, 8, "expired_admin"),
        principal_write_with_name(user_manager, 1, 1, "credential_manager"),
        object_write_with_name(user_manager, 8, "credential_manager"),
        membership_write_with_grantor(admin_to_disable, admin_role, admin_role),
        membership_write_with_grantor(expired_admin, admin_role, admin_role),
        dependency_write(admin_to_disable, admin_role, 5),
        dependency_write(expired_admin, admin_role, 5),
        credential_verifier_write(
            active_token_id,
            admin_to_disable,
            &active_secret,
            1,
            i64::MAX,
            None,
            1,
        ),
        credential_verifier_write(expired_token_id, expired_admin, &[0xb5; 32], 1, 2, None, 1),
        grant_write(user_manager, database_id, 12, 1, database_id, admin_role),
        dependency_write(user_manager, database_id, 5),
    ];
    let mut setup_audit = identity_audit_event(0xb0, admin_role, admin_to_disable);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb0), admin_role, 9_999, &records, &[setup_audit])?;

    let mut disable = principal_write_with_login(admin_to_disable, 1, false, 2, "admin_to_disable");
    let mut revoke = credential_verifier_write(
        active_token_id,
        admin_to_disable,
        &active_secret,
        1,
        i64::MAX,
        Some(2),
        2,
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        disable.expected_value = Some(
            catalog
                .get(disable.key.as_slice())?
                .ok_or("admin principal missing")?
                .value()
                .to_vec(),
        );
        revoke.expected_value = Some(
            catalog
                .get(revoke.key.as_slice())?
                .ok_or("admin token missing")?
                .value()
                .to_vec(),
        );
    }
    let mut audit = identity_audit_event(0xb6, user_manager, admin_to_disable);
    audit.target_ids = vec![admin_to_disable, active_token_id];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0xb6),
            user_manager,
            9_999,
            &[disable, revoke],
            &[audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(0xb6), user_manager, 1)?
            .ok_or("missing aborted outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let token_now_us = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_micros(),
    )?;
    let token_expires_at_us = token_now_us.saturating_add(3_600_000_000);
    let mut issue_audit = identity_audit_event(0xb7, user_manager, expired_admin);
    issue_audit.correlation_id = id(0xb7);
    issue_audit.action = AuditAction::TokenLifecycle;
    storage.issue_token(TokenIssueRequest {
        invocation_id: id(0xb7),
        authenticated_actor_id: user_manager,
        user_id: expired_admin,
        label: "second admin credential",
        expires_at_us: token_expires_at_us,
        retention_deadline_us: token_expires_at_us.saturating_add(3_600_000_000),
        audit_event: issue_audit,
    })?;

    let first_disable_invocation = id(0xb8);
    let mut first_disable_audit = identity_audit_event(0xb9, admin_to_disable, expired_admin);
    first_disable_audit.correlation_id = first_disable_invocation;
    let second_disable_invocation = id(0xbb);
    let mut second_disable_audit = identity_audit_event(0xbc, expired_admin, admin_to_disable);
    second_disable_audit.correlation_id = second_disable_invocation;
    let disable_barrier = Arc::new(Barrier::new(3));
    let storage_ref = &storage;
    let (first_disable, second_disable) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&disable_barrier);
        let first = scope.spawn(move || {
            barrier.wait();
            storage_ref.set_user_login_enabled(
                first_disable_invocation,
                admin_to_disable,
                expired_admin,
                false,
                9_999,
                first_disable_audit,
            )
        });
        let barrier = Arc::clone(&disable_barrier);
        let second = scope.spawn(move || {
            barrier.wait();
            storage_ref.set_user_login_enabled(
                second_disable_invocation,
                expired_admin,
                admin_to_disable,
                false,
                9_999,
                second_disable_audit,
            )
        });
        disable_barrier.wait();
        (
            first.join().expect("first admin disable thread panicked"),
            second.join().expect("second admin disable thread panicked"),
        )
    });
    let (surviving_admin, disabled_admin, winning_invocation, losing_invocation) = match (
        first_disable,
        second_disable,
    ) {
        (
            Ok(_),
            Err(
                prdb_engine::storage_manager::IdentityLifecycleError::Unauthorized
                | prdb_engine::storage_manager::IdentityLifecycleError::Commit(
                    CommitError::Conflict | CommitError::DdlAuthorizationDenied,
                ),
            ),
        ) => (
            admin_to_disable,
            expired_admin,
            first_disable_invocation,
            second_disable_invocation,
        ),
        (
            Err(
                prdb_engine::storage_manager::IdentityLifecycleError::Unauthorized
                | prdb_engine::storage_manager::IdentityLifecycleError::Commit(
                    CommitError::Conflict | CommitError::DdlAuthorizationDenied,
                ),
            ),
            Ok(_),
        ) => (
            expired_admin,
            admin_to_disable,
            second_disable_invocation,
            first_disable_invocation,
        ),
        (first, second) => panic!(
            "concurrent LOGIN disables must have exactly one winner: first={first:?}, second={second:?}"
        ),
    };
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let login_enabled = |user_id| -> Result<bool, Box<dyn std::error::Error>> {
        let encoded = catalog
            .get(id_catalog_key(10, user_id).as_slice())?
            .ok_or("admin principal missing after concurrent disable")?
            .value()
            .to_vec();
        let principal = decode_registered_envelope(&encoded)?;
        let enabled = principal
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("admin LOGIN field missing")?;
        match enabled.payload.as_slice() {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err("invalid admin LOGIN field".into()),
        }
    };
    assert!(!login_enabled(disabled_admin)?);
    assert!(login_enabled(surviving_admin)?);
    drop(catalog);
    drop(read);
    let disabled_tokens = storage.list_tokens(surviving_admin, disabled_admin)?;
    assert!(!disabled_tokens.is_empty());
    assert!(disabled_tokens.iter().all(|token| token.revoked));
    let surviving_tokens = storage.list_tokens(surviving_admin, surviving_admin)?;
    assert!(surviving_tokens.iter().any(|token| !token.revoked));
    assert_eq!(
        lookup_outcome(&database, winning_invocation, surviving_admin, 1)?
            .ok_or("winning admin disable outcome missing")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, losing_invocation, disabled_admin, 1)?
            .ok_or("losing admin disable outcome missing")?
            .status,
        OutcomeStatus::Aborted
    );
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}
