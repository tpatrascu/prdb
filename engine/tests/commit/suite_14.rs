use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::commit::{CatalogWrite, CommitError, RowWrite, commit_rows};
use prdb_engine::metadata::EngineMetadata;
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::schema::{self, CATALOG, ENGINE_META};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{
    ColumnSchema, FieldPatch, ScalarType, ScalarValue, decode_tuple, encode_tuple,
};
use redb::{Database, ReadableDatabase};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn system_privilege_dependency_survives_until_the_last_grant_is_revoked()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AdministrativeSystemPrivilege as Privilege, SystemPrivilegeChange,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let administrator = id(0xc1);
    let grantee = id(0xc2);
    let admin_role = builtin_owner_id();
    let setup = [
        principal_write_with_name(administrator, 1, 1, "privilege_admin"),
        object_write_with_name(administrator, 8, "privilege_admin"),
        membership_write_with_grantor(administrator, admin_role, admin_role),
        dependency_write(administrator, admin_role, 5),
        principal_write_with_name(grantee, 1, 1, "privilege_grantee"),
        object_write_with_name(grantee, 8, "privilege_grantee"),
    ];
    let mut setup_audit = identity_audit_event(0xc3, admin_role, administrator);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xc3), admin_role, 9_999, &setup, &[setup_audit])?;

    let read = database.begin_read()?;
    let metadata = read
        .open_table(ENGINE_META)?
        .get(&b"format"[..])?
        .ok_or("missing engine metadata")?
        .value()
        .to_vec();
    let database_id = EngineMetadata::decode(&metadata)?.database_id;
    drop(read);

    let change = |invocation_id: [u8; 16], privilege: Privilege, grant: bool| {
        let mut audit = identity_audit_event(invocation_id[0], administrator, grantee);
        audit.correlation_id = invocation_id;
        audit.action = AuditAction::AuthorityChange;
        storage.set_system_privilege(
            invocation_id,
            administrator,
            SystemPrivilegeChange {
                grantee_id: grantee,
                privilege,
                grant,
            },
            9_999,
            audit,
        )
    };
    let dependency_key = dependency_write(grantee, database_id, 5).key;
    let create_schema_key =
        grant_write(grantee, database_id, 14, 1, database_id, administrator).key;
    let manage_users_key = grant_write(grantee, database_id, 12, 1, database_id, administrator).key;

    let grant_barrier = Arc::new(Barrier::new(2));
    let (create_schema_result, manage_users_result) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&grant_barrier);
        let create_schema = scope.spawn(move || {
            barrier.wait();
            change(id(0xc4), Privilege::CreateSchema, true)
        });
        let barrier = Arc::clone(&grant_barrier);
        let manage_users = scope.spawn(move || {
            barrier.wait();
            change(id(0xc5), Privilege::ManageUsers, true)
        });
        (
            create_schema
                .join()
                .expect("CREATE_SCHEMA grant thread panicked"),
            manage_users
                .join()
                .expect("MANAGE_USERS grant thread panicked"),
        )
    });
    for (result, privilege, retry_invocation) in [
        (create_schema_result, Privilege::CreateSchema, id(0xc8)),
        (manage_users_result, Privilege::ManageUsers, id(0xc9)),
    ] {
        match result {
            Ok(_) => {}
            Err(prdb_engine::storage_manager::SystemPrivilegeLifecycleError::Commit(
                CommitError::Conflict,
            )) => {
                change(retry_invocation, privilege, true)?;
            }
            Err(error) => {
                return Err(format!("concurrent privilege grant failed: {error:?}").into());
            }
        }
    }
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(create_schema_key.as_slice())?.is_some());
    assert!(catalog.get(manage_users_key.as_slice())?.is_some());
    assert!(catalog.get(dependency_key.as_slice())?.is_some());
    drop(catalog);
    drop(read);

    change(id(0xc6), Privilege::CreateSchema, false)?;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(create_schema_key.as_slice())?.is_none());
    assert!(catalog.get(manage_users_key.as_slice())?.is_some());
    assert!(catalog.get(dependency_key.as_slice())?.is_some());
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;

    change(id(0xc7), Privilege::ManageUsers, false)?;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(manage_users_key.as_slice())?.is_none());
    assert!(catalog.get(dependency_key.as_slice())?.is_none());
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_authorizes_membership_ddl_by_system_privilege_and_admin_role()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let role_manager = id(0x61);
    let denied_user = id(0x62);
    let admin_user = id(0x63);
    let first_member = id(0x64);
    let second_member = id(0x65);
    let app_role = id(0x67);
    let admin_parent_role = id(0x68);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(role_manager, 1, 1, "role_manager"),
        object_write_with_name(role_manager, 8, "role_manager"),
        principal_write_with_name(denied_user, 1, 1, "role_denied"),
        object_write_with_name(denied_user, 8, "role_denied"),
        principal_write_with_name(admin_user, 1, 1, "role_admin"),
        object_write_with_name(admin_user, 8, "role_admin"),
        principal_write_with_name(first_member, 1, 1, "first_member"),
        object_write_with_name(first_member, 8, "first_member"),
        principal_write_with_name(second_member, 1, 1, "second_member"),
        object_write_with_name(second_member, 8, "second_member"),
        principal_write_with_name(app_role, 2, 1, "app_role"),
        object_write_with_name(app_role, 9, "app_role"),
        principal_write_with_name(admin_parent_role, 2, 1, "admin_parent_role"),
        object_write_with_name(admin_parent_role, 9, "admin_parent_role"),
        grant_write(role_manager, database_id, 13, 1, database_id, admin_role),
        dependency_write(role_manager, database_id, 5),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        credential_verifier_write(id(0x6a), admin_user, &[0x6a; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(0x6b), second_member, &[0x6b; 32], 1, i64::MAX, None, 1),
        membership_write_with_grantor(admin_parent_role, admin_role, admin_role),
        dependency_write(admin_parent_role, admin_role, 5),
    ];
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let preflight = database.begin_read()?;
    let preflight_catalog = preflight.open_table(CATALOG)?;
    for record in &records {
        assert!(record.expected_value.is_none());
        assert!(
            preflight_catalog.get(record.key.as_slice())?.is_none(),
            "fixture catalog key already exists: {:?}",
            record.key
        );
    }
    drop(preflight_catalog);
    drop(preflight);
    let mut setup_audit = audit_event(79);
    setup_audit.actor_id = Some(admin_role);
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(id(79), admin_role, 9_999, &records, &[setup_audit])
        .map_err(|error| format!("membership authorization fixture setup: {error:?}"))?;
    let bump_member_epoch =
        |member_id, epoch, name: &str| -> Result<_, Box<dyn std::error::Error>> {
            let mut write = principal_write_with_name(member_id, 1, epoch, name);
            let read = database.begin_read()?;
            write.expected_value = Some(
                read.open_table(CATALOG)?
                    .get(id_catalog_key(10, member_id).as_slice())?
                    .ok_or("missing member principal")?
                    .value()
                    .to_vec(),
            );
            Ok(write)
        };
    let remove_catalog_record = |mut write: CatalogWrite| -> Result<_, Box<dyn std::error::Error>> {
        let read = database.begin_read()?;
        write.expected_value = Some(
            read.open_table(CATALOG)?
                .get(write.key.as_slice())?
                .ok_or("missing catalog record to remove")?
                .value()
                .to_vec(),
        );
        write.payload = None;
        Ok(write)
    };
    let mut allowed_audit = audit_event(80);
    allowed_audit.actor_id = Some(role_manager);
    allowed_audit.action = AuditAction::AuthorityChange;
    let manager_membership = membership_write_with_grantor(first_member, app_role, role_manager);
    assert_eq!(
        storage
            .commit_authorized_membership_ddl(
                id(80),
                role_manager,
                9_999,
                &[
                    manager_membership.clone(),
                    dependency_write(first_member, app_role, 5),
                    bump_member_epoch(first_member, 2, "first_member")?,
                ],
                &[allowed_audit],
            )
            .map_err(|error| format!("MANAGE_ROLES membership change: {error:?}"))?,
        2
    );

    let mut removal_audit = audit_event(84);
    removal_audit.actor_id = Some(role_manager);
    removal_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(84),
            role_manager,
            9_999,
            &[
                remove_catalog_record(manager_membership.clone())?,
                remove_catalog_record(dependency_write(first_member, app_role, 5))?,
                bump_member_epoch(first_member, 3, "first_member")?,
            ],
            &[removal_audit],
        )?,
        3
    );

    let denied_membership = membership_write_with_grantor(second_member, app_role, denied_user);
    let mut denied_audit = audit_event(81);
    denied_audit.actor_id = Some(denied_user);
    denied_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(81),
            denied_user,
            9_999,
            &[
                denied_membership.clone(),
                dependency_write(second_member, app_role, 5),
                bump_member_epoch(second_member, 2, "second_member")?,
            ],
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let inherited_admin_membership =
        membership_write_with_grantor(second_member, admin_parent_role, role_manager);
    let mut inherited_admin_audit = audit_event(87);
    inherited_admin_audit.actor_id = Some(role_manager);
    inherited_admin_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(87),
            role_manager,
            9_999,
            &[
                inherited_admin_membership.clone(),
                dependency_write(second_member, admin_parent_role, 5),
                bump_member_epoch(second_member, 2, "second_member")?,
            ],
            &[inherited_admin_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let admin_role_membership =
        membership_write_with_grantor(second_member, admin_role, role_manager);
    let admin_role_membership_by_admin =
        membership_write_with_grantor(second_member, admin_role, admin_user);
    let mut restricted_audit = audit_event(82);
    restricted_audit.actor_id = Some(role_manager);
    restricted_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(82),
            role_manager,
            9_999,
            &[
                admin_role_membership.clone(),
                dependency_write(second_member, admin_role, 5),
                bump_member_epoch(second_member, 2, "second_member")?,
            ],
            &[restricted_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let mut admin_audit = audit_event(83);
    admin_audit.actor_id = Some(admin_user);
    admin_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(83),
            admin_user,
            9_999,
            &[
                admin_role_membership_by_admin.clone(),
                dependency_write(second_member, admin_role, 5),
                bump_member_epoch(second_member, 2, "second_member")?,
            ],
            &[admin_audit],
        )?,
        4
    );
    let mut reduce_admin_audit = audit_event(85);
    reduce_admin_audit.actor_id = Some(admin_user);
    reduce_admin_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(85),
            admin_user,
            9_999,
            &[
                remove_catalog_record(membership_write_with_grantor(
                    admin_user, admin_role, admin_role,
                ))?,
                remove_catalog_record(dependency_write(admin_user, admin_role, 5))?,
                bump_member_epoch(admin_user, 2, "role_admin")?,
            ],
            &[reduce_admin_audit],
        )?,
        5
    );

    let mut last_admin_audit = audit_event(86);
    last_admin_audit.actor_id = Some(second_member);
    last_admin_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(86),
            second_member,
            9_999,
            &[
                remove_catalog_record(admin_role_membership_by_admin.clone())?,
                remove_catalog_record(dependency_write(second_member, admin_role, 5))?,
                bump_member_epoch(second_member, 3, "second_member")?,
            ],
            &[last_admin_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 5);
    assert_eq!(
        lookup_outcome(&database, id(80), role_manager, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(84), role_manager, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    for (invocation_id, principal_id) in [
        (id(81), denied_user),
        (id(82), role_manager),
        (id(87), role_manager),
    ] {
        assert_eq!(
            lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted
        );
    }
    assert_eq!(
        lookup_outcome(&database, id(83), admin_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(85), admin_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(86), second_member, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(manager_membership.key.as_slice())?.is_none());
    assert!(catalog.get(denied_membership.key.as_slice())?.is_none());
    assert!(
        catalog
            .get(inherited_admin_membership.key.as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(admin_role_membership_by_admin.key.as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(
                membership_write_with_grantor(admin_user, admin_role, admin_role)
                    .key
                    .as_slice()
            )?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(10, second_member).as_slice())?
            .is_some()
    );
    drop(catalog);
    drop(read);

    let membership_race_invocation = id(88);
    let privilege_revoke_invocation = id(89);
    let before_authority_race = commit_sequence(&database)?;
    let authority_race_barrier = Arc::new(Barrier::new(3));
    let (membership_result, privilege_revoke_result) = std::thread::scope(|scope| {
        let membership_barrier = Arc::clone(&authority_race_barrier);
        let membership_storage = &storage;
        let membership = scope.spawn(move || {
            let mut audit = audit_event(88);
            audit.actor_id = Some(role_manager);
            audit.correlation_id = membership_race_invocation;
            audit.action = AuditAction::AuthorityChange;
            membership_barrier.wait();
            membership_storage.set_role_membership(
                membership_race_invocation,
                role_manager,
                prdb_engine::storage_manager::RoleMembershipChange {
                    member_id: first_member,
                    role_id: app_role,
                    grant: true,
                },
                9_999,
                audit,
            )
        });
        let revoke_barrier = Arc::clone(&authority_race_barrier);
        let revoke_storage = &storage;
        let revoke = scope.spawn(move || {
            let mut audit = audit_event(89);
            audit.actor_id = Some(second_member);
            audit.correlation_id = privilege_revoke_invocation;
            audit.action = AuditAction::AuthorityChange;
            revoke_barrier.wait();
            revoke_storage.set_system_privilege(
                privilege_revoke_invocation,
                second_member,
                prdb_engine::storage_manager::SystemPrivilegeChange {
                    grantee_id: role_manager,
                    privilege:
                        prdb_engine::storage_manager::AdministrativeSystemPrivilege::ManageRoles,
                    grant: false,
                },
                9_999,
                audit,
            )
        });
        authority_race_barrier.wait();
        (
            membership.join().expect("membership grant thread panicked"),
            revoke.join().expect("MANAGE_ROLES revoke thread panicked"),
        )
    });
    let membership_sequence = match membership_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::storage_manager::MembershipLifecycleError::Unauthorized) => None,
        Err(prdb_engine::storage_manager::MembershipLifecycleError::Commit(
            CommitError::Conflict,
        )) => None,
        Err(error) => return Err(Box::new(error)),
    };
    let privilege_revoke_sequence = privilege_revoke_result
        .map_err(|error| format!("MANAGE_ROLES revoke failed: {error:?}"))?;
    assert_eq!(
        privilege_revoke_sequence,
        before_authority_race + 1 + u64::from(membership_sequence.is_some())
    );
    if let Some(sequence) = membership_sequence {
        assert!(sequence < privilege_revoke_sequence);
    }
    assert_eq!(commit_sequence(&database)?, privilege_revoke_sequence);
    assert_eq!(
        lookup_outcome(&database, membership_race_invocation, role_manager, 1)?
            .ok_or("missing raced membership outcome")?
            .status,
        if membership_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        lookup_outcome(&database, privilege_revoke_invocation, second_member, 1)?
            .ok_or("missing MANAGE_ROLES revoke outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let membership_key = membership_write_with_grantor(first_member, app_role, role_manager).key;
    assert_eq!(
        catalog.get(membership_key.as_slice())?.is_some(),
        membership_sequence.is_some()
    );
    assert!(
        catalog
            .get(
                grant_write(role_manager, database_id, 13, 1, database_id, admin_role)
                    .key
                    .as_slice()
            )?
            .is_none()
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn membership_ddl_requires_an_unexpired_credential_for_a_final_admin()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0xa1);
    let added_admin = id(0xa2);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let records = [
        principal_write_with_name(admin_user, 1, 1, "expired_admin"),
        object_write_with_name(admin_user, 8, "expired_admin"),
        principal_write_with_name(added_admin, 1, 1, "expired_added_admin"),
        object_write_with_name(added_admin, 8, "expired_added_admin"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        credential_verifier_write(id(0xa3), admin_user, &[0xa3; 32], 1, 2, None, 1),
        credential_verifier_write(id(0xa4), added_admin, &[0xa4; 32], 1, 2, None, 1),
    ];
    let mut setup_audit = identity_audit_event(0xa0, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xa0), admin_role, 9_999, &records, &[setup_audit])?;

    let mut bump = principal_write_with_name(added_admin, 1, 2, "expired_added_admin");
    bump.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(10, added_admin).as_slice())?
            .ok_or("added admin principal missing")?
            .value()
            .to_vec(),
    );
    let membership = membership_write_with_grantor(added_admin, admin_role, admin_user);
    let mut audit = identity_audit_event(0xa5, admin_user, added_admin);
    audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(0xa5),
            admin_user,
            9_999,
            &[
                membership,
                dependency_write(added_admin, admin_role, 5),
                bump,
            ],
            &[audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(0xa5), admin_user, 1)?
            .ok_or("missing aborted outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn concurrent_admin_membership_revocations_preserve_a_usable_administrator()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::RoleMembershipChange;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let first_admin = id(0xd1);
    let second_admin = id(0xd2);
    let admin_role = builtin_owner_id();
    let first_invocation = id(0xd3);
    let second_invocation = id(0xd4);
    let records = [
        principal_write_with_name(first_admin, 1, 1, "concurrent_admin_first"),
        object_write_with_name(first_admin, 8, "concurrent_admin_first"),
        principal_write_with_name(second_admin, 1, 1, "concurrent_admin_second"),
        object_write_with_name(second_admin, 8, "concurrent_admin_second"),
        membership_write_with_grantor(first_admin, admin_role, admin_role),
        dependency_write(first_admin, admin_role, 5),
        membership_write_with_grantor(second_admin, admin_role, admin_role),
        dependency_write(second_admin, admin_role, 5),
        credential_verifier_write(id(0xd5), first_admin, &[0xd5; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(0xd6), second_admin, &[0xd6; 32], 1, i64::MAX, None, 1),
    ];
    let mut setup_audit = identity_audit_event(0xd0, admin_role, first_admin);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xd0), admin_role, 9_999, &records, &[setup_audit])?;

    let barrier = Arc::new(Barrier::new(2));
    let (first_result, second_result) = std::thread::scope(|scope| {
        let first_barrier = Arc::clone(&barrier);
        let first_storage = &storage;
        let first = scope.spawn(move || {
            let mut audit = identity_audit_event(0xd7, first_admin, second_admin);
            audit.correlation_id = first_invocation;
            audit.action = AuditAction::AuthorityChange;
            first_barrier.wait();
            first_storage.set_role_membership(
                first_invocation,
                first_admin,
                RoleMembershipChange {
                    member_id: second_admin,
                    role_id: admin_role,
                    grant: false,
                },
                9_999,
                audit,
            )
        });
        let mut audit = identity_audit_event(0xd8, second_admin, first_admin);
        audit.correlation_id = second_invocation;
        audit.action = AuditAction::AuthorityChange;
        barrier.wait();
        let second = storage.set_role_membership(
            second_invocation,
            second_admin,
            RoleMembershipChange {
                member_id: first_admin,
                role_id: admin_role,
                grant: false,
            },
            9_999,
            audit,
        );
        (
            first.join().expect("first admin DDL thread panicked"),
            second,
        )
    });

    let first_committed = first_result.is_ok();
    let second_committed = second_result.is_ok();
    assert_ne!(
        first_committed, second_committed,
        "exactly one admin membership revocation may commit"
    );
    for error in [first_result.as_ref().err(), second_result.as_ref().err()]
        .into_iter()
        .flatten()
    {
        assert!(
            matches!(
                error,
                prdb_engine::storage_manager::MembershipLifecycleError::Commit(
                    CommitError::Conflict | CommitError::DdlAuthorizationDenied
                )
            ),
            "unexpected competing administrator revocation result: {error:?}"
        );
    }
    let winner = if first_committed {
        first_admin
    } else {
        second_admin
    };
    let removed = if first_committed {
        second_admin
    } else {
        first_admin
    };
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(membership_write(winner, admin_role).key.as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(dependency_write(winner, admin_role, 5).key.as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(membership_write(removed, admin_role).key.as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(dependency_write(removed, admin_role, 5).key.as_slice())?
            .is_none()
    );
    drop(catalog);
    drop(read);
    for (invocation_id, committed) in [
        (first_invocation, first_committed),
        (second_invocation, second_committed),
    ] {
        let outcome = lookup_outcome(
            &database,
            invocation_id,
            if invocation_id == first_invocation {
                first_admin
            } else {
                second_admin
            },
            1,
        )?
        .ok_or("missing competing membership outcome")?;
        assert_eq!(
            outcome.status,
            if committed {
                OutcomeStatus::Committed
            } else {
                OutcomeStatus::Aborted
            }
        );
    }
    assert_eq!(commit_sequence(&database)?, 2);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_restricts_system_privilege_ddl_to_database_admins()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0x71);
    let role_manager = id(0x72);
    let target_user = id(0x73);
    let admin_role = builtin_owner_id();
    let database_id = id(0x33);
    let records = [
        principal_write_with_name(admin_user, 1, 1, "system_admin"),
        object_write_with_name(admin_user, 8, "system_admin"),
        principal_write_with_name(role_manager, 1, 1, "system_role_manager"),
        object_write_with_name(role_manager, 8, "system_role_manager"),
        principal_write_with_name(target_user, 1, 1, "system_target"),
        object_write_with_name(target_user, 8, "system_target"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        grant_write(role_manager, database_id, 13, 1, database_id, admin_role),
        dependency_write(role_manager, database_id, 5),
    ];
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut setup_audit = audit_event(90);
    setup_audit.actor_id = Some(admin_role);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(90), admin_role, 9_999, &records, &[setup_audit])?;

    let bump_target_epoch = |epoch| -> Result<_, Box<dyn std::error::Error>> {
        let mut write = principal_write_with_name(target_user, 1, epoch, "system_target");
        let read = database.begin_read()?;
        write.expected_value = Some(
            read.open_table(CATALOG)?
                .get(id_catalog_key(10, target_user).as_slice())?
                .ok_or("missing system grant target")?
                .value()
                .to_vec(),
        );
        Ok(write)
    };
    let remove_catalog_record = |mut write: CatalogWrite| -> Result<_, Box<dyn std::error::Error>> {
        let read = database.begin_read()?;
        write.expected_value = Some(
            read.open_table(CATALOG)?
                .get(write.key.as_slice())?
                .ok_or("missing system privilege record")?
                .value()
                .to_vec(),
        );
        write.payload = None;
        Ok(write)
    };

    let manage_users_grant = grant_write(target_user, database_id, 12, 1, database_id, admin_user);
    let mut grant_audit = audit_event(91);
    grant_audit.actor_id = Some(admin_user);
    grant_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_system_privilege_ddl(
            id(91),
            admin_user,
            9_999,
            &[
                manage_users_grant.clone(),
                dependency_write(target_user, database_id, 5),
                bump_target_epoch(2)?,
            ],
            &[grant_audit],
        )?,
        2
    );

    let create_schema_grant =
        grant_write(target_user, database_id, 14, 1, database_id, role_manager);
    let mut denied_audit = audit_event(92);
    denied_audit.actor_id = Some(role_manager);
    denied_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_system_privilege_ddl(
            id(92),
            role_manager,
            9_999,
            &[
                create_schema_grant,
                dependency_write(target_user, database_id, 5),
                bump_target_epoch(3)?,
            ],
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let mut revoke_audit = audit_event(93);
    revoke_audit.actor_id = Some(admin_user);
    revoke_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_system_privilege_ddl(
            id(93),
            admin_user,
            9_999,
            &[
                remove_catalog_record(manage_users_grant.clone())?,
                remove_catalog_record(dependency_write(target_user, database_id, 5))?,
                bump_target_epoch(3)?,
            ],
            &[revoke_audit],
        )?,
        3
    );
    assert_eq!(commit_sequence(&database)?, 3);
    for (invocation_id, principal_id, status) in [
        (id(91), admin_user, OutcomeStatus::Committed),
        (id(92), role_manager, OutcomeStatus::Aborted),
        (id(93), admin_user, OutcomeStatus::Committed),
    ] {
        assert_eq!(
            lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            status
        );
    }
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(manage_users_grant.key.as_slice())?.is_none());
    assert!(
        catalog
            .get(dependency_write(target_user, database_id, 5).key.as_slice())?
            .is_none()
    );
    Ok(())
}

pub(super) fn identity_ddl_refuses_to_reuse_a_tombstoned_id()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0xc1);
    let retired_id = id(0xc2);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let setup = [
        principal_write_with_name(admin_user, 1, 1, "tombstone_admin"),
        object_write_with_name(admin_user, 8, "tombstone_admin"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0xc0, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xc0), admin_role, 9_999, &setup, &[setup_audit])?;
    storage.commit_catalog_and_audit(
        id(0xc3),
        admin_user,
        9_999,
        &[identity_tombstone_write(retired_id)],
        &[identity_audit_event(0xc3, admin_user, retired_id)],
    )?;

    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0xc4),
            admin_user,
            9_999,
            &[
                principal_write_with_name(retired_id, 1, 1, "retired_identity"),
                object_write_with_name(retired_id, 8, "retired_identity"),
            ],
            &[identity_audit_event(0xc4, admin_user, retired_id)],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(0xc5),
            admin_user,
            9_999,
            &[identity_tombstone_write(admin_user)],
            &[identity_audit_event(0xc5, admin_user, admin_user)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 2);
    assert!(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(10, retired_id).as_slice())?
            .is_none()
    );
    Ok(())
}

pub(super) fn identity_drop_aborts_a_role_authorized_staged_update()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{Effect, Expr, Operation, Subject};
    use prdb_engine::storage_manager::{
        CreatePolicyRequest, CreateTableRequest, ObjectGrantPrivilege, ObjectGrantTarget,
        PolicyKind, SetObjectGrantRequest, TableColumnDefinition,
    };
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_user = id(0xb0);
    let role_manager = id(0xb1);
    let token_id = id(0xb3);
    let token_secret = [0xb4; 32];
    let admin_role = builtin_owner_id();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let mut setup_audit = identity_audit_event(0xb5, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xb5),
        admin_role,
        now + 3_600_000_000,
        &[
            principal_write_with_name(admin_user, 1, 1, "drop_writer_admin"),
            object_write_with_name(admin_user, 8, "drop_writer_admin"),
            principal_write_with_name(role_manager, 1, 1, "drop_writer_role_manager"),
            object_write_with_name(role_manager, 8, "drop_writer_role_manager"),
            grant_write(role_manager, id(0x33), 13, 1, id(0x33), admin_role),
            dependency_write(role_manager, id(0x33), 5),
            membership_write_with_grantor(admin_user, admin_role, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(id(0xb2), admin_user, &[0xb2; 32], 1, i64::MAX, None, 1),
        ],
        &[setup_audit],
    )?;
    let mut create_user_audit = identity_audit_event(0xb6, admin_user, admin_user);
    create_user_audit.correlation_id = id(0xb6);
    let created_user = storage
        .create_user(
            id(0xb6),
            admin_user,
            "drop_writer",
            now + 3_600_000_000,
            create_user_audit,
        )
        .map_err(|error| format!("create user for stale drop writer: {error:?}"))?;
    let writer_user = created_user.user_id;
    let mut create_role_audit = identity_audit_event(0xb7, admin_user, admin_user);
    create_role_audit.correlation_id = id(0xb7);
    let created_role = storage
        .create_role(
            id(0xb7),
            admin_user,
            "drop_writer_role",
            now + 3_600_000_000,
            create_role_audit,
        )
        .map_err(|error| format!("create role for stale drop writer: {error:?}"))?;
    let writer_role = created_role.role_id;
    assert_ne!(writer_role, admin_role);
    let mut token_audit = identity_audit_event(0xb8, admin_user, writer_user);
    token_audit.action = AuditAction::TokenLifecycle;
    storage
        .commit_catalog_and_audit(
            id(0xb8),
            admin_user,
            now + 3_600_000_000,
            &[credential_verifier_write(
                token_id,
                writer_user,
                &token_secret,
                now - 1_000_000,
                now + 3_600_000_000,
                None,
                1,
            )],
            &[token_audit],
        )
        .map_err(|error| format!("issue token for stale drop writer: {error:?}"))?;
    let mut membership_audit = identity_audit_event(0xb9, role_manager, writer_user);
    membership_audit.correlation_id = id(0xb9);
    membership_audit.action = AuditAction::AuthorityChange;
    storage
        .set_role_membership(
            id(0xb9),
            role_manager,
            prdb_engine::storage_manager::RoleMembershipChange {
                member_id: writer_user,
                role_id: writer_role,
                grant: true,
            },
            now + 3_600_000_000,
            membership_audit,
        )
        .map_err(|error| format!("grant role membership for stale drop writer: {error:?}"))?;

    let mut schema_audit = identity_audit_event(0xba, admin_user, admin_user);
    schema_audit.correlation_id = id(0xba);
    schema_audit.action = AuditAction::Ddl;
    let schema = storage
        .create_schema(
            id(0xba),
            admin_user,
            "drop_writer_schema",
            admin_user,
            now + 3_600_000_000,
            schema_audit,
        )
        .map_err(|error| format!("create schema for stale drop writer: {error:?}"))?;
    let mut table_audit = identity_audit_event(0xbb, admin_user, schema.schema_id);
    table_audit.correlation_id = id(0xbb);
    table_audit.action = AuditAction::Ddl;
    let table = storage
        .create_table(CreateTableRequest {
            invocation_id: id(0xbb),
            authenticated_actor_id: admin_user,
            schema_id: schema.schema_id,
            name: "drop_writer_table".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "value".to_owned(),
                scalar_type: ScalarType::Bytea,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: now + 3_600_000_000,
            audit_event: table_audit,
        })
        .map_err(|error| format!("create table for stale drop writer: {error:?}"))?;
    let column_id = table.column_ids[0];
    for (offset, (object_id, privilege, target)) in [
        (
            schema.schema_id,
            ObjectGrantPrivilege::Usage,
            ObjectGrantTarget::Object,
        ),
        (
            table.table_id,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Object,
        ),
        (
            table.table_id,
            ObjectGrantPrivilege::Update,
            ObjectGrantTarget::Object,
        ),
        (
            table.table_id,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Column(column_id),
        ),
        (
            table.table_id,
            ObjectGrantPrivilege::Update,
            ObjectGrantTarget::Column(column_id),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let invocation_id = id(0xbc + offset as u8);
        let mut grant_audit = identity_audit_event(0xbc + offset as u8, admin_user, object_id);
        grant_audit.correlation_id = invocation_id;
        grant_audit.action = AuditAction::AuthorityChange;
        storage
            .set_object_grant(SetObjectGrantRequest {
                invocation_id,
                authenticated_actor_id: admin_user,
                grantee_id: writer_role,
                object_id,
                privilege,
                target,
                grant: true,
                retention_deadline_us: now + 3_600_000_000,
                audit_event: grant_audit,
            })
            .map_err(|error| format!("grant role table access for stale drop writer: {error:?}"))?;
    }
    let allow = Some(Expr::Literal(Some(ScalarValue::Boolean(true))));
    for (offset, (name, kind, operation, columns)) in [
        (
            "drop_writer_select_row",
            PolicyKind::Row,
            Operation::Select,
            Vec::new(),
        ),
        (
            "drop_writer_update_row",
            PolicyKind::Row,
            Operation::Update,
            Vec::new(),
        ),
        (
            "drop_writer_select_cell",
            PolicyKind::Cell,
            Operation::Select,
            vec![column_id],
        ),
        (
            "drop_writer_update_cell",
            PolicyKind::Cell,
            Operation::Update,
            vec![column_id],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let invocation_id = id(0xc1 + offset as u8);
        let mut policy_audit =
            identity_audit_event(0xc1 + offset as u8, admin_user, table.table_id);
        policy_audit.correlation_id = invocation_id;
        policy_audit.action = AuditAction::Ddl;
        storage
            .create_policy(CreatePolicyRequest {
                invocation_id,
                authenticated_actor_id: admin_user,
                table_id: table.table_id,
                name: name.to_owned(),
                kind,
                operation,
                effect: Effect::Allow,
                recipients: vec![Subject::Role(writer_role)],
                target_column_ids: columns,
                using_expression: matches!(operation, Operation::Select | Operation::Update)
                    .then(|| allow.clone())
                    .flatten(),
                with_check_expression: (operation == Operation::Update)
                    .then(|| allow.clone())
                    .flatten(),
                retention_deadline_us: now + 3_600_000_000,
                audit_event: policy_audit,
            })
            .map_err(|error| format!("create role policy for stale drop writer: {error:?}"))?;
    }
    let row_id = id(0xc5);
    let row_key = table_row_key(table.table_id, row_id);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: false,
    }];
    let original_value = ScalarValue::Bytea(b"before identity drop".to_vec());
    commit_rows(
        &database,
        id(0xc6),
        admin_user,
        now + 3_600_000_000,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(encode_tuple(&columns, 1, &[Some(original_value.clone())])?),
        }],
    )?;

    let rename_writer_invocation = id(0xc7);
    let (mut stale_writer, roles) = storage.begin_token_authenticated_invocation(
        rename_writer_invocation,
        token_id,
        &token_secret,
        now + 3_600_000_000,
        4,
        4096,
    )?;
    assert_eq!(roles.len(), 1);
    assert!(roles.contains(&writer_role));
    {
        let mut executor = stale_writer.open_executor_metadata_connection();
        let (relation, _) = executor
            .resolve_relation("drop_writer_schema", "drop_writer_table")?
            .ok_or("role-authorized relation was not visible")?;
        let cursor = executor.open_relation_scan(&relation, &[column_id])?;
        let page = executor.next_relation_scan_batch(&cursor, 4, 4096)?;
        assert_eq!(page.rows.len(), 1);
        assert_eq!(
            page.rows[0].1,
            vec![prdb_engine::authorization::SanitizedCell::Value(
                original_value.clone()
            )]
        );
        executor.stage_update(
            &page.rows[0].0,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(b"after identity rename".to_vec())),
            }],
        )?;
    }

    let race_barrier = Arc::new(Barrier::new(2));
    let (rename_write_result, rename_sequence) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&race_barrier);
        let storage = &storage;
        let renamer = scope.spawn(move || {
            let mut rename_audit = identity_audit_event(0xc8, admin_user, writer_user);
            rename_audit.correlation_id = id(0xc8);
            rename_audit.action = AuditAction::Ddl;
            barrier.wait();
            storage
                .rename_identity(
                    id(0xc8),
                    admin_user,
                    writer_user,
                    "renamed_drop_writer",
                    now + 3_600_000_000,
                    rename_audit,
                )
                .expect("identity rename should commit during stale-write race")
        });
        race_barrier.wait();
        let write_result = stale_writer.commit();
        let rename_sequence = renamer.join().expect("identity rename thread panicked");
        (write_result, rename_sequence)
    });
    let (rename_write_sequence, rename_write_status) = match rename_write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(TransactionError::Commit(CommitError::Conflict)) => (None, OutcomeStatus::Aborted),
        Err(error) => panic!("unexpected staged-writer result in identity rename race: {error:?}"),
    };
    assert_eq!(commit_sequence(&database)?, rename_sequence);
    if let Some(sequence) = rename_write_sequence {
        assert!(sequence < rename_sequence);
    }
    let value_after_rename = if rename_write_status == OutcomeStatus::Committed {
        ScalarValue::Bytea(b"after identity rename".to_vec())
    } else {
        original_value.clone()
    };
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &row(&database, &row_key)?
                .ok_or("row disappeared after identity drop")?
                .payload,
        )?
        .values,
        [Some(value_after_rename.clone())],
        "staged row must match the serialized identity rename race"
    );
    assert_eq!(
        lookup_outcome(&database, rename_writer_invocation, writer_user, now)?
            .ok_or("missing identity-rename stale writer outcome")?
            .status,
        rename_write_status
    );
    let (mut renamed_session, renamed_roles) = storage.begin_token_authenticated_invocation(
        id(0xc9),
        token_id,
        &token_secret,
        now + 3_600_000_000,
        4,
        4096,
    )?;
    assert!(renamed_roles.contains(&writer_role));
    renamed_session.abort()?;

    let rename_first_invocation = id(0xd0);
    let (mut rename_first_writer, _) = storage.begin_token_authenticated_invocation(
        rename_first_invocation,
        token_id,
        &token_secret,
        now + 3_600_000_000,
        4,
        4096,
    )?;
    {
        let mut executor = rename_first_writer.open_executor_metadata_connection();
        let (relation, _) = executor
            .resolve_relation("drop_writer_schema", "drop_writer_table")?
            .ok_or("relation disappeared before second identity rename")?;
        let cursor = executor.open_relation_scan(&relation, &[column_id])?;
        let page = executor.next_relation_scan_batch(&cursor, 4, 4096)?;
        assert_eq!(page.rows.len(), 1);
        executor.stage_update(
            &page.rows[0].0,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(b"after second identity rename".to_vec())),
            }],
        )?;
    }
    let mut second_rename_audit = identity_audit_event(0xd1, admin_user, writer_user);
    second_rename_audit.correlation_id = id(0xd1);
    second_rename_audit.action = AuditAction::Ddl;
    let second_rename_sequence = storage.rename_identity(
        id(0xd1),
        admin_user,
        writer_user,
        "renamed_drop_writer_again",
        now + 3_600_000_000,
        second_rename_audit,
    )?;
    assert!(matches!(
        rename_first_writer.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert_eq!(commit_sequence(&database)?, second_rename_sequence);
    assert_eq!(
        lookup_outcome(&database, rename_first_invocation, writer_user, now)?
            .ok_or("missing rename-first stale writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &row(&database, &row_key)?
                .ok_or("row disappeared after second identity rename")?
                .payload,
        )?
        .values,
        [Some(value_after_rename.clone())],
        "rename-first schedule must not publish the stale update"
    );

    let drop_writer_invocation = id(0xca);
    let (mut drop_writer, drop_roles) = storage.begin_token_authenticated_invocation(
        drop_writer_invocation,
        token_id,
        &token_secret,
        now + 3_600_000_000,
        4,
        4096,
    )?;
    assert!(drop_roles.contains(&writer_role));
    {
        let mut executor = drop_writer.open_executor_metadata_connection();
        let (relation, _) = executor
            .resolve_relation("drop_writer_schema", "drop_writer_table")?
            .ok_or("renamed role-authorized relation was not visible")?;
        let cursor = executor.open_relation_scan(&relation, &[column_id])?;
        let page = executor.next_relation_scan_batch(&cursor, 4, 4096)?;
        assert_eq!(page.rows.len(), 1);
        assert_eq!(
            page.rows[0].1,
            vec![prdb_engine::authorization::SanitizedCell::Value(
                value_after_rename.clone()
            )]
        );
        executor.stage_update(
            &page.rows[0].0,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(b"after identity drop".to_vec())),
            }],
        )?;
    }

    let race_barrier = Arc::new(Barrier::new(2));
    let (write_result, (membership_sequence, drop_sequence)) = std::thread::scope(|scope| {
        let barrier = Arc::clone(&race_barrier);
        let storage = &storage;
        let revoker = scope.spawn(move || {
            let mut revoke_membership_audit = identity_audit_event(0xcb, role_manager, writer_user);
            revoke_membership_audit.correlation_id = id(0xcb);
            revoke_membership_audit.action = AuditAction::AuthorityChange;
            barrier.wait();
            let membership_sequence = storage
                .set_role_membership(
                    id(0xcb),
                    role_manager,
                    prdb_engine::storage_manager::RoleMembershipChange {
                        member_id: writer_user,
                        role_id: writer_role,
                        grant: false,
                    },
                    now + 3_600_000_000,
                    revoke_membership_audit,
                )
                .expect("membership revocation should commit during identity race");
            let mut drop_audit = identity_audit_event(0xcc, admin_user, writer_user);
            drop_audit.correlation_id = id(0xcc);
            drop_audit.action = AuditAction::Ddl;
            let drop_sequence = storage
                .drop_identity(
                    id(0xcc),
                    admin_user,
                    writer_user,
                    now + 3_600_000_000,
                    drop_audit,
                )
                .expect("identity drop should commit after membership revocation");
            (membership_sequence, drop_sequence)
        });
        race_barrier.wait();
        let write_result = drop_writer.commit();
        let revocation_sequences = revoker.join().expect("identity revoker thread panicked");
        (write_result, revocation_sequences)
    });
    let (write_sequence, write_status) = match write_result {
        Ok(sequence) => (Some(sequence), OutcomeStatus::Committed),
        Err(TransactionError::Commit(CommitError::Conflict)) => (None, OutcomeStatus::Aborted),
        Err(error) => panic!("unexpected staged-writer result in identity race: {error:?}"),
    };
    assert!(membership_sequence < drop_sequence);
    assert_eq!(commit_sequence(&database)?, drop_sequence);
    if let Some(sequence) = write_sequence {
        assert!(sequence < membership_sequence);
    }
    let expected_value = if write_status == OutcomeStatus::Committed {
        ScalarValue::Bytea(b"after identity drop".to_vec())
    } else {
        value_after_rename
    };
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &row(&database, &row_key)?
                .ok_or("row disappeared after identity drop")?
                .payload,
        )?
        .values,
        [Some(expected_value)],
        "staged row must match the serialized identity/revocation race"
    );
    assert_eq!(
        lookup_outcome(&database, drop_writer_invocation, writer_user, now)?
            .ok_or("missing identity-drop stale writer outcome")?
            .status,
        write_status
    );
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0xcd),
            token_id,
            &token_secret,
            now + 3_600_000_000,
            4,
            4096,
        ),
        Err(TransactionError::AuthenticationFailed)
    ));
    assert_eq!(lookup_outcome(&database, id(0xcd), writer_user, now)?, None);
    Ok(())
}

pub(super) fn identity_login_disable_survives_process_exit_after_commit()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::TokenIssueRequest;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_IDENTITY_EXIT_AFTER_LOGIN_DISABLE_DATABASE";
    const CHILD_USER_ENV: &str = "PRDB_TEST_IDENTITY_EXIT_AFTER_LOGIN_DISABLE_USER";
    const CHILD_INVOCATION_ID: [u8; 16] = [0xa3; 16];
    const CHILD_AUDIT_ID: [u8; 16] = [0xa4; 16];
    const MANAGE_USERS_ACTOR: [u8; 16] = [0xa5; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let user_id_wire = std::env::var(CHILD_USER_ENV)?;
        if user_id_wire.len() != 32 {
            return Err("child user ID must be 32 hexadecimal characters".into());
        }
        let user_id = user_id_wire
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16)
                    .map_err(|error| format!("invalid child user ID: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let user_id: [u8; 16] = user_id
            .try_into()
            .map_err(|_| "child user ID must contain 16 bytes")?;
        let database = Database::open(database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let mut audit = identity_audit_event(CHILD_AUDIT_ID[0], MANAGE_USERS_ACTOR, user_id);
        audit.correlation_id = CHILD_INVOCATION_ID;
        storage.set_user_login_enabled(
            CHILD_INVOCATION_ID,
            MANAGE_USERS_ACTOR,
            user_id,
            false,
            i64::MAX,
            audit,
        )?;
        // Lose the process-local handles and any reply immediately after the
        // identity catalog commit has reported durable success.
        // SAFETY: `_exit` intentionally bypasses Rust destructors to model a
        // process crash after the redb transaction has committed.
        unsafe { libc::_exit(0) };
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    let database = initialize(&database_path)?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xa6, admin_role, MANAGE_USERS_ACTOR);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xa6),
        admin_role,
        i64::MAX,
        &[
            principal_write_with_name(MANAGE_USERS_ACTOR, 1, 1, "process_exit_user_manager"),
            object_write_with_name(MANAGE_USERS_ACTOR, 8, "process_exit_user_manager"),
            membership_write_with_grantor(MANAGE_USERS_ACTOR, admin_role, admin_role),
            dependency_write(MANAGE_USERS_ACTOR, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let now_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let expires_at_us = now_us.saturating_add(3_600_000_000);
    let mut manager_token_audit =
        identity_audit_event(0xa8, MANAGE_USERS_ACTOR, MANAGE_USERS_ACTOR);
    manager_token_audit.correlation_id = id(0xa7);
    manager_token_audit.action = AuditAction::TokenLifecycle;
    manager_token_audit.target_ids = vec![MANAGE_USERS_ACTOR];
    storage.issue_token(TokenIssueRequest {
        invocation_id: id(0xa7),
        authenticated_actor_id: MANAGE_USERS_ACTOR,
        user_id: MANAGE_USERS_ACTOR,
        label: "admin-session",
        expires_at_us,
        retention_deadline_us: expires_at_us,
        audit_event: manager_token_audit,
    })?;
    let mut create_audit = identity_audit_event(0xa0, MANAGE_USERS_ACTOR, MANAGE_USERS_ACTOR);
    create_audit.correlation_id = id(0xa0);
    let user = storage.create_user(
        id(0xa0),
        MANAGE_USERS_ACTOR,
        "crash_login_disable_user",
        expires_at_us,
        create_audit,
    )?;
    let mut token_audit = identity_audit_event(0xa1, MANAGE_USERS_ACTOR, user.user_id);
    token_audit.correlation_id = id(0xa1);
    token_audit.action = AuditAction::TokenLifecycle;
    token_audit.target_ids = vec![user.user_id];
    storage.issue_token(TokenIssueRequest {
        invocation_id: id(0xa1),
        authenticated_actor_id: MANAGE_USERS_ACTOR,
        user_id: user.user_id,
        label: "process-exit",
        expires_at_us,
        retention_deadline_us: expires_at_us,
        audit_event: token_audit,
    })?;
    assert_eq!(
        storage.list_tokens(MANAGE_USERS_ACTOR, user.user_id)?.len(),
        1
    );
    drop(storage);
    drop(database);

    let child_user_id = user
        .user_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("identity_login_disable_survives_process_exit_after_commit")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .env(CHILD_USER_ENV, child_user_id)
        .status()?;
    assert!(
        child_status.success(),
        "identity DDL child exited with {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    let principal = reopened
        .begin_read()?
        .open_table(CATALOG)?
        .get(id_catalog_key(10, user.user_id).as_slice())?
        .ok_or("disabled user principal was not durable")?;
    let principal = decode_registered_envelope(principal.value())?;
    let login_enabled = principal
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .ok_or("user LOGIN field missing")?;
    assert_eq!(login_enabled.payload, [0]);
    let identity_epoch = principal
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("user epoch missing")?;
    assert_eq!(
        u64::from_le_bytes(identity_epoch.payload.as_slice().try_into()?),
        2
    );
    let tokens = storage.list_tokens(MANAGE_USERS_ACTOR, user.user_id)?;
    assert_eq!(tokens.len(), 1);
    assert!(tokens[0].revoked);
    assert_eq!(
        lookup_outcome(&reopened, CHILD_INVOCATION_ID, MANAGE_USERS_ACTOR, now_us)?
            .ok_or("login-disable outcome missing after reopen")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(
        reopened
            .begin_read()?
            .open_table(schema::AUDIT)?
            .get(&CHILD_AUDIT_ID[..])?
            .is_some(),
        "identity DDL audit record missing after reopen"
    );
    prdb_engine::startup::validate_persisted_records(&reopened)?;
    Ok(())
}
