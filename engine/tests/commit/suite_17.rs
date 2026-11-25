use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::catalog_index::lookup_policies_for_table_operation;
use prdb_engine::commit::{
    CatalogWrite, CommitError, IndexWrite, RowWrite, commit_catalog_and_audit_with_fences,
    commit_rows, commit_rows_and_indexes,
};
use prdb_engine::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::schema::{self, CATALOG};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::{ReadableDatabase, ReadableTable};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_authorizes_policy_ddl_for_owners_and_inherited_manage_policies()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::Expr;
    use prdb_engine::catalog::{PolicyTarget, encode_policy_target};
    use prdb_engine::tuple::ScalarValue;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_owner = id(0x30);
    let policy_manager = id(0x31);
    let denied_user = id(0x32);
    let manager_role = id(0x3b);
    let schema_id = id(0x34);
    let table_id = id(0x35);
    let policy_id = id(0x36);
    let expression_id = id(0x37);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let schema_object = object_write_with_parent_and_owner_and_name(
        schema_id,
        1,
        None,
        table_owner,
        "policy_schema",
    );
    let table_object = object_write_with_parent_and_owner_and_name(
        table_id,
        3,
        Some(schema_id),
        table_owner,
        "policy_table",
    );
    let policy_key = id_catalog_key(13, policy_id);
    let table_key = id_catalog_key(5, table_id);
    let policy_payload_for =
        |target_policy_id: [u8; 16], target_expression_id: [u8; 16], epoch: u64| {
            let key = id_catalog_key(13, target_policy_id);
            encode_envelope(
                &family_schema(13).unwrap(),
                &key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::Id,
                        payload: table_id.to_vec(),
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
                        wire_type: WireType::U16,
                        payload: 1u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::Id,
                        payload: target_expression_id.to_vec(),
                    },
                    CatalogField {
                        tag: 7,
                        wire_type: WireType::U64,
                        payload: epoch.to_le_bytes().to_vec(),
                    },
                ],
            )
            .unwrap()
        };
    let policy_payload = |epoch| policy_payload_for(policy_id, expression_id, epoch);
    let table_payload = |epoch: u64| {
        table_write_with_versions(table_id, schema_id, 1, epoch)
            .payload
            .unwrap()
    };
    let policy_target_payload = encode_policy_target(&PolicyTarget {
        policy_id,
        kind: 2,
        target_id: manager_role,
    })?;
    let policy_target = CatalogWrite {
        key: policy_target_payload[32..].to_vec(),
        expected_value: None,
        payload: Some(policy_target_payload),
    };
    let mut setup_audit = audit_event(0x80);
    setup_audit.actor_id = Some(admin_role);
    let setup_writes = vec![
        principal_write_with_name(table_owner, 1, 1, "policy_table_owner"),
        object_write_with_name(table_owner, 8, "policy_table_owner"),
        principal_write_with_name(policy_manager, 1, 1, "policy_manager"),
        object_write_with_name(policy_manager, 8, "policy_manager"),
        principal_write_with_name(denied_user, 1, 1, "policy_denied"),
        object_write_with_name(denied_user, 8, "policy_denied"),
        principal_write_with_name(manager_role, 2, 1, "policy_managers"),
        object_write_with_name(manager_role, 9, "policy_managers"),
        membership_write(policy_manager, manager_role),
        dependency_write(policy_manager, manager_role, 5),
        grant_write(manager_role, schema_id, 10, 1, schema_id, admin_role),
        dependency_write(manager_role, schema_id, 5),
        schema_object,
        schema_write(schema_id),
        table_object,
        table_write_with_versions(table_id, schema_id, 1, 1),
        dependency_write(table_id, schema_id, 1),
        CatalogWrite {
            key: policy_key.clone(),
            expected_value: None,
            payload: Some(policy_payload(1)),
        },
        object_write_with_parent_and_owner_and_name(
            policy_id,
            10,
            None,
            table_owner,
            "visible_rows",
        ),
        expression_write(
            expression_id,
            3,
            &[],
            &Expr::Literal(Some(ScalarValue::Boolean(true))),
        ),
        object_write_with_parent_and_owner_and_name(
            expression_id,
            5,
            None,
            table_owner,
            "visible_rows_expression",
        ),
        dependency_write(policy_id, expression_id, 2),
        dependency_write(policy_id, manager_role, 5),
        policy_target,
    ];
    assert_eq!(
        storage
            .commit_catalog_and_audit(id(0x80), admin_role, 9_999, &setup_writes, &[setup_audit],)
            .map_err(|error| format!("base catalog setup: {error:?}"))?,
        1
    );
    let policy_snapshot = database.begin_read()?;
    let indexed_policies = lookup_policies_for_table_operation(&policy_snapshot, table_id, 1)?;
    assert_eq!(indexed_policies.len(), 1);
    assert_eq!(indexed_policies[0].logical_key, policy_key);

    let change_policy = |policy_epoch: u64, table_epoch: u64| {
        vec![
            CatalogWrite {
                key: policy_key.clone(),
                expected_value: Some(policy_payload(policy_epoch - 1)),
                payload: Some(policy_payload(policy_epoch)),
            },
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_payload(table_epoch - 1)),
                payload: Some(table_payload(table_epoch)),
            },
        ]
    };
    let policy_audit = |event_id, actor_id| {
        let mut event = audit_event(event_id);
        event.actor_id = Some(actor_id);
        event.action = AuditAction::Ddl;
        event.target_ids = vec![policy_id];
        event
    };
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x81),
            table_owner,
            9_999,
            &change_policy(2, 2),
            &[policy_audit(0x81, table_owner)],
        )?,
        2
    );
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x82),
            policy_manager,
            9_999,
            &change_policy(3, 3),
            &[policy_audit(0x82, policy_manager)],
        )?,
        3
    );

    let added_policy_id = id(0x38);
    let added_expression_id = id(0x39);
    let added_policy_key = id_catalog_key(13, added_policy_id);
    let added_policy_payload =
        |epoch| policy_payload_for(added_policy_id, added_expression_id, epoch);
    let added_policy_object = object_write_with_parent_and_owner_and_name(
        added_policy_id,
        10,
        None,
        table_owner,
        "additional_rows",
    );
    let added_expression = expression_write(
        added_expression_id,
        3,
        &[],
        &Expr::Literal(Some(ScalarValue::Boolean(true))),
    );
    let added_expression_object = object_write_with_parent_and_owner_and_name(
        added_expression_id,
        5,
        None,
        table_owner,
        "additional_rows_expression",
    );
    let added_target_payload = encode_policy_target(&PolicyTarget {
        policy_id: added_policy_id,
        kind: 2,
        target_id: manager_role,
    })?;
    let added_target = CatalogWrite {
        key: added_target_payload[32..].to_vec(),
        expected_value: None,
        payload: Some(added_target_payload.clone()),
    };
    let added_expression_edge = dependency_write(added_policy_id, added_expression_id, 2);
    let added_recipient_edge = dependency_write(added_policy_id, manager_role, 5);
    let mut add_table_epoch = table_write_with_versions(table_id, schema_id, 1, 4);
    add_table_epoch.expected_value = Some(table_payload(3));
    let mut create_audit = policy_audit(0x84, table_owner);
    create_audit.target_ids = vec![added_policy_id];
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x84),
            table_owner,
            9_999,
            &[
                CatalogWrite {
                    key: added_policy_key.clone(),
                    expected_value: None,
                    payload: Some(added_policy_payload(1)),
                },
                added_policy_object.clone(),
                added_expression.clone(),
                added_expression_object.clone(),
                added_target.clone(),
                added_expression_edge.clone(),
                added_recipient_edge.clone(),
                add_table_epoch,
            ],
            &[create_audit.clone()],
        )?,
        4
    );

    let mut deleted_policy_object = added_policy_object.clone();
    deleted_policy_object.expected_value = added_policy_object.payload.clone();
    deleted_policy_object.payload = None;
    let mut deleted_expression = added_expression.clone();
    deleted_expression.expected_value = added_expression.payload.clone();
    deleted_expression.payload = None;
    let mut deleted_expression_object = added_expression_object.clone();
    deleted_expression_object.expected_value = added_expression_object.payload.clone();
    deleted_expression_object.payload = None;
    let mut deleted_target = added_target.clone();
    deleted_target.expected_value = Some(added_target_payload);
    deleted_target.payload = None;
    let mut deleted_expression_edge = added_expression_edge.clone();
    deleted_expression_edge.expected_value = added_expression_edge.payload.clone();
    deleted_expression_edge.payload = None;
    let mut deleted_recipient_edge = added_recipient_edge.clone();
    deleted_recipient_edge.expected_value = added_recipient_edge.payload.clone();
    deleted_recipient_edge.payload = None;
    let mut delete_table_epoch = table_write_with_versions(table_id, schema_id, 1, 5);
    delete_table_epoch.expected_value = Some(table_payload(4));
    let deleted_policy = CatalogWrite {
        key: added_policy_key.clone(),
        expected_value: Some(added_policy_payload(1)),
        payload: None,
    };
    let mut delete_audit = policy_audit(0x85, table_owner);
    delete_audit.target_ids = vec![added_policy_id];
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x85),
            table_owner,
            9_999,
            &[
                deleted_policy.clone(),
                deleted_policy_object,
                deleted_expression,
                deleted_expression_object,
                deleted_target,
                deleted_expression_edge,
                deleted_recipient_edge,
                identity_tombstone_write(added_policy_id),
                identity_tombstone_write(added_expression_id),
                delete_table_epoch,
            ],
            &[delete_audit],
        )?,
        5
    );

    let unassigned_policy_id = id(0x40);
    let unassigned_expression_id = id(0x41);
    let unassigned_policy_key = id_catalog_key(13, unassigned_policy_id);
    let unassigned_policy_payload =
        policy_payload_for(unassigned_policy_id, unassigned_expression_id, 1);
    let unassigned_policy_object = object_write_with_parent_and_owner_and_name(
        unassigned_policy_id,
        10,
        None,
        table_owner,
        "unassigned_rows",
    );
    let unassigned_expression = expression_write(
        unassigned_expression_id,
        3,
        &[],
        &Expr::Literal(Some(ScalarValue::Boolean(true))),
    );
    let unassigned_expression_object = object_write_with_parent_and_owner_and_name(
        unassigned_expression_id,
        5,
        None,
        table_owner,
        "unassigned_rows_expression",
    );
    let mut unassigned_table_epoch = table_write_with_versions(table_id, schema_id, 1, 6);
    unassigned_table_epoch.expected_value = Some(table_payload(5));
    let mut unassigned_audit = policy_audit(0x86, table_owner);
    unassigned_audit.target_ids = vec![unassigned_policy_id];
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x86),
            table_owner,
            9_999,
            &[
                CatalogWrite {
                    key: unassigned_policy_key.clone(),
                    expected_value: None,
                    payload: Some(unassigned_policy_payload),
                },
                unassigned_policy_object,
                unassigned_expression,
                unassigned_expression_object,
                dependency_write(unassigned_policy_id, unassigned_expression_id, 2),
                unassigned_table_epoch,
            ],
            &[unassigned_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x83),
            denied_user,
            9_999,
            &change_policy(4, 6),
            &[policy_audit(0x83, denied_user)],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    assert_eq!(commit_sequence(&database)?, 5);
    assert_eq!(
        lookup_outcome(&database, id(0x81), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0x82), policy_manager, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0x84), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0x85), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0x86), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, id(0x83), denied_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert_eq!(
        catalog.get(policy_key.as_slice())?.unwrap().value(),
        policy_payload(3).as_slice()
    );
    assert_eq!(
        catalog.get(table_key.as_slice())?.unwrap().value(),
        table_payload(5).as_slice()
    );
    assert!(catalog.get(added_policy_key.as_slice())?.is_none());
    drop(catalog);
    drop(read);
    let mut create_policy_audit = policy_audit(0x87, table_owner);
    create_policy_audit.correlation_id = id(0x87);
    let created_policy =
        storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
            invocation_id: id(0x87),
            authenticated_actor_id: table_owner,
            table_id,
            name: "typed_policy".to_owned(),
            kind: prdb_engine::storage_manager::PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients: vec![prdb_engine::authorization::Subject::Role(manager_role)],
            target_column_ids: Vec::new(),
            using_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: create_policy_audit,
        })?;
    assert_ne!(created_policy.policy_id, [0; 16]);
    assert!(created_policy.using_expression_id.is_some());
    assert!(created_policy.with_check_expression_id.is_none());
    assert_eq!(created_policy.commit_sequence, 6);
    assert_eq!(commit_sequence(&database)?, 6);
    assert_eq!(
        lookup_outcome(&database, id(0x87), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    let duplicate_policy_invocation = id(0x8c);
    let mut duplicate_policy_audit = policy_audit(0x8c, table_owner);
    duplicate_policy_audit.correlation_id = duplicate_policy_invocation;
    assert!(matches!(
        storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
            invocation_id: duplicate_policy_invocation,
            authenticated_actor_id: table_owner,
            table_id,
            name: "typed_policy".to_owned(),
            kind: prdb_engine::storage_manager::PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients: vec![prdb_engine::authorization::Subject::Role(manager_role)],
            target_column_ids: Vec::new(),
            using_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: duplicate_policy_audit,
        }),
        Err(prdb_engine::storage_manager::TableLifecycleError::InvalidRequest)
    ));
    assert_eq!(commit_sequence(&database)?, 6);
    let mut drop_policy_audit = policy_audit(0x88, table_owner);
    drop_policy_audit.correlation_id = id(0x88);
    let drop_sequence = storage.drop_policy(prdb_engine::storage_manager::DropPolicyRequest {
        invocation_id: id(0x88),
        authenticated_actor_id: table_owner,
        policy_id: created_policy.policy_id,
        retention_deadline_us: 9_999,
        audit_event: drop_policy_audit,
    })?;
    assert_eq!(drop_sequence, 7);
    assert_eq!(commit_sequence(&database)?, 7);
    assert_eq!(
        lookup_outcome(&database, id(0x88), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        for retired_id in [
            created_policy.policy_id,
            created_policy
                .using_expression_id
                .ok_or("policy USING expression ID missing")?,
        ] {
            let reservation = catalog
                .get(id_catalog_key(17, retired_id).as_slice())?
                .ok_or("DROP POLICY must reserve every removed object ID")?;
            assert!(
                decode_registered_envelope(reservation.value())?
                    .fields
                    .is_empty()
            );
            drop(reservation);
        }
    }

    let replacement_invocation = id(0x89);
    let revoke_invocation = id(0x8a);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let table_record = decode_registered_envelope(
        catalog
            .get(table_key.as_slice())?
            .ok_or("policy replacement table record is missing")?
            .value(),
    )?;
    let table_epoch_before_replacement = u64::from_le_bytes(
        table_record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("policy replacement table epoch is missing")?
            .payload
            .as_slice()
            .try_into()?,
    );
    drop(catalog);
    drop(read);
    let replacement_barrier = Arc::new(Barrier::new(3));
    let manager_grant = grant_write(manager_role, schema_id, 10, 1, schema_id, admin_role);
    let replacement_result = std::thread::scope(|scope| {
        let replace_barrier = replacement_barrier.clone();
        let replace_storage = &storage;
        let replacement = scope.spawn(move || {
            let mut audit = policy_audit(0x8b, policy_manager);
            audit.correlation_id = replacement_invocation;
            replace_barrier.wait();
            replace_storage.replace_policy(
                prdb_engine::storage_manager::CreatePolicyRequest {
                    invocation_id: replacement_invocation,
                    authenticated_actor_id: policy_manager,
                    table_id,
                    name: "visible_rows".to_owned(),
                    kind: prdb_engine::storage_manager::PolicyKind::Row,
                    operation: prdb_engine::authorization::Operation::Select,
                    effect: prdb_engine::authorization::Effect::Allow,
                    recipients: vec![prdb_engine::authorization::Subject::Principal(table_owner)],
                    target_column_ids: Vec::new(),
                    using_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
                    with_check_expression: None,
                    retention_deadline_us: 9_999,
                    audit_event: audit,
                },
                policy_id,
            )
        });
        let revoke_barrier = replacement_barrier.clone();
        let revoke_storage = &storage;
        let revoke = scope.spawn(move || {
            let mut audit = identity_audit_event(0x8c, table_owner, manager_role);
            audit.correlation_id = revoke_invocation;
            audit.action = AuditAction::AuthorityChange;
            revoke_barrier.wait();
            revoke_storage.set_object_grant(prdb_engine::storage_manager::SetObjectGrantRequest {
                invocation_id: revoke_invocation,
                authenticated_actor_id: table_owner,
                grantee_id: manager_role,
                object_id: schema_id,
                privilege: prdb_engine::storage_manager::ObjectGrantPrivilege::ManagePolicies,
                target: prdb_engine::storage_manager::ObjectGrantTarget::Object,
                grant: false,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        replacement_barrier.wait();
        (
            replacement.join().expect("replacement DDL thread panicked"),
            revoke.join().expect("grant revoke thread panicked"),
        )
    });
    let (replacement_status, replacement_policy_id) = match replacement_result.0 {
        Ok(created) => {
            assert_ne!(created.policy_id, policy_id);
            (OutcomeStatus::Committed, Some(created.policy_id))
        }
        Err(prdb_engine::storage_manager::TableLifecycleError::Unauthorized)
        | Err(prdb_engine::storage_manager::TableLifecycleError::Commit(CommitError::Conflict)) => {
            (OutcomeStatus::Aborted, None)
        }
        other => panic!("unexpected replacement/revocation race: {other:?}"),
    };
    assert!(
        replacement_result.1.is_ok(),
        "database administrator must be able to revoke MANAGE_POLICIES: {:?}",
        replacement_result.1
    );
    assert_eq!(
        lookup_outcome(&database, replacement_invocation, policy_manager, 1)?
            .ok_or("missing policy replacement race outcome")?
            .status,
        replacement_status
    );
    assert_eq!(
        commit_sequence(&database)?,
        7 + u64::from(replacement_status == OutcomeStatus::Committed) + 1
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(manager_grant.key.as_slice())?.is_none());
    let final_table_record = decode_registered_envelope(
        catalog
            .get(table_key.as_slice())?
            .ok_or("policy replacement table record disappeared")?
            .value(),
    )?;
    let table_epoch_after_replacement = u64::from_le_bytes(
        final_table_record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("policy replacement table epoch disappeared")?
            .payload
            .as_slice()
            .try_into()?,
    );
    assert_eq!(
        table_epoch_after_replacement,
        table_epoch_before_replacement + u64::from(replacement_status == OutcomeStatus::Committed)
    );
    assert_eq!(
        catalog.get(policy_key.as_slice())?.is_some(),
        replacement_status == OutcomeStatus::Aborted
    );
    if let Some(replacement_policy_id) = replacement_policy_id {
        assert!(
            catalog
                .get(id_catalog_key(13, replacement_policy_id).as_slice())?
                .is_some()
        );
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_separates_foreign_key_alter_and_references_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let alter_and_reference_user = id(0x90);
    let reference_only_user = id(0x91);
    let alter_only_user = id(0x92);
    let ddl_admin = id(0x7e);
    let target_table_id = id(81);
    let source_table_id = id(82);
    let target_column_id = id(83);
    let target_constraint_id = id(85);
    let foreign_key_id = id(86);
    let schema_id = id(80);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let foreign_key_key = id_catalog_key(8, foreign_key_id);
    let foreign_key_object_key = id_catalog_key(2, foreign_key_id);
    let target_edge = dependency_write(foreign_key_id, target_constraint_id, 3);

    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != foreign_key_key
                && write.key != foreign_key_object_key
                && write.key != target_edge.key
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(ddl_admin, 1, 1, "column_drop_dba"),
        object_write_with_name(ddl_admin, 8, "column_drop_dba"),
        membership_write(ddl_admin, admin_role),
        dependency_write(ddl_admin, admin_role, 5),
        principal_write_with_name(alter_and_reference_user, 1, 1, "alter_reference_user"),
        object_write_with_name(alter_and_reference_user, 8, "alter_reference_user"),
        principal_write_with_name(reference_only_user, 1, 1, "reference_only_user"),
        object_write_with_name(reference_only_user, 8, "reference_only_user"),
        principal_write_with_name(alter_only_user, 1, 1, "alter_only_user"),
        object_write_with_name(alter_only_user, 8, "alter_only_user"),
    ]);
    let mut setup_audit = audit_event(0x70);
    setup_audit.actor_id = Some(admin_role);
    assert_eq!(
        storage
            .commit_catalog_and_audit(id(0x70), admin_role, 9_999, &setup_writes, &[setup_audit],)
            .map_err(|error| format!("base catalog setup: {error:?}"))?,
        1
    );
    let bump_principal_epoch = |principal_id, name: &str| {
        let mut write = principal_write_with_name(principal_id, 1, 2, name);
        write.expected_value = Some(
            principal_write_with_name(principal_id, 1, 1, name)
                .payload
                .unwrap(),
        );
        write
    };
    let authority_setup = [
        grant_write(
            alter_and_reference_user,
            source_table_id,
            8,
            1,
            source_table_id,
            admin_role,
        ),
        dependency_write(alter_and_reference_user, source_table_id, 5),
        grant_write(
            alter_and_reference_user,
            target_table_id,
            6,
            2,
            target_column_id,
            admin_role,
        ),
        dependency_write(alter_and_reference_user, target_column_id, 5),
        grant_write(
            reference_only_user,
            target_table_id,
            6,
            1,
            target_table_id,
            admin_role,
        ),
        dependency_write(reference_only_user, target_table_id, 5),
        grant_write(
            alter_only_user,
            source_table_id,
            8,
            1,
            source_table_id,
            admin_role,
        ),
        dependency_write(alter_only_user, source_table_id, 5),
        bump_principal_epoch(alter_and_reference_user, "alter_reference_user"),
        bump_principal_epoch(reference_only_user, "reference_only_user"),
        bump_principal_epoch(alter_only_user, "alter_only_user"),
    ];
    let mut authority_setup_audit = audit_event(0x75);
    authority_setup_audit.actor_id = Some(admin_role);
    assert_eq!(
        storage
            .commit_catalog_and_audit(
                id(0x75),
                admin_role,
                9_999,
                &authority_setup,
                &[authority_setup_audit],
            )
            .map_err(|error| format!("authority setup: {error:?}"))?,
        2
    );

    let foreign_key_definition = foreign_key_catalog_writes(true, true)
        .into_iter()
        .find(|write| write.key == foreign_key_key)
        .ok_or("missing foreign-key test definition")?;
    let foreign_key_object = object_write(foreign_key_id, 6);
    let add_edge = target_edge.clone();
    let mut source_table_add = table_write_with_versions(source_table_id, schema_id, 1, 2);
    source_table_add.expected_value = table_write(source_table_id, schema_id).payload;
    let mut target_table_add = table_write_with_versions(target_table_id, schema_id, 1, 2);
    target_table_add.expected_value = table_write(target_table_id, schema_id).payload;
    let stale_source_invocation = id(0x76);
    let stale_source_row = id(0x78);
    let mut stale_source_writer = storage.begin_invocation(
        stale_source_invocation,
        alter_and_reference_user,
        9_999,
        1,
        1024,
    )?;
    let stale_source_schema = stale_source_writer.load_catalog_table_schema(source_table_id)?;
    stale_source_writer.load_catalog_indexes(&stale_source_schema)?;
    let stale_source_tuple = encode_tuple(
        &stale_source_schema.columns,
        stale_source_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"before foreign key".to_vec()))],
    )?;
    stale_source_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: source_table_id,
            row: stale_source_row,
        },
        &stale_source_tuple,
    )?;
    let stale_target_invocation = id(0x77);
    let stale_target_row = id(0x79);
    let mut stale_target_writer = storage.begin_invocation(
        stale_target_invocation,
        alter_and_reference_user,
        9_999,
        1,
        1024,
    )?;
    let stale_target_schema = stale_target_writer.load_catalog_table_schema(target_table_id)?;
    stale_target_writer.load_catalog_indexes(&stale_target_schema)?;
    let stale_target_tuple = encode_tuple(
        &stale_target_schema.columns,
        stale_target_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"before foreign key".to_vec()))],
    )?;
    stale_target_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: target_table_id,
            row: stale_target_row,
        },
        &stale_target_tuple,
    )?;
    let mut alter_only_audit = audit_event(0x71);
    alter_only_audit.actor_id = Some(alter_only_user);
    assert_eq!(
        storage.commit_authorized_foreign_key_ddl(
            id(0x71),
            alter_only_user,
            9_999,
            &[
                foreign_key_definition.clone(),
                foreign_key_object.clone(),
                add_edge.clone(),
                source_table_add.clone(),
                target_table_add.clone(),
            ],
            &[alter_only_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let mut alter_reference_audit = audit_event(0x72);
    alter_reference_audit.actor_id = Some(alter_and_reference_user);
    assert_eq!(
        storage.commit_authorized_foreign_key_ddl(
            id(0x72),
            alter_and_reference_user,
            9_999,
            &[
                foreign_key_definition.clone(),
                foreign_key_object.clone(),
                add_edge.clone(),
                source_table_add,
                target_table_add,
            ],
            &[alter_reference_audit],
        )?,
        3
    );
    assert!(matches!(
        stale_source_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(source_table_id, stale_source_row))?.is_none());
    assert_eq!(
        lookup_outcome(
            &database,
            stale_source_invocation,
            alter_and_reference_user,
            1
        )?
        .ok_or("missing stale source-table FK writer outcome")?
        .status,
        OutcomeStatus::Aborted
    );
    assert!(matches!(
        stale_target_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(target_table_id, stale_target_row))?.is_none());
    assert_eq!(
        lookup_outcome(
            &database,
            stale_target_invocation,
            alter_and_reference_user,
            1
        )?
        .ok_or("missing target-table FK writer outcome")?
        .status,
        OutcomeStatus::Aborted
    );

    for (invocation_id, column_id) in [(id(0x7a), target_column_id), (id(0x7c), id(84))] {
        let mut drop_audit = identity_audit_event(invocation_id[0], ddl_admin, column_id);
        drop_audit.correlation_id = invocation_id;
        assert_eq!(
            storage.drop_column(
                invocation_id,
                ddl_admin,
                if column_id == target_column_id {
                    target_table_id
                } else {
                    source_table_id
                },
                column_id,
                9_999,
                drop_audit,
            ),
            Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
                CommitError::ColumnHasDependencies
            ))
        );
        assert_eq!(
            lookup_outcome(&database, invocation_id, ddl_admin, 1)?
                .ok_or("missing FK-restricted column-drop outcome")?
                .status,
            OutcomeStatus::Aborted
        );
    }
    let restricted_table_drop = id(0x7f);
    let mut restricted_table_drop_audit = identity_audit_event(0x7f, ddl_admin, target_table_id);
    restricted_table_drop_audit.correlation_id = restricted_table_drop;
    assert_eq!(
        storage.drop_table(
            restricted_table_drop,
            ddl_admin,
            target_table_id,
            9_999,
            restricted_table_drop_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::TableHasDependencies
        ))
    );
    assert_eq!(
        lookup_outcome(&database, restricted_table_drop, ddl_admin, 1)?
            .ok_or("missing FK-restricted table-drop outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        assert!(catalog.get(foreign_key_key.as_slice())?.is_some());
        assert!(
            catalog
                .get(id_catalog_key(5, target_table_id).as_slice())?
                .is_some()
        );
    }

    let stale_source_drop_invocation = id(0x80);
    let stale_source_drop_row = id(0x82);
    let mut stale_source_drop_writer = storage.begin_invocation(
        stale_source_drop_invocation,
        alter_and_reference_user,
        9_999,
        1,
        1024,
    )?;
    let stale_source_drop_schema =
        stale_source_drop_writer.load_catalog_table_schema(source_table_id)?;
    stale_source_drop_writer.load_catalog_indexes(&stale_source_drop_schema)?;
    let stale_source_drop_tuple = encode_tuple(
        &stale_source_drop_schema.columns,
        stale_source_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"before foreign key drop".to_vec(),
        ))],
    )?;
    stale_source_drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: source_table_id,
            row: stale_source_drop_row,
        },
        &stale_source_drop_tuple,
    )?;
    let stale_target_drop_invocation = id(0x81);
    let stale_target_drop_row = id(0x83);
    let mut stale_target_drop_writer = storage.begin_invocation(
        stale_target_drop_invocation,
        alter_and_reference_user,
        9_999,
        1,
        1024,
    )?;
    let stale_target_drop_schema =
        stale_target_drop_writer.load_catalog_table_schema(target_table_id)?;
    stale_target_drop_writer.load_catalog_indexes(&stale_target_drop_schema)?;
    let stale_target_drop_tuple = encode_tuple(
        &stale_target_drop_schema.columns,
        stale_target_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"before foreign key drop".to_vec(),
        ))],
    )?;
    stale_target_drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: target_table_id,
            row: stale_target_drop_row,
        },
        &stale_target_drop_tuple,
    )?;

    let mut removed_foreign_key = foreign_key_definition.clone();
    removed_foreign_key.expected_value = foreign_key_definition.payload.clone();
    removed_foreign_key.payload = None;
    let mut removed_object = foreign_key_object.clone();
    removed_object.expected_value = foreign_key_object.payload.clone();
    removed_object.payload = None;
    let mut removed_edge = add_edge.clone();
    removed_edge.expected_value = add_edge.payload.clone();
    removed_edge.payload = None;
    let mut source_table_drop = table_write_with_versions(source_table_id, schema_id, 1, 3);
    source_table_drop.expected_value =
        table_write_with_versions(source_table_id, schema_id, 1, 2).payload;
    let mut target_table_drop = table_write_with_versions(target_table_id, schema_id, 1, 3);
    target_table_drop.expected_value =
        table_write_with_versions(target_table_id, schema_id, 1, 2).payload;
    let mut alter_only_drop_audit = audit_event(0x73);
    alter_only_drop_audit.actor_id = Some(alter_only_user);
    assert_eq!(
        storage.commit_authorized_foreign_key_ddl(
            id(0x73),
            alter_only_user,
            9_999,
            &[
                removed_foreign_key,
                removed_object,
                removed_edge,
                source_table_drop,
                target_table_drop,
            ],
            &[alter_only_drop_audit],
        )?,
        4
    );
    for (writer, invocation_id, table_id, row_id, label) in [
        (
            stale_source_drop_writer,
            stale_source_drop_invocation,
            source_table_id,
            stale_source_drop_row,
            "source",
        ),
        (
            stale_target_drop_writer,
            stale_target_drop_invocation,
            target_table_id,
            stale_target_drop_row,
            "target",
        ),
    ] {
        assert!(matches!(
            writer.commit(),
            Err(prdb_engine::transaction::TransactionError::Commit(
                CommitError::Conflict
            ))
        ));
        assert!(row(&database, &table_row_key(table_id, row_id))?.is_none());
        assert_eq!(
            lookup_outcome(&database, invocation_id, alter_and_reference_user, 1)?
                .ok_or_else(|| format!("missing stale {label}-table FK-drop outcome"))?
                .status,
            OutcomeStatus::Aborted
        );
    }

    let mut reference_only_audit = audit_event(0x74);
    reference_only_audit.actor_id = Some(reference_only_user);
    let mut source_table_reference_only =
        table_write_with_versions(source_table_id, schema_id, 1, 4);
    source_table_reference_only.expected_value =
        table_write_with_versions(source_table_id, schema_id, 1, 3).payload;
    let mut target_table_reference_only =
        table_write_with_versions(target_table_id, schema_id, 1, 4);
    target_table_reference_only.expected_value =
        table_write_with_versions(target_table_id, schema_id, 1, 3).payload;
    assert_eq!(
        storage.commit_authorized_foreign_key_ddl(
            id(0x74),
            reference_only_user,
            9_999,
            &[
                foreign_key_definition,
                foreign_key_object,
                add_edge.clone(),
                source_table_reference_only,
                target_table_reference_only,
            ],
            &[reference_only_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    assert_eq!(commit_sequence(&database)?, 4);
    for (invocation_id, principal_id, status) in [
        (id(0x71), alter_only_user, OutcomeStatus::Aborted),
        (id(0x72), alter_and_reference_user, OutcomeStatus::Committed),
        (id(0x73), alter_only_user, OutcomeStatus::Committed),
        (id(0x74), reference_only_user, OutcomeStatus::Aborted),
        (
            stale_source_drop_invocation,
            alter_and_reference_user,
            OutcomeStatus::Aborted,
        ),
        (
            stale_target_drop_invocation,
            alter_and_reference_user,
            OutcomeStatus::Aborted,
        ),
        (restricted_table_drop, ddl_admin, OutcomeStatus::Aborted),
        (
            stale_source_invocation,
            alter_and_reference_user,
            OutcomeStatus::Aborted,
        ),
        (
            stale_target_invocation,
            alter_and_reference_user,
            OutcomeStatus::Aborted,
        ),
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
    assert!(catalog.get(foreign_key_key.as_slice())?.is_none());
    assert!(catalog.get(foreign_key_object_key.as_slice())?.is_none());
    assert!(catalog.get(add_edge.key.as_slice())?.is_none());
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[0x71; 16][..])?
            .is_none()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[0x74; 16][..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn storage_manager_rejects_unique_index_backfill_with_duplicate_values()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let payload = bytea_tuple(b"duplicate");
    let row_keys = [table_row_key(id(16), id(71)), table_row_key(id(16), id(72))];
    let old_indexes = row_keys
        .iter()
        .map(|row_key| {
            let derived = typed_bytea_index_key(id(0x44), row_key, &payload);
            IndexWrite {
                key: derived.key,
                row_key: *row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }
        })
        .collect::<Vec<_>>();
    commit_rows_and_indexes(
        &database,
        id(78),
        id(9),
        9_999,
        &row_keys
            .iter()
            .map(|row_key| RowWrite {
                key: *row_key,
                expected_version: None,
                payload: Some(payload.clone()),
            })
            .collect::<Vec<_>>(),
        &old_indexes,
    )?;

    let (unique_key, unique_payload) =
        index_definition_with_options(id(0x89), id(16), &[id(0x66)], true);
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(79),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: unique_key.clone(),
                    expected_value: None,
                    payload: Some(unique_payload),
                },
                object_write(id(0x89), 7),
            ],
            &[audit_event(79)],
        ),
        Err(CommitError::UniqueViolation)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(79), id(9), 1)?.unwrap().status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(unique_key.as_slice())?
            .is_none()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[79; 16][..])?
            .is_none()
    );
    assert!(
        read.open_table(schema::INDEXES)?.iter()?.all(|entry| !entry
            .unwrap()
            .0
            .value()
            .starts_with(&id(0x89)))
    );
    Ok(())
}

pub(super) fn catalog_index_backfill_limit_fails_without_publishing_catalog()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let payload = bytea_tuple(b"bounded");
    let rows = (1u64..=65_537)
        .map(|row_id| {
            let mut row_suffix = [0; 16];
            row_suffix[8..].copy_from_slice(&row_id.to_be_bytes());
            RowWrite {
                key: table_row_key(id(16), row_suffix),
                expected_version: None,
                payload: Some(payload.clone()),
            }
        })
        .collect::<Vec<_>>();
    commit_rows(&database, id(80), id(9), 9_999, &rows)?;

    let (index_key, index_payload) = index_definition(id(0x8a), id(16));
    let ddl_fences = DdlFenceManager::new();
    let coordinator = DdlFenceCoordinator::new(ddl_fences, Duration::from_secs(1));
    assert_eq!(
        commit_catalog_and_audit_with_fences(
            &database,
            &coordinator,
            id(81),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: index_key.clone(),
                    expected_value: None,
                    payload: Some(index_payload),
                },
                object_write(id(0x8a), 7),
            ],
            &[audit_event(81)],
        ),
        Err(CommitError::IndexDdlLimitExceeded)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(81), id(9), 1)?.unwrap().status,
        OutcomeStatus::Aborted
    );
    assert!(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(index_key.as_slice())?
            .is_none()
    );
    Ok(())
}

pub(super) fn storage_manager_add_column_row_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AddTableColumnRequest, CreateTableRequest, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x8b);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x8c, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x8c),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "column_limit_owner"),
            object_write_with_name(actor, 8, "column_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x8d);
    let mut schema_audit = identity_audit_event(0x8e, actor, id(0x8f));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_limit_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x90);
    let mut create_audit = identity_audit_event(0x91, actor, id(0x92));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "bounded_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let old_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        }],
        1,
        &[Some(ScalarValue::Integer(1))],
    )?;
    let rows = (1u64..=65_537)
        .map(|row_number| {
            let mut row_suffix = [0; 16];
            row_suffix[8..].copy_from_slice(&row_number.to_be_bytes());
            RowWrite {
                key: table_row_key(table.table_id, row_suffix),
                expected_version: None,
                payload: Some(old_tuple.clone()),
            }
        })
        .collect::<Vec<_>>();
    let rows_invocation = id(0x93);
    let before_add = commit_rows(&database, rows_invocation, actor, 9_999, &rows)?;
    let add_invocation = id(0x94);
    let mut add_audit = identity_audit_event(0x95, actor, id(0x96));
    add_audit.correlation_id = add_invocation;
    assert_eq!(
        storage.add_table_column(AddTableColumnRequest {
            invocation_id: add_invocation,
            authenticated_actor_id: actor,
            table_id: table.table_id,
            column: TableColumnDefinition {
                name: "extra".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            retention_deadline_us: 9_999,
            audit_event: add_audit,
        }),
        Err(TableLifecycleError::Commit(
            CommitError::ColumnAddLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, before_add);
    assert_eq!(
        lookup_outcome(&database, add_invocation, actor, 1)?
            .ok_or("missing add-column limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let table_record = decode_registered_envelope(
        read.open_table(CATALOG)?
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing after add-column limit")?
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
    let mut scanned_columns = 0;
    for entry in read
        .open_table(CATALOG)?
        .range(6u16.to_be_bytes().as_slice()..)?
    {
        let (key, value) = entry?;
        let key = key.value();
        if !key.starts_with(&6u16.to_be_bytes()) {
            break;
        }
        let record = decode_registered_envelope(value.value())?;
        if record
            .fields
            .iter()
            .any(|field| field.tag == 1 && field.payload.as_slice() == table.table_id)
        {
            scanned_columns += 1;
        }
    }
    assert_eq!(scanned_columns, 1);
    drop(read);

    let mut last_row_suffix = [0; 16];
    last_row_suffix[8..].copy_from_slice(&65_537u64.to_be_bytes());
    let last_row_key = table_row_key(table.table_id, last_row_suffix);
    let remove_last_row_invocation = id(0x97);
    assert_eq!(
        commit_rows(
            &database,
            remove_last_row_invocation,
            actor,
            9_999,
            &[RowWrite {
                key: last_row_key,
                expected_version: Some(before_add),
                payload: None,
            }],
        )?,
        before_add + 1
    );
    let boundary_invocation = id(0x98);
    let mut boundary_audit = identity_audit_event(0x99, actor, id(0x9a));
    boundary_audit.correlation_id = boundary_invocation;
    let boundary_add = storage.add_table_column(AddTableColumnRequest {
        invocation_id: boundary_invocation,
        authenticated_actor_id: actor,
        table_id: table.table_id,
        column: TableColumnDefinition {
            name: "extra".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        },
        retention_deadline_us: 9_999,
        audit_event: boundary_audit,
    })?;
    assert_eq!(boundary_add.schema_version, 2);
    assert_eq!(boundary_add.commit_sequence, before_add + 2);
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact-bound add-column outcome")?
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_drop_column_row_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x8b);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x8c, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x8c),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "column_drop_limit_owner"),
            object_write_with_name(actor, 8, "column_drop_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x8d);
    let mut schema_audit = identity_audit_event(0x8e, actor, id(0x8f));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_drop_limit_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x90);
    let mut create_audit = identity_audit_event(0x91, actor, id(0x92));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "bounded_drop_table".to_owned(),
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
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let old_tuple = encode_tuple(
        &[
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
        ],
        1,
        &[Some(ScalarValue::Integer(1)), Some(ScalarValue::Integer(2))],
    )?;
    let rows = (1u64..=65_537)
        .map(|row_number| {
            let mut row_suffix = [0; 16];
            row_suffix[8..].copy_from_slice(&row_number.to_be_bytes());
            RowWrite {
                key: table_row_key(table.table_id, row_suffix),
                expected_version: None,
                payload: Some(old_tuple.clone()),
            }
        })
        .collect::<Vec<_>>();
    let rows_invocation = id(0x93);
    let before_drop = commit_rows(&database, rows_invocation, actor, 9_999, &rows)?;
    let drop_invocation = id(0x94);
    let mut drop_audit = identity_audit_event(0x95, actor, id(0x96));
    drop_audit.correlation_id = drop_invocation;
    assert_eq!(
        storage.drop_column(
            drop_invocation,
            actor,
            table.table_id,
            table.column_ids[0],
            9_999,
            drop_audit,
        ),
        Err(TableLifecycleError::Commit(
            CommitError::TableDropLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, before_drop);
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing DROP COLUMN limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let mut last_row_suffix = [0; 16];
    last_row_suffix[8..].copy_from_slice(&65_537u64.to_be_bytes());
    let last_row_key = table_row_key(table.table_id, last_row_suffix);
    assert_eq!(
        commit_rows(
            &database,
            id(0x97),
            actor,
            9_999,
            &[RowWrite {
                key: last_row_key,
                expected_version: Some(before_drop),
                payload: None,
            }],
        )?,
        before_drop + 1
    );
    let boundary_invocation = id(0x98);
    let mut boundary_audit = identity_audit_event(0x99, actor, id(0x9a));
    boundary_audit.correlation_id = boundary_invocation;
    let dropped = storage.drop_column(
        boundary_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        9_999,
        boundary_audit,
    )?;
    assert_eq!(dropped.column_id, table.column_ids[0]);
    assert_eq!(dropped.commit_sequence, before_drop + 2);
    let mut first_row_suffix = [0; 16];
    first_row_suffix[8..].copy_from_slice(&1u64.to_be_bytes());
    let first_row_key = table_row_key(table.table_id, first_row_suffix);
    let remaining_row =
        row(&database, &first_row_key)?.ok_or("row missing after exact-bound DROP COLUMN")?;
    let decoded = decode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        }],
        2,
        &remaining_row.payload,
    )?;
    assert_eq!(decoded.values, vec![Some(ScalarValue::Integer(2))]);
    let table_record = decode_registered_envelope(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing after exact-bound DROP COLUMN")?
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
        2
    );
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact-bound DROP COLUMN outcome")?
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}
