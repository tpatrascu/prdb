use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::catalog_index::{
    lookup_dependencies_for_dependent, lookup_dependents_for_referenced, lookup_grants_for_grantee,
    lookup_grants_for_object, lookup_index_id_by_name, lookup_members_for_role,
    lookup_relation_id_by_name, lookup_roles_for_member, lookup_schema_id_by_name,
};
use prdb_engine::commit::{CatalogWrite, CommitError, commit_catalog_and_audit};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::schema::CATALOG;
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ScalarType, ScalarValue};
use redb::ReadableDatabase;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn schema_create_drop_pressure_preserves_pinned_name_index_snapshots()
-> Result<(), Box<dyn std::error::Error>> {
    const SCHEMA_COUNT: usize = 128;
    const READER_ROUNDS: usize = 256;

    let pressure_id = |namespace: u8, index: usize| {
        let mut value = [namespace; 16];
        value[8..].copy_from_slice(&(index as u64).to_be_bytes());
        value
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x41);
    let membership_member = id(0x42);
    let pressure_role_id = pressure_id(0x45, 0);
    let admin_role = builtin_owner_id();
    let setup = [
        principal_write_with_name(actor, 1, 1, "dictionary_pressure_actor"),
        object_write_with_name(actor, 8, "dictionary_pressure_actor"),
        principal_write_with_name(membership_member, 1, 1, "dictionary_pressure_member"),
        object_write_with_name(membership_member, 8, "dictionary_pressure_member"),
        principal_write_with_name(pressure_role_id, 2, 1, "dictionary_pressure_role"),
        object_write_with_name(pressure_role_id, 9, "dictionary_pressure_role"),
        grant_write(actor, id(0x33), 14, 1, id(0x33), admin_role),
        dependency_write(actor, id(0x33), 5),
    ];
    let mut setup_audit = identity_audit_event(0xf1, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xf1), admin_role, 9_999, &setup, &[setup_audit])?;

    let target_schema_invocation = pressure_id(0xa0, 0);
    let mut target_schema_audit = identity_audit_event(0xa1, actor, id(0));
    target_schema_audit.correlation_id = target_schema_invocation;
    let target_schema = storage.create_schema(
        target_schema_invocation,
        actor,
        "dictionary_pressure_fk_target",
        actor,
        9_999,
        target_schema_audit,
    )?;
    let target_table_invocation = pressure_id(0xa2, 0);
    let mut target_table_audit = identity_audit_event(0xa3, actor, target_schema.schema_id);
    target_table_audit.correlation_id = target_table_invocation;
    let target_table = storage.create_table(prdb_engine::storage_manager::CreateTableRequest {
        invocation_id: target_table_invocation,
        authenticated_actor_id: actor,
        schema_id: target_schema.schema_id,
        name: "pressure_parent".to_owned(),
        columns: vec![prdb_engine::storage_manager::TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: target_table_audit,
    })?;
    let target_constraint_invocation = pressure_id(0xa4, 0);
    let mut target_constraint_audit = identity_audit_event(0xa5, actor, target_table.table_id);
    target_constraint_audit.correlation_id = target_constraint_invocation;
    let target_constraint = storage.create_unique_constraint(
        prdb_engine::storage_manager::CreateUniqueConstraintRequest {
            invocation_id: target_constraint_invocation,
            authenticated_actor_id: actor,
            table_id: target_table.table_id,
            column_ids: target_table.column_ids.clone(),
            retention_deadline_us: 9_999,
            audit_event: target_constraint_audit,
        },
    )?;
    let references_grant_invocation = pressure_id(0xa6, 0);
    let mut references_grant_audit = identity_audit_event(0xa7, actor, target_table.table_id);
    references_grant_audit.correlation_id = references_grant_invocation;
    references_grant_audit.action = AuditAction::AuthorityChange;
    storage.set_object_grant(prdb_engine::storage_manager::SetObjectGrantRequest {
        invocation_id: references_grant_invocation,
        authenticated_actor_id: actor,
        grantee_id: actor,
        object_id: target_table.table_id,
        privilege: prdb_engine::storage_manager::ObjectGrantPrivilege::References,
        target: prdb_engine::storage_manager::ObjectGrantTarget::Object,
        grant: true,
        retention_deadline_us: 9_999,
        audit_event: references_grant_audit,
    })?;
    let membership_epoch_update = |next_epoch: u64| -> Result<_, Box<dyn std::error::Error>> {
        let mut write = principal_write_with_name(
            membership_member,
            1,
            next_epoch,
            "dictionary_pressure_member",
        );
        let read = database.begin_read()?;
        write.expected_value = Some(
            read.open_table(CATALOG)?
                .get(id_catalog_key(10, membership_member).as_slice())?
                .ok_or("membership pressure principal disappeared")?
                .value()
                .to_vec(),
        );
        Ok(write)
    };
    let mut membership_member_epoch = 1u64;

    let names = (0..SCHEMA_COUNT)
        .map(|index| format!("dictionary_pressure_{index:03}"))
        .collect::<Vec<_>>();
    let retained_snapshot = database.begin_read()?;
    for name in &names {
        assert_eq!(lookup_schema_id_by_name(&retained_snapshot, name)?, None);
    }

    let start = Arc::new(Barrier::new(2));
    let reader_names = names.clone();
    let reader_database = &database;
    let reader_start = Arc::clone(&start);
    let reader = std::thread::scope(|scope| {
        let reader = scope.spawn(move || {
            reader_start.wait();
            let mut observations = 0;
            for _ in 0..READER_ROUNDS {
                let snapshot = reader_database
                    .begin_read()
                    .expect("reader snapshot should open during schema DDL");
                for name in &reader_names {
                    if let Some(schema_id) = lookup_schema_id_by_name(&snapshot, name)
                        .expect("schema name lookup should succeed during DDL")
                    {
                        assert_ne!(schema_id, [0; 16]);
                    }
                }
                observations += 1;
            }
            observations
        });

        start.wait();
        for (index, name) in names.iter().enumerate() {
            let create_id = pressure_id(0x40, index);
            let mut create_audit = identity_audit_event(index as u8, actor, id(0));
            create_audit.event_id = pressure_id(0x41, index);
            create_audit.correlation_id = create_id;
            let created =
                storage.create_schema(create_id, actor, name, actor, 9_999, create_audit)?;
            {
                let snapshot = database.begin_read()?;
                assert_eq!(
                    lookup_schema_id_by_name(&snapshot, name)?,
                    Some(created.schema_id)
                );
            }

            let table_create_invocation = pressure_id(0xc0, index);
            let mut table_create_audit =
                identity_audit_event(index as u8, actor, created.schema_id);
            table_create_audit.event_id = pressure_id(0xc2, index);
            table_create_audit.correlation_id = table_create_invocation;
            let table = storage.create_table(prdb_engine::storage_manager::CreateTableRequest {
                invocation_id: table_create_invocation,
                authenticated_actor_id: actor,
                schema_id: created.schema_id,
                name: "pressure_table".to_owned(),
                columns: vec![prdb_engine::storage_manager::TableColumnDefinition {
                    name: "value".to_owned(),
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                }],
                retention_deadline_us: 9_999,
                audit_event: table_create_audit,
            })?;
            let membership_grant_invocation = pressure_id(0xb4, index);
            let mut membership_grant_audit =
                identity_audit_event(index as u8, actor, pressure_role_id);
            membership_grant_audit.event_id = pressure_id(0xb5, index);
            membership_grant_audit.correlation_id = membership_grant_invocation;
            membership_grant_audit.action = AuditAction::AuthorityChange;
            storage
                .commit_catalog_and_audit(
                    membership_grant_invocation,
                    actor,
                    9_999,
                    &[
                        membership_write_with_grantor(membership_member, pressure_role_id, actor),
                        dependency_write(membership_member, pressure_role_id, 5),
                        membership_epoch_update(membership_member_epoch + 1)?,
                    ],
                    &[membership_grant_audit],
                )
                .map_err(|error| format!("pressure membership grant commit failed: {error:?}"))?;
            membership_member_epoch += 1;
            let retained_membership_snapshot = database.begin_read()?;
            assert!(
                prdb_engine::catalog_index::lookup_roles_for_member(
                    &retained_membership_snapshot,
                    membership_member,
                )?
                .contains(&pressure_role_id)
            );
            assert!(
                prdb_engine::catalog_index::lookup_members_for_role(
                    &retained_membership_snapshot,
                    pressure_role_id,
                )?
                .contains(&membership_member)
            );
            let mut membership_remove =
                membership_write_with_grantor(membership_member, pressure_role_id, actor);
            let mut dependency_remove = dependency_write(membership_member, pressure_role_id, 5);
            {
                let current = database.begin_read()?;
                let catalog = current.open_table(CATALOG)?;
                membership_remove.expected_value = Some(
                    catalog
                        .get(membership_remove.key.as_slice())?
                        .ok_or("membership disappeared before pressure revoke")?
                        .value()
                        .to_vec(),
                );
                membership_remove.payload = None;
                dependency_remove.expected_value = Some(
                    catalog
                        .get(dependency_remove.key.as_slice())?
                        .ok_or("membership dependency disappeared before pressure revoke")?
                        .value()
                        .to_vec(),
                );
                dependency_remove.payload = None;
            }
            let membership_revoke_invocation = pressure_id(0xb6, index);
            let mut membership_revoke_audit =
                identity_audit_event(index as u8, actor, pressure_role_id);
            membership_revoke_audit.event_id = pressure_id(0xb7, index);
            membership_revoke_audit.correlation_id = membership_revoke_invocation;
            membership_revoke_audit.action = AuditAction::AuthorityChange;
            storage.commit_catalog_and_audit(
                membership_revoke_invocation,
                actor,
                9_999,
                &[
                    membership_remove,
                    dependency_remove,
                    membership_epoch_update(membership_member_epoch + 1)?,
                ],
                &[membership_revoke_audit],
            )?;
            membership_member_epoch += 1;
            assert!(
                prdb_engine::catalog_index::lookup_roles_for_member(
                    &retained_membership_snapshot,
                    membership_member,
                )?
                .contains(&pressure_role_id)
            );
            assert!(
                prdb_engine::catalog_index::lookup_members_for_role(
                    &retained_membership_snapshot,
                    pressure_role_id,
                )?
                .contains(&membership_member)
            );
            drop(retained_membership_snapshot);
            let after_membership_revoke = database.begin_read()?;
            assert!(
                !prdb_engine::catalog_index::lookup_roles_for_member(
                    &after_membership_revoke,
                    membership_member,
                )?
                .contains(&pressure_role_id)
            );
            assert!(
                !prdb_engine::catalog_index::lookup_members_for_role(
                    &after_membership_revoke,
                    pressure_role_id,
                )?
                .contains(&membership_member)
            );
            let grant_create_invocation = pressure_id(0xb0, index);
            let mut grant_create_audit = identity_audit_event(index as u8, actor, table.table_id);
            grant_create_audit.event_id = pressure_id(0xb1, index);
            grant_create_audit.correlation_id = grant_create_invocation;
            grant_create_audit.action = AuditAction::AuthorityChange;
            storage.set_object_grant(prdb_engine::storage_manager::SetObjectGrantRequest {
                invocation_id: grant_create_invocation,
                authenticated_actor_id: actor,
                grantee_id: actor,
                object_id: table.table_id,
                privilege: prdb_engine::storage_manager::ObjectGrantPrivilege::Select,
                target: prdb_engine::storage_manager::ObjectGrantTarget::Object,
                grant: true,
                retention_deadline_us: 9_999,
                audit_event: grant_create_audit,
            })?;
            let retained_grant_snapshot = database.begin_read()?;
            assert_eq!(
                prdb_engine::catalog_index::lookup_grants_for_object(
                    &retained_grant_snapshot,
                    table.table_id,
                )?
                .len(),
                1,
                "the object-grant index should resolve a fresh table grant"
            );
            let grant_drop_invocation = pressure_id(0xb2, index);
            let mut grant_drop_audit = identity_audit_event(index as u8, actor, table.table_id);
            grant_drop_audit.event_id = pressure_id(0xb3, index);
            grant_drop_audit.correlation_id = grant_drop_invocation;
            grant_drop_audit.action = AuditAction::AuthorityChange;
            storage.set_object_grant(prdb_engine::storage_manager::SetObjectGrantRequest {
                invocation_id: grant_drop_invocation,
                authenticated_actor_id: actor,
                grantee_id: actor,
                object_id: table.table_id,
                privilege: prdb_engine::storage_manager::ObjectGrantPrivilege::Select,
                target: prdb_engine::storage_manager::ObjectGrantTarget::Object,
                grant: false,
                retention_deadline_us: 9_999,
                audit_event: grant_drop_audit,
            })?;
            assert_eq!(
                prdb_engine::catalog_index::lookup_grants_for_object(
                    &retained_grant_snapshot,
                    table.table_id,
                )?
                .len(),
                1,
                "a retained snapshot must keep the revoked table grant"
            );
            drop(retained_grant_snapshot);
            let after_grant_revoke = database.begin_read()?;
            assert!(
                prdb_engine::catalog_index::lookup_grants_for_object(
                    &after_grant_revoke,
                    table.table_id,
                )?
                .is_empty(),
                "a fresh snapshot must omit the revoked table grant"
            );
            let index_create_invocation = pressure_id(0xd0, index);
            let mut index_create_audit = identity_audit_event(index as u8, actor, table.table_id);
            index_create_audit.event_id = pressure_id(0xd1, index);
            index_create_audit.correlation_id = index_create_invocation;
            let created_index =
                storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
                    invocation_id: index_create_invocation,
                    authenticated_actor_id: actor,
                    table_id: table.table_id,
                    name: "pressure_value_idx".to_owned(),
                    column_ids: table.column_ids.clone(),
                    unique: false,
                    retention_deadline_us: 9_999,
                    audit_event: index_create_audit,
                })?;
            let retained_index_snapshot = database.begin_read()?;
            assert_eq!(
                lookup_index_id_by_name(
                    &retained_index_snapshot,
                    created.schema_id,
                    "pressure_value_idx",
                )?,
                Some(created_index.index_id)
            );
            assert_eq!(
                prdb_engine::catalog_index::scan_index_ids_for_table(
                    &retained_index_snapshot,
                    table.table_id,
                    1,
                )?,
                vec![created_index.index_id]
            );

            let index_drop_invocation = pressure_id(0xd2, index);
            let mut index_drop_audit =
                identity_audit_event(index as u8, actor, created_index.index_id);
            index_drop_audit.event_id = pressure_id(0xd3, index);
            index_drop_audit.correlation_id = index_drop_invocation;
            storage.drop_index(prdb_engine::storage_manager::DropIndexRequest {
                invocation_id: index_drop_invocation,
                authenticated_actor_id: actor,
                index_id: created_index.index_id,
                retention_deadline_us: 9_999,
                audit_event: index_drop_audit,
            })?;
            assert_eq!(
                lookup_index_id_by_name(
                    &retained_index_snapshot,
                    created.schema_id,
                    "pressure_value_idx",
                )?,
                Some(created_index.index_id),
                "a pinned dictionary snapshot must retain the dropped index name"
            );
            assert_eq!(
                prdb_engine::catalog_index::scan_index_ids_for_table(
                    &retained_index_snapshot,
                    table.table_id,
                    1,
                )?,
                vec![created_index.index_id]
            );
            drop(retained_index_snapshot);
            let after_index_drop = database.begin_read()?;
            assert_eq!(
                lookup_index_id_by_name(
                    &after_index_drop,
                    created.schema_id,
                    "pressure_value_idx",
                )?,
                None
            );
            assert!(
                prdb_engine::catalog_index::scan_index_ids_for_table(
                    &after_index_drop,
                    table.table_id,
                    1,
                )?
                .is_empty()
            );

            let policy_create_invocation = pressure_id(0xe0, index);
            let mut policy_create_audit = identity_audit_event(index as u8, actor, table.table_id);
            policy_create_audit.event_id = pressure_id(0xe1, index);
            policy_create_audit.correlation_id = policy_create_invocation;
            let created_policy =
                storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
                    invocation_id: policy_create_invocation,
                    authenticated_actor_id: actor,
                    table_id: table.table_id,
                    name: "pressure_select_policy".to_owned(),
                    kind: prdb_engine::storage_manager::PolicyKind::Row,
                    operation: prdb_engine::authorization::Operation::Select,
                    effect: prdb_engine::authorization::Effect::Allow,
                    recipients: vec![prdb_engine::authorization::Subject::Principal(actor)],
                    target_column_ids: Vec::new(),
                    using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                        ScalarValue::Boolean(true),
                    ))),
                    with_check_expression: None,
                    retention_deadline_us: 9_999,
                    audit_event: policy_create_audit,
                })?;
            let retained_policy_snapshot = database.begin_read()?;
            let retained_policies =
                prdb_engine::catalog_index::lookup_policies_for_table_operation(
                    &retained_policy_snapshot,
                    table.table_id,
                    1,
                )?;
            assert!(retained_policies.iter().any(|record| {
                record.logical_key == id_catalog_key(13, created_policy.policy_id)
            }));
            let policy_drop_invocation = pressure_id(0xe2, index);
            let mut policy_drop_audit =
                identity_audit_event(index as u8, actor, created_policy.policy_id);
            policy_drop_audit.event_id = pressure_id(0xe3, index);
            policy_drop_audit.correlation_id = policy_drop_invocation;
            storage.drop_policy(prdb_engine::storage_manager::DropPolicyRequest {
                invocation_id: policy_drop_invocation,
                authenticated_actor_id: actor,
                policy_id: created_policy.policy_id,
                retention_deadline_us: 9_999,
                audit_event: policy_drop_audit,
            })?;
            assert!(
                prdb_engine::catalog_index::lookup_policies_for_table_operation(
                    &retained_policy_snapshot,
                    table.table_id,
                    1,
                )?
                .iter()
                .any(|record| record.logical_key == id_catalog_key(13, created_policy.policy_id)),
                "a pinned snapshot must retain the dropped policy index entry"
            );
            drop(retained_policy_snapshot);
            let after_policy_drop = database.begin_read()?;
            assert!(
                prdb_engine::catalog_index::lookup_policies_for_table_operation(
                    &after_policy_drop,
                    table.table_id,
                    1,
                )?
                .is_empty(),
                "a fresh snapshot must omit the dropped policy index entry"
            );

            let constraint_create_invocation = pressure_id(0xf0, index);
            let mut constraint_create_audit =
                identity_audit_event(index as u8, actor, table.table_id);
            constraint_create_audit.event_id = pressure_id(0xf1, index);
            constraint_create_audit.correlation_id = constraint_create_invocation;
            let created_constraint = storage.create_unique_constraint(
                prdb_engine::storage_manager::CreateUniqueConstraintRequest {
                    invocation_id: constraint_create_invocation,
                    authenticated_actor_id: actor,
                    table_id: table.table_id,
                    column_ids: table.column_ids.clone(),
                    retention_deadline_us: 9_999,
                    audit_event: constraint_create_audit,
                },
            )?;
            let foreign_key_create_invocation = pressure_id(0xf4, index);
            let mut foreign_key_create_audit =
                identity_audit_event(index as u8, actor, table.table_id);
            foreign_key_create_audit.event_id = pressure_id(0xf5, index);
            foreign_key_create_audit.correlation_id = foreign_key_create_invocation;
            let foreign_key = storage.create_foreign_key(
                prdb_engine::storage_manager::CreateForeignKeyRequest {
                    invocation_id: foreign_key_create_invocation,
                    authenticated_actor_id: actor,
                    source_table_id: table.table_id,
                    source_column_ids: table.column_ids.clone(),
                    target_constraint_id: target_constraint.constraint_id,
                    retention_deadline_us: 9_999,
                    audit_event: foreign_key_create_audit,
                },
            )?;
            let retained_constraint_snapshot = database.begin_read()?;
            let mut expected_constraints =
                vec![created_constraint.constraint_id, foreign_key.foreign_key_id];
            expected_constraints.sort_unstable();
            let foreign_key_dependency_key = dependency_write(
                foreign_key.foreign_key_id,
                target_constraint.constraint_id,
                3,
            )
            .key;
            assert_eq!(
                prdb_engine::catalog_index::scan_constraint_ids_for_table(
                    &retained_constraint_snapshot,
                    table.table_id,
                    8,
                )?,
                expected_constraints
            );
            assert!(
                prdb_engine::catalog_index::lookup_dependents_for_referenced(
                    &retained_constraint_snapshot,
                    target_constraint.constraint_id,
                )?
                .iter()
                .any(|record| record.logical_key == foreign_key_dependency_key)
            );
            assert_eq!(
                prdb_engine::catalog_index::scan_index_ids_for_table(
                    &retained_constraint_snapshot,
                    table.table_id,
                    8,
                )?,
                vec![created_constraint.index_id]
            );
            let constraint_drop_invocation = pressure_id(0xf2, index);
            let foreign_key_drop_invocation = pressure_id(0xf6, index);
            let mut foreign_key_drop_audit =
                identity_audit_event(index as u8, actor, foreign_key.foreign_key_id);
            foreign_key_drop_audit.event_id = pressure_id(0xf7, index);
            foreign_key_drop_audit.correlation_id = foreign_key_drop_invocation;
            storage.drop_foreign_key(
                foreign_key_drop_invocation,
                actor,
                foreign_key.foreign_key_id,
                9_999,
                foreign_key_drop_audit,
            )?;
            assert_eq!(
                prdb_engine::catalog_index::scan_constraint_ids_for_table(
                    &retained_constraint_snapshot,
                    table.table_id,
                    8,
                )?,
                expected_constraints,
                "a pinned snapshot must retain the dropped foreign key"
            );
            assert!(
                prdb_engine::catalog_index::lookup_dependents_for_referenced(
                    &retained_constraint_snapshot,
                    target_constraint.constraint_id,
                )?
                .iter()
                .any(|record| record.logical_key == foreign_key_dependency_key)
            );
            let after_foreign_key_drop = database.begin_read()?;
            assert_eq!(
                prdb_engine::catalog_index::scan_constraint_ids_for_table(
                    &after_foreign_key_drop,
                    table.table_id,
                    8,
                )?,
                vec![created_constraint.constraint_id]
            );
            assert!(
                !prdb_engine::catalog_index::lookup_dependents_for_referenced(
                    &after_foreign_key_drop,
                    target_constraint.constraint_id,
                )?
                .iter()
                .any(|record| record.logical_key == foreign_key_dependency_key)
            );
            drop(after_foreign_key_drop);
            let mut constraint_drop_audit =
                identity_audit_event(index as u8, actor, created_constraint.constraint_id);
            constraint_drop_audit.event_id = pressure_id(0xf3, index);
            constraint_drop_audit.correlation_id = constraint_drop_invocation;
            storage.drop_unique_constraint(
                constraint_drop_invocation,
                actor,
                created_constraint.constraint_id,
                9_999,
                constraint_drop_audit,
            )?;
            assert_eq!(
                prdb_engine::catalog_index::scan_constraint_ids_for_table(
                    &retained_constraint_snapshot,
                    table.table_id,
                    8,
                )?,
                expected_constraints,
                "a pinned snapshot must retain the dropped constraint and backing index"
            );
            assert_eq!(
                prdb_engine::catalog_index::scan_index_ids_for_table(
                    &retained_constraint_snapshot,
                    table.table_id,
                    8,
                )?,
                vec![created_constraint.index_id]
            );
            drop(retained_constraint_snapshot);
            let after_constraint_drop = database.begin_read()?;
            assert!(
                prdb_engine::catalog_index::scan_constraint_ids_for_table(
                    &after_constraint_drop,
                    table.table_id,
                    8,
                )?
                .is_empty()
            );
            assert!(
                prdb_engine::catalog_index::scan_index_ids_for_table(
                    &after_constraint_drop,
                    table.table_id,
                    8,
                )?
                .is_empty()
            );

            let retained_table_snapshot = database.begin_read()?;
            assert_eq!(
                lookup_relation_id_by_name(
                    &retained_table_snapshot,
                    created.schema_id,
                    "pressure_table",
                )?,
                Some(table.table_id)
            );

            let table_drop_invocation = pressure_id(0xc1, index);
            let mut table_drop_audit = identity_audit_event(index as u8, actor, table.table_id);
            table_drop_audit.event_id = pressure_id(0xc3, index);
            table_drop_audit.correlation_id = table_drop_invocation;
            storage.drop_table(
                table_drop_invocation,
                actor,
                table.table_id,
                9_999,
                table_drop_audit,
            )?;
            assert_eq!(
                lookup_relation_id_by_name(
                    &retained_table_snapshot,
                    created.schema_id,
                    "pressure_table",
                )?,
                Some(table.table_id),
                "a pinned dictionary snapshot must retain the dropped table name"
            );
            drop(retained_table_snapshot);
            let after_table_drop = database.begin_read()?;
            assert_eq!(
                lookup_relation_id_by_name(&after_table_drop, created.schema_id, "pressure_table")?,
                None
            );

            let drop_id = pressure_id(0x80, index);
            let mut drop_audit = identity_audit_event(index as u8, actor, created.schema_id);
            drop_audit.event_id = pressure_id(0x81, index);
            drop_audit.correlation_id = drop_id;
            storage.drop_schema(drop_id, actor, created.schema_id, 9_999, drop_audit)?;
            let snapshot = database.begin_read()?;
            assert_eq!(lookup_schema_id_by_name(&snapshot, name)?, None);
        }

        Ok::<_, Box<dyn std::error::Error>>(reader.join().expect("schema-index reader panicked"))
    })?;

    assert_eq!(reader, READER_ROUNDS);
    for name in &names {
        assert_eq!(lookup_schema_id_by_name(&retained_snapshot, name)?, None);
        let current_snapshot = database.begin_read()?;
        assert_eq!(lookup_schema_id_by_name(&current_snapshot, name)?, None);
    }
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn membership_indexes_track_both_directions_per_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let member_id = id(0x85);
    let role_id = builtin_owner_id();
    let membership = membership_write(member_id, role_id);
    let dependency = dependency_write(member_id, role_id, 5);
    let mut event = audit_event(0x7a);
    event.actor_id = Some(role_id);
    commit_catalog_and_audit(
        &database,
        id(0x7a),
        role_id,
        9_999,
        &[
            principal_write_with_name(member_id, 1, 1, "membership_index_member"),
            object_write_with_name(member_id, 8, "membership_index_member"),
            membership.clone(),
            dependency.clone(),
        ],
        &[event],
    )
    .map_err(|error| format!("initial membership-index commit: {error:?}"))?;
    let membership_keys_from_catalog = |snapshot: &redb::ReadTransaction| {
        let catalog = snapshot.open_table(CATALOG)?;
        let prefix = 11u16.to_be_bytes();
        let mut keys = Vec::new();
        for entry in catalog.range::<&[u8]>(prefix.as_slice()..)? {
            let (key, _) = entry?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            keys.push(key.value().to_vec());
        }
        Ok::<_, Box<dyn std::error::Error>>(keys)
    };
    let roles_for_member_from_scan = |keys: &[Vec<u8>], member_id: [u8; 16]| {
        let mut roles = keys
            .iter()
            .filter(|key| key.get(3..19) == Some(member_id.as_slice()))
            .filter_map(|key| key.get(20..36)?.try_into().ok())
            .collect::<Vec<[u8; 16]>>();
        roles.sort();
        roles
    };
    let members_for_role_from_scan = |keys: &[Vec<u8>], role_id: [u8; 16]| {
        let mut members = keys
            .iter()
            .filter(|key| key.get(20..36) == Some(role_id.as_slice()))
            .filter_map(|key| key.get(3..19)?.try_into().ok())
            .collect::<Vec<[u8; 16]>>();
        members.sort();
        members
    };
    let old_snapshot = database.begin_read()?;
    let old_membership_keys = membership_keys_from_catalog(&old_snapshot)?;
    assert_eq!(
        lookup_roles_for_member(&old_snapshot, member_id)?,
        roles_for_member_from_scan(&old_membership_keys, member_id)
    );
    assert_eq!(
        lookup_members_for_role(&old_snapshot, role_id)?,
        members_for_role_from_scan(&old_membership_keys, role_id)
    );
    assert_eq!(
        lookup_dependencies_for_dependent(&old_snapshot, member_id)?.len(),
        1
    );
    assert_eq!(
        lookup_dependents_for_referenced(&old_snapshot, role_id)?.len(),
        1
    );

    let second_member_id = id(0x86);
    let second_membership = membership_write(second_member_id, role_id);
    let second_dependency = dependency_write(second_member_id, role_id, 5);
    let mut event = audit_event(0x7b);
    event.actor_id = Some(role_id);
    commit_catalog_and_audit(
        &database,
        id(0x7b),
        role_id,
        9_999,
        &[
            principal_write_with_name(second_member_id, 1, 1, "membership_index_member_2"),
            object_write_with_name(second_member_id, 8, "membership_index_member_2"),
            second_membership,
            second_dependency,
        ],
        &[event],
    )?;
    assert_eq!(
        lookup_roles_for_member(&old_snapshot, member_id)?,
        vec![role_id]
    );
    let new_snapshot = database.begin_read()?;
    let new_membership_keys = membership_keys_from_catalog(&new_snapshot)?;
    assert_eq!(
        lookup_roles_for_member(&new_snapshot, member_id)?,
        roles_for_member_from_scan(&new_membership_keys, member_id)
    );
    assert_eq!(
        lookup_members_for_role(&new_snapshot, role_id)?,
        members_for_role_from_scan(&new_membership_keys, role_id)
    );
    assert_eq!(
        lookup_dependencies_for_dependent(&old_snapshot, member_id)?.len(),
        1
    );
    assert_eq!(
        lookup_dependents_for_referenced(&new_snapshot, role_id)?.len(),
        2
    );
    Ok(())
}

pub(super) fn grant_indexes_track_grantee_and_object_per_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let grantee_id = id(0x87);
    let object_id = builtin_id(1);
    let mut event = audit_event(0x7c);
    event.actor_id = Some(builtin_owner_id());
    commit_catalog_and_audit(
        &database,
        id(0x7c),
        builtin_owner_id(),
        9_999,
        &[
            principal_write_with_name(grantee_id, 1, 1, "grant_index_grantee"),
            object_write_with_name(grantee_id, 8, "grant_index_grantee"),
            grant_write(grantee_id, object_id, 7, 1, object_id, builtin_owner_id()),
            dependency_write(grantee_id, object_id, 5),
        ],
        &[event],
    )?;
    let grant_keys_from_catalog = |snapshot: &redb::ReadTransaction| {
        let catalog = snapshot.open_table(CATALOG)?;
        let prefix = 12u16.to_be_bytes();
        let mut keys = Vec::new();
        for entry in catalog.range::<&[u8]>(prefix.as_slice()..)? {
            let (key, _) = entry?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            keys.push(key.value().to_vec());
        }
        Ok::<_, Box<dyn std::error::Error>>(keys)
    };
    let record_keys = |records: Vec<prdb_engine::catalog::CatalogRecord>| {
        let mut keys = records
            .into_iter()
            .map(|record| record.logical_key)
            .collect::<Vec<_>>();
        keys.sort();
        keys
    };
    let keys_for_grantee = |all: &[Vec<u8>], grantee_id: [u8; 16]| {
        all.iter()
            .filter(|key| key.get(3..19) == Some(grantee_id.as_slice()))
            .cloned()
            .collect::<Vec<_>>()
    };
    let keys_for_object = |all: &[Vec<u8>], object_id: [u8; 16]| {
        all.iter()
            .filter(|key| key.get(20..36) == Some(object_id.as_slice()))
            .cloned()
            .collect::<Vec<_>>()
    };

    let snapshot = database.begin_read()?;
    let catalog_keys = grant_keys_from_catalog(&snapshot)?;
    assert_eq!(
        record_keys(lookup_grants_for_grantee(&snapshot, grantee_id)?),
        keys_for_grantee(&catalog_keys, grantee_id)
    );
    assert_eq!(
        record_keys(lookup_grants_for_object(&snapshot, object_id)?),
        keys_for_object(&catalog_keys, object_id)
    );
    Ok(())
}

pub(super) fn catalog_commit_rejects_malformed_constraint_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(86),
            id(9),
            9_999,
            &[malformed_foreign_key_write()],
            &[audit_event(86)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_rejects_schema_mismatched_check_expression()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let check_id = id(0xa7);
    let expression_id = id(0xa8);
    let table_id = id(82);
    let column_id = id(84);
    let mut writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
        })
        .collect::<Vec<_>>();

    let constraint_key = id_catalog_key(8, check_id);
    let id_list = 0u32.to_le_bytes().to_vec();
    let constraint_payload = encode_envelope(
        &family_schema(8).unwrap(),
        &constraint_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 4u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: id_list.clone(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: id_list,
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 10,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ],
    )?;
    writes.push(CatalogWrite {
        key: constraint_key,
        expected_value: None,
        payload: Some(constraint_payload),
    });
    writes.push(object_write(check_id, 6));

    let expression_key = id_catalog_key(7, expression_id);
    let mut declared_dependencies = 1u32.to_le_bytes().to_vec();
    declared_dependencies.extend_from_slice(&column_id);
    let expression_payload = encode_envelope(
        &family_schema(7).unwrap(),
        &expression_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 2u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Id,
                payload: builtin_id(101).to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bytes,
                payload: vec![0; 8],
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: prdb_engine::expression::encode_expression(
                    &prdb_engine::authorization::Expr::Field(0),
                )?,
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: declared_dependencies,
            },
        ],
    )?;
    writes.push(CatalogWrite {
        key: expression_key,
        expected_value: None,
        payload: Some(expression_payload),
    });
    writes.push(object_write(expression_id, 5));
    writes.push(dependency_write(check_id, expression_id, 2));
    writes.push(dependency_write(expression_id, column_id, 2));

    let admin = builtin_owner_id();
    let mut audit = audit_event(0xaa);
    audit.actor_id = Some(admin);
    let invocation_id = id(0xaa);
    assert_eq!(
        commit_catalog_and_audit(&database, invocation_id, admin, 9_999, &writes, &[audit],),
        Err(CommitError::InvalidCatalog),
        "a CHECK whose expression infers BYTEA must not publish as BOOLEAN"
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        lookup_outcome(&database, invocation_id, admin, 1)?
            .ok_or("missing rejected CHECK outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn catalog_commit_rejects_nullable_primary_key_columns()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(16);
    let column_id = id(0x66);
    let index_id = id(0x44);
    let constraint_id = id(0x70);
    let (index_key, index_payload) =
        owned_unique_index_definition(index_id, table_id, &[column_id], constraint_id);
    let (old_index_key, _) = index_definition(index_id, table_id);
    assert_eq!(index_key, old_index_key);
    let (table_key, table_payload, old_index_payload) = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_key = id_catalog_key(5, table_id);
        let table_payload = catalog.get(table_key.as_slice())?.unwrap().value().to_vec();
        let index_payload = catalog.get(index_key.as_slice())?.unwrap().value().to_vec();
        (table_key, table_payload, index_payload)
    };
    let mut table_update = table_write_with_versions(table_id, id(20), 1, 2);
    table_update.key = table_key;
    table_update.expected_value = Some(table_payload);
    let index_update = CatalogWrite {
        key: index_key,
        expected_value: Some(old_index_payload),
        payload: Some(index_payload),
    };
    let constraint = constraint_write(
        constraint_id,
        table_id,
        2,
        &[column_id],
        None,
        &[],
        Some(index_id),
    );
    let object = object_write(constraint_id, 6);
    let dependency = dependency_write(constraint_id, index_id, 4);

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(91),
            id(9),
            9_999,
            &[table_update, index_update, constraint, object, dependency],
            &[audit_event(91)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_rejects_constraints_for_missing_tables()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(90),
            id(9),
            9_999,
            &[check_constraint_without_table_write()],
            &[audit_event(90)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_rejects_dangling_dependency_edges_and_records_abort()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let edge = dependency_write(id(101), id(102), 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(50),
            id(9),
            9_999,
            std::slice::from_ref(&edge),
            &[audit_event(50)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        lookup_outcome(&database, id(50), id(9), 9_998)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(read.open_table(CATALOG)?.get(&edge.key[..])?.is_none());
    assert!(
        read.open_table(prdb_engine::schema::AUDIT)?
            .get(&id(50)[..])?
            .is_none()
    );
    Ok(())
}
