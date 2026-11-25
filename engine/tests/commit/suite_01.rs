use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::{lookup_relation_id_by_name, lookup_schema_id_by_name};
use prdb_engine::commit::{CatalogWrite, CommitError, commit_catalog_and_audit};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::schema::CATALOG;
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ScalarType, ScalarValue};
use redb::{ReadableDatabase, ReadableTable};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_creates_schema_atomically_for_privileged_actor()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, SchemaLifecycleError, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let setup_admin = id(0xe1);
    let denied = id(0xe2);
    let actor = id(0xe8);
    let successor_owner = id(0xea);
    let successor_role = id(0xf7);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(setup_admin, 1, 1, "schema_admin"),
        object_write_with_name(setup_admin, 8, "schema_admin"),
        membership_write(setup_admin, admin_role),
        dependency_write(setup_admin, admin_role, 5),
        principal_write_with_name(denied, 1, 1, "schema_denied"),
        object_write_with_name(denied, 8, "schema_denied"),
        principal_write_with_name(actor, 1, 1, "schema_creator"),
        object_write_with_name(actor, 8, "schema_creator"),
        principal_write_with_name(successor_owner, 1, 1, "schema_successor"),
        object_write_with_name(successor_owner, 8, "schema_successor"),
        principal_write_with_name(successor_role, 2, 1, "schema_successor_role"),
        object_write_with_name(successor_role, 9, "schema_successor_role"),
        membership_write(successor_owner, successor_role),
        dependency_write(successor_owner, successor_role, 5),
        grant_write(actor, id(0x33), 14, 1, id(0x33), admin_role),
        dependency_write(actor, id(0x33), 5),
    ];
    let mut setup_audit = identity_audit_event(0xe3, admin_role, setup_admin);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xe3), admin_role, 9_999, &setup, &[setup_audit])?;

    let denied_id = id(0xe4);
    let mut denied_audit = identity_audit_event(0xe4, denied, id(0xe5));
    denied_audit.correlation_id = denied_id;
    let denied_result = storage.create_schema(
        denied_id,
        denied,
        "private_schema",
        denied,
        9_999,
        denied_audit,
    );
    assert!(
        matches!(denied_result, Err(SchemaLifecycleError::Unauthorized)),
        "{denied_result:?}"
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, denied_id, denied, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let invocation_id = id(0xe6);
    let mut audit = identity_audit_event(0xe6, actor, id(0xe7));
    audit.correlation_id = invocation_id;
    let created = storage.create_schema(invocation_id, actor, "app_data", actor, 9_999, audit)?;
    assert_eq!(created.commit_sequence, 2);
    let schema_key = id_catalog_key(3, created.schema_id);
    let object_key = id_catalog_key(2, created.schema_id);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        prdb_engine::catalog_index::scan_object_ids_for_owner(&read, actor, 4096)?
            .contains(&created.schema_id),
        "new schema is missing from its owner's object index"
    );
    assert!(catalog.get(schema_key.as_slice())?.is_some());
    let object = catalog
        .get(object_key.as_slice())?
        .ok_or("missing schema object")?;
    let record = decode_registered_envelope(object.value())?;
    assert_eq!(record.family_code, 2);
    assert_eq!(
        record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .unwrap()
            .payload,
        b"app_data"
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(object);
    let transfer_invocation = id(0xc1);
    let mut transfer_audit = identity_audit_event(0xc2, actor, created.schema_id);
    transfer_audit.correlation_id = transfer_invocation;
    let transfer_result = storage.set_schema_owner(
        transfer_invocation,
        actor,
        created.schema_id,
        successor_role,
        9_999,
        transfer_audit,
    );
    assert!(
        transfer_result.is_ok(),
        "schema ownership transfer failed: {transfer_result:?}"
    );
    assert_eq!(transfer_result?, 3);
    assert!(
        prdb_engine::catalog_index::scan_object_ids_for_owner(&read, actor, 4096)?
            .contains(&created.schema_id),
        "the retained owner snapshot lost its schema index entry"
    );
    assert!(
        !prdb_engine::catalog_index::scan_object_ids_for_owner(&read, successor_role, 4096)?
            .contains(&created.schema_id),
        "the retained owner snapshot observed a future owner index entry"
    );
    let retained_object = decode_registered_envelope(
        catalog
            .get(object_key.as_slice())?
            .ok_or("schema object disappeared from retained owner snapshot")?
            .value(),
    )?;
    assert_eq!(
        retained_object
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .unwrap()
            .payload,
        actor,
        "the pre-transfer reader must retain the old schema owner"
    );
    assert_eq!(
        u64::from_le_bytes(
            retained_object
                .fields
                .iter()
                .find(|field| field.tag == 5)
                .unwrap()
                .payload
                .as_slice()
                .try_into()?
        ),
        1,
        "the pre-transfer reader must retain the old object version"
    );
    {
        let read = database.begin_read()?;
        assert!(
            !prdb_engine::catalog_index::scan_object_ids_for_owner(&read, actor, 4096)?
                .contains(&created.schema_id),
            "the former owner retained the transferred schema in the current index"
        );
        assert!(
            prdb_engine::catalog_index::scan_object_ids_for_owner(&read, successor_role, 4096)?
                .contains(&created.schema_id),
            "the successor owner is missing the transferred schema from the index"
        );
        let object = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(object_key.as_slice())?
                .ok_or("schema object disappeared after ownership transfer")?
                .value(),
        )?;
        assert_eq!(
            object
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .unwrap()
                .payload,
            successor_role
        );
        assert_eq!(
            u64::from_le_bytes(
                object
                    .fields
                    .iter()
                    .find(|field| field.tag == 5)
                    .unwrap()
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            2
        );
    }
    drop(catalog);
    drop(read);
    let owned_table_invocation = id(0xc5);
    let mut owned_table_audit = identity_audit_event(0xc6, successor_owner, id(0xc7));
    owned_table_audit.correlation_id = owned_table_invocation;
    let owned_table = storage.create_table(CreateTableRequest {
        invocation_id: owned_table_invocation,
        authenticated_actor_id: successor_owner,
        schema_id: created.schema_id,
        name: "successor_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: owned_table_audit,
    })?;
    let child_grant_prefix = {
        let mut prefix = 12u16.to_be_bytes().to_vec();
        prefix.push(1);
        prefix.extend_from_slice(&successor_role);
        prefix
    };
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        assert!(
            catalog
                .range(child_grant_prefix.as_slice()..)?
                .next()
                .transpose()?
                .is_none_or(|(key, _)| !key.value().starts_with(&child_grant_prefix))
        );
    }
    let owned_table_drop_invocation = id(0xc8);
    let mut owned_table_drop_audit = identity_audit_event(0xc9, successor_owner, id(0xca));
    owned_table_drop_audit.correlation_id = owned_table_drop_invocation;
    storage.drop_table(
        owned_table_drop_invocation,
        successor_owner,
        owned_table.table_id,
        9_999,
        owned_table_drop_audit,
    )?;
    let old_owner_drop = id(0xc3);
    let mut old_owner_drop_audit = identity_audit_event(0xc4, actor, created.schema_id);
    old_owner_drop_audit.correlation_id = old_owner_drop;
    assert!(matches!(
        storage.drop_schema(
            old_owner_drop,
            actor,
            created.schema_id,
            9_999,
            old_owner_drop_audit,
        ),
        Err(SchemaLifecycleError::Unauthorized)
    ));
    let denied_drop_invocation = id(0xf8);
    let mut denied_drop_audit = identity_audit_event(0xf9, denied, id(0xfa));
    denied_drop_audit.correlation_id = denied_drop_invocation;
    assert!(matches!(
        storage.drop_schema(
            denied_drop_invocation,
            denied,
            created.schema_id,
            9_999,
            denied_drop_audit,
        ),
        Err(SchemaLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(&database, denied_drop_invocation, denied, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let duplicate_id = id(0xe9);
    let mut duplicate_audit = identity_audit_event(0xea, actor, id(0xeb));
    duplicate_audit.correlation_id = duplicate_id;
    assert!(matches!(
        storage.create_schema(
            duplicate_id,
            actor,
            "app_data",
            actor,
            9_999,
            duplicate_audit
        ),
        Err(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))
    ));
    assert_eq!(
        lookup_outcome(&database, duplicate_id, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let rename_target_invocation = id(0xcb);
    let mut rename_target_audit = identity_audit_event(0xcc, actor, id(0xcd));
    rename_target_audit.correlation_id = rename_target_invocation;
    let rename_target = storage.create_schema(
        rename_target_invocation,
        actor,
        "rename_target",
        actor,
        9_999,
        rename_target_audit,
    )?;
    let duplicate_rename_invocation = id(0xce);
    let mut duplicate_rename_audit = identity_audit_event(0xcf, successor_owner, created.schema_id);
    duplicate_rename_audit.correlation_id = duplicate_rename_invocation;
    assert!(matches!(
        storage.rename_schema(
            duplicate_rename_invocation,
            successor_owner,
            created.schema_id,
            "rename_target",
            9_999,
            duplicate_rename_audit,
        ),
        Err(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))
    ));
    assert_eq!(
        lookup_outcome(&database, duplicate_rename_invocation, successor_owner, 1)?
            .ok_or("missing duplicate schema-name outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let rename_invocation = id(0xd0);
    let mut rename_audit = identity_audit_event(0xd1, successor_owner, created.schema_id);
    rename_audit.correlation_id = rename_invocation;
    let renamed_schema = storage.rename_schema(
        rename_invocation,
        successor_owner,
        created.schema_id,
        "app_archive",
        9_999,
        rename_audit,
    );
    assert!(
        renamed_schema.is_ok(),
        "schema rename failed: {renamed_schema:?}"
    );
    assert_eq!(renamed_schema?, 7);
    let renamed_object_bytes = {
        let read = database.begin_read()?;
        read.open_table(CATALOG)?
            .get(object_key.as_slice())?
            .ok_or("renamed schema object missing")?
            .value()
            .to_vec()
    };
    let renamed_object = decode_registered_envelope(&renamed_object_bytes)?;
    assert_eq!(
        renamed_object
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .unwrap()
            .payload,
        b"app_archive"
    );
    assert_eq!(
        u64::from_le_bytes(
            renamed_object
                .fields
                .iter()
                .find(|field| field.tag == 5)
                .unwrap()
                .payload
                .as_slice()
                .try_into()?
        ),
        3
    );
    let old_owner_rename_invocation = id(0xd2);
    let mut old_owner_rename_audit = identity_audit_event(0xd3, actor, created.schema_id);
    old_owner_rename_audit.correlation_id = old_owner_rename_invocation;
    assert!(matches!(
        storage.rename_schema(
            old_owner_rename_invocation,
            actor,
            created.schema_id,
            "old_owner_name",
            9_999,
            old_owner_rename_audit,
        ),
        Err(SchemaLifecycleError::Unauthorized)
    ));
    let role_owner_transfer_invocation = id(0xd6);
    let mut role_owner_transfer_audit =
        identity_audit_event(0xd7, successor_owner, created.schema_id);
    role_owner_transfer_audit.correlation_id = role_owner_transfer_invocation;
    let role_owner_transfer = storage.set_schema_owner(
        role_owner_transfer_invocation,
        successor_owner,
        created.schema_id,
        actor,
        9_999,
        role_owner_transfer_audit,
    );
    assert!(
        role_owner_transfer.is_ok(),
        "inherited schema owner could not transfer ownership: {role_owner_transfer:?}"
    );
    assert_eq!(role_owner_transfer?, 8);
    assert_eq!(
        lookup_outcome(
            &database,
            role_owner_transfer_invocation,
            successor_owner,
            1
        )?
        .ok_or("missing role-owner transfer outcome")?
        .status,
        OutcomeStatus::Committed
    );
    let role_member_rename_invocation = id(0xd8);
    let mut role_member_rename_audit =
        identity_audit_event(0xd9, successor_owner, created.schema_id);
    role_member_rename_audit.correlation_id = role_member_rename_invocation;
    assert!(matches!(
        storage.rename_schema(
            role_member_rename_invocation,
            successor_owner,
            created.schema_id,
            "former_role_owner_name",
            9_999,
            role_member_rename_audit,
        ),
        Err(SchemaLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(&database, role_member_rename_invocation, successor_owner, 1)?
            .ok_or("missing former role-owner rename outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let drop_invocation = id(0xec);
    let mut drop_audit = identity_audit_event(0xed, actor, id(0xee));
    drop_audit.correlation_id = drop_invocation;
    let pre_drop_schema_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_schema_id_by_name(&pre_drop_schema_snapshot, "app_archive")?,
        Some(created.schema_id)
    );
    let pre_drop_catalog = pre_drop_schema_snapshot.open_table(CATALOG)?;
    let retired_object_bytes = pre_drop_catalog
        .get(object_key.as_slice())?
        .ok_or("pre-drop schema object is missing")?
        .value()
        .to_vec();
    let retired_schema_bytes = pre_drop_catalog
        .get(schema_key.as_slice())?
        .ok_or("pre-drop schema record is missing")?
        .value()
        .to_vec();
    let dropped =
        storage.drop_schema(drop_invocation, actor, created.schema_id, 9_999, drop_audit)?;
    assert_eq!(dropped.schema_id, created.schema_id);
    assert_eq!(dropped.commit_sequence, 9);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(schema_key.as_slice())?.is_none());
    assert!(catalog.get(object_key.as_slice())?.is_none());
    assert!(
        catalog
            .get(id_catalog_key(17, created.schema_id).as_slice())?
            .is_some(),
        "DROP SCHEMA must permanently reserve its retired object ID"
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_schema_id_by_name(&read, "app_archive")?,
        None,
        "a fresh snapshot after DROP must not resolve the retired schema name"
    );
    let reuse_invocation = id(0x88);
    let mut reuse_audit = identity_audit_event(0x89, actor, created.schema_id);
    reuse_audit.correlation_id = reuse_invocation;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            reuse_invocation,
            actor,
            9_999,
            &[
                CatalogWrite {
                    key: object_key.clone(),
                    expected_value: None,
                    payload: Some(retired_object_bytes),
                },
                CatalogWrite {
                    key: schema_key.clone(),
                    expected_value: None,
                    payload: Some(retired_schema_bytes),
                },
            ],
            &[reuse_audit],
        ),
        Err(CommitError::InvalidCatalog),
        "a dropped schema ID must not be reusable by a later catalog commit"
    );
    let recreate_invocation = id(0x8a);
    let mut recreate_audit = identity_audit_event(0x8b, actor, id(0x8c));
    recreate_audit.correlation_id = recreate_invocation;
    let recreated = storage.create_schema(
        recreate_invocation,
        actor,
        "app_archive",
        actor,
        9_999,
        recreate_audit,
    )?;
    assert_ne!(
        recreated.schema_id, created.schema_id,
        "recreating a dropped schema name must allocate a fresh stable ID"
    );
    assert_eq!(
        lookup_schema_id_by_name(&pre_drop_schema_snapshot, "app_archive")?,
        Some(created.schema_id),
        "the pinned pre-drop snapshot must continue resolving the original schema"
    );
    let recreated_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_schema_id_by_name(&recreated_snapshot, "app_archive")?,
        Some(recreated.schema_id)
    );
    let drop_target_invocation = id(0xd4);
    let mut drop_target_audit = identity_audit_event(0xd5, actor, rename_target.schema_id);
    drop_target_audit.correlation_id = drop_target_invocation;
    storage.drop_schema(
        drop_target_invocation,
        actor,
        rename_target.schema_id,
        9_999,
        drop_target_audit,
    )?;

    let populated_schema_invocation = id(0xef);
    let mut populated_schema_audit = identity_audit_event(0xf0, actor, id(0xf1));
    populated_schema_audit.correlation_id = populated_schema_invocation;
    let populated_schema = storage.create_schema(
        populated_schema_invocation,
        actor,
        "populated_schema",
        setup_admin,
        9_999,
        populated_schema_audit,
    )?;
    let create_table_invocation = id(0xf2);
    let mut create_table_audit = identity_audit_event(0xf3, setup_admin, id(0xf4));
    create_table_audit.correlation_id = create_table_invocation;
    storage.create_table(CreateTableRequest {
        invocation_id: create_table_invocation,
        authenticated_actor_id: setup_admin,
        schema_id: populated_schema.schema_id,
        name: "present_child".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "label".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_table_audit,
    })?;
    let rejected_drop_invocation = id(0xf5);
    let mut rejected_drop_audit = identity_audit_event(0xf6, setup_admin, id(0xf7));
    rejected_drop_audit.correlation_id = rejected_drop_invocation;
    assert!(matches!(
        storage.drop_schema(
            rejected_drop_invocation,
            setup_admin,
            populated_schema.schema_id,
            9_999,
            rejected_drop_audit,
        ),
        Err(SchemaLifecycleError::Commit(CommitError::SchemaNotEmpty))
    ));
    assert_eq!(
        lookup_outcome(&database, rejected_drop_invocation, setup_admin, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(id_catalog_key(3, populated_schema.schema_id).as_slice())?
            .is_some()
    );
    Ok(())
}

pub(super) fn schema_drop_racing_table_creation_never_leaves_an_orphan()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, SchemaLifecycleError, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xa1);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "schema_race_admin"),
        object_write_with_name(actor, 8, "schema_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0xa2, admin_role, id(0xa2));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xa2), admin_role, 9_999, &setup, &[setup_audit])?;
    let mut create_schema_audit = identity_audit_event(0xa3, actor, id(0xa3));
    create_schema_audit.correlation_id = id(0xa3);
    let schema = storage.create_schema(
        id(0xa3),
        actor,
        "schema_race",
        actor,
        9_999,
        create_schema_audit,
    )?;

    let create_table_invocation = id(0xa4);
    let drop_schema_invocation = id(0xa7);
    let barrier = Arc::new(Barrier::new(3));
    let (table_result, drop_result) = std::thread::scope(|scope| {
        let create_barrier = barrier.clone();
        let create_storage = &storage;
        let create = scope.spawn(move || {
            create_barrier.wait();
            let mut audit = identity_audit_event(0xa5, actor, id(0xa6));
            audit.correlation_id = create_table_invocation;
            create_storage.create_table(CreateTableRequest {
                invocation_id: create_table_invocation,
                authenticated_actor_id: actor,
                schema_id: schema.schema_id,
                name: "racing_child".to_owned(),
                columns: vec![TableColumnDefinition {
                    name: "value".to_owned(),
                    scalar_type: ScalarType::Text,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                }],
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        let drop_barrier = barrier.clone();
        let drop_storage = &storage;
        let drop_schema = scope.spawn(move || {
            drop_barrier.wait();
            let mut audit = identity_audit_event(0xa8, actor, id(0xa9));
            audit.correlation_id = drop_schema_invocation;
            drop_storage.drop_schema(
                drop_schema_invocation,
                actor,
                schema.schema_id,
                9_999,
                audit,
            )
        });
        barrier.wait();
        (create.join().unwrap(), drop_schema.join().unwrap())
    });
    let table_created = match (table_result, drop_result) {
        (Ok(table), Err(SchemaLifecycleError::Commit(CommitError::SchemaNotEmpty))) => Some(table),
        (
            Err(
                TableLifecycleError::Unauthorized
                | TableLifecycleError::Commit(CommitError::Conflict),
            ),
            Ok(_),
        ) => None,
        (table, dropped) => {
            panic!("invalid schema-drop/table-create race: {table:?} / {dropped:?}")
        }
    };
    let create_outcome = lookup_outcome(&database, create_table_invocation, actor, 1)?
        .ok_or("missing racing table-create outcome")?;
    let drop_outcome = lookup_outcome(&database, drop_schema_invocation, actor, 1)?
        .ok_or("missing racing schema-drop outcome")?;
    let catalog_read = database.begin_read()?;
    let catalog = catalog_read.open_table(CATALOG)?;
    match table_created {
        Some(table) => {
            assert_eq!(create_outcome.status, OutcomeStatus::Committed);
            assert_eq!(drop_outcome.status, OutcomeStatus::Aborted);
            assert!(
                catalog
                    .get(id_catalog_key(2, schema.schema_id).as_slice())?
                    .is_some()
            );
            assert!(
                catalog
                    .get(id_catalog_key(3, schema.schema_id).as_slice())?
                    .is_some()
            );
            assert!(
                catalog
                    .get(id_catalog_key(2, table.table_id).as_slice())?
                    .is_some()
            );
            assert!(
                catalog
                    .get(id_catalog_key(5, table.table_id).as_slice())?
                    .is_some()
            );
        }
        None => {
            assert_eq!(create_outcome.status, OutcomeStatus::Aborted);
            assert_eq!(drop_outcome.status, OutcomeStatus::Committed);
            assert!(
                catalog
                    .get(id_catalog_key(2, schema.schema_id).as_slice())?
                    .is_none()
            );
            assert!(
                catalog
                    .get(id_catalog_key(3, schema.schema_id).as_slice())?
                    .is_none()
            );
            for entry in catalog.iter()? {
                let (key, value) = entry?;
                if key.value().get(..2) == Some(2u16.to_be_bytes().as_slice()) {
                    let object = decode_registered_envelope(value.value())?;
                    assert!(
                        object
                            .fields
                            .iter()
                            .find(|field| field.tag == 2)
                            .is_none_or(|field| field.payload.as_slice() != schema.schema_id)
                    );
                }
            }
        }
    }
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn schema_rename_racing_table_creation_preserves_catalog_identity()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xb1);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "schema_rename_race_admin"),
        object_write_with_name(actor, 8, "schema_rename_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0xb2, admin_role, id(0xb2));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb2), admin_role, 9_999, &setup, &[setup_audit])?;
    let mut create_schema_audit = identity_audit_event(0xb3, actor, id(0xb3));
    create_schema_audit.correlation_id = id(0xb3);
    let schema = storage.create_schema(
        id(0xb3),
        actor,
        "schema_rename_race_before",
        actor,
        9_999,
        create_schema_audit,
    )?;

    let create_invocation = id(0xb4);
    let rename_invocation = id(0xb7);
    let barrier = Arc::new(Barrier::new(3));
    let (create_result, rename_result) = std::thread::scope(|scope| {
        let create_barrier = Arc::clone(&barrier);
        let create_storage = &storage;
        let create = scope.spawn(move || {
            create_barrier.wait();
            let mut audit = identity_audit_event(0xb5, actor, id(0xb6));
            audit.correlation_id = create_invocation;
            create_storage.create_table(CreateTableRequest {
                invocation_id: create_invocation,
                authenticated_actor_id: actor,
                schema_id: schema.schema_id,
                name: "created_during_rename".to_owned(),
                columns: vec![TableColumnDefinition {
                    name: "value".to_owned(),
                    scalar_type: ScalarType::Text,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                }],
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        let rename_barrier = Arc::clone(&barrier);
        let rename_storage = &storage;
        let rename = scope.spawn(move || {
            rename_barrier.wait();
            let mut audit = identity_audit_event(0xb8, actor, id(0xb9));
            audit.correlation_id = rename_invocation;
            rename_storage.rename_schema(
                rename_invocation,
                actor,
                schema.schema_id,
                "schema_rename_race_after",
                9_999,
                audit,
            )
        });
        barrier.wait();
        (
            create.join().expect("schema table-create thread panicked"),
            rename.join().expect("schema rename thread panicked"),
        )
    });

    assert!(
        rename_result.is_ok(),
        "schema rename failed: {rename_result:?}"
    );
    let table = match create_result {
        Ok(table) => Some(table),
        Err(TableLifecycleError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    let rename_outcome = lookup_outcome(&database, rename_invocation, actor, 1)?
        .ok_or("missing racing schema-rename outcome")?;
    assert_eq!(rename_outcome.status, OutcomeStatus::Committed);
    let create_outcome = lookup_outcome(&database, create_invocation, actor, 1)?
        .ok_or("missing racing table-create outcome")?;
    assert_eq!(
        create_outcome.status,
        if table.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );

    let read = database.begin_read()?;
    assert_eq!(
        lookup_schema_id_by_name(&read, "schema_rename_race_before")?,
        None
    );
    assert_eq!(
        lookup_schema_id_by_name(&read, "schema_rename_race_after")?,
        Some(schema.schema_id)
    );
    if let Some(table) = table {
        assert_eq!(
            lookup_relation_id_by_name(&read, schema.schema_id, "created_during_rename")?,
            Some(table.table_id)
        );
        let object = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(2, table.table_id).as_slice())?
                .ok_or("created table object missing")?
                .value(),
        )?;
        assert!(
            object
                .fields
                .iter()
                .any(|field| { field.tag == 2 && field.payload.as_slice() == schema.schema_id })
        );
    } else {
        assert_eq!(
            lookup_relation_id_by_name(&read, schema.schema_id, "created_during_rename")?,
            None
        );
    }
    Ok(())
}

pub(super) fn storage_manager_creates_typed_table_with_atomic_check_constraint()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefault, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xd1);
    let denied = id(0xd2);
    let schema_id = id(0xd3);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "table_creator"),
        object_write_with_name(actor, 8, "table_creator"),
        principal_write_with_name(denied, 1, 1, "table_denied"),
        object_write_with_name(denied, 8, "table_denied"),
        object_write_with_parent_and_owner_and_name(schema_id, 1, None, admin_role, "table_schema"),
        schema_write(schema_id),
        grant_write(actor, schema_id, 7, 1, schema_id, admin_role),
        dependency_write(actor, schema_id, 5),
    ];
    let mut setup_audit = identity_audit_event(0xd4, admin_role, id(0xd4));
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(id(0xd4), admin_role, 9_999, &setup, &[setup_audit])
        .map_err(|error| format!("setup failed: {error:?}"))?;

    let denied_invocation = id(0xd5);
    let mut denied_audit = identity_audit_event(0xd5, denied, id(0xd6));
    denied_audit.correlation_id = denied_invocation;
    let denied_result = storage.create_table(CreateTableRequest {
        invocation_id: denied_invocation,
        authenticated_actor_id: denied,
        schema_id,
        name: "ledger".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Uuid,
            nullable: false,
            generated_uuid_identity: true,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: denied_audit,
    });
    assert!(matches!(
        denied_result,
        Err(TableLifecycleError::Unauthorized)
    ));

    let reserved_invocation = id(0xdb);
    let mut reserved_audit = identity_audit_event(0xdb, actor, id(0xdc));
    reserved_audit.correlation_id = reserved_invocation;
    let reserved_result = storage.create_table(CreateTableRequest {
        invocation_id: reserved_invocation,
        authenticated_actor_id: actor,
        schema_id: builtin_id(1),
        name: "forbidden_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Uuid,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: reserved_audit,
    });
    assert!(matches!(
        reserved_result,
        Err(TableLifecycleError::Unauthorized)
    ));

    let invocation_id = id(0xd7);
    let mut audit = identity_audit_event(0xd7, actor, id(0xd8));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_table_with_named_constraints_and_foreign_keys(
            CreateTableRequest {
                invocation_id,
                authenticated_actor_id: actor,
                schema_id,
                name: "ledger".to_owned(),
                columns: vec![
                    TableColumnDefinition {
                        name: "id".to_owned(),
                        scalar_type: ScalarType::Uuid,
                        nullable: false,
                        generated_uuid_identity: true,
                        default: None,
                    },
                    TableColumnDefinition {
                        name: "label".to_owned(),
                        scalar_type: ScalarType::Text,
                        nullable: true,
                        generated_uuid_identity: false,
                        default: Some(TableColumnDefault::Literal(Some(ScalarValue::Text(
                            "untitled".to_owned(),
                        )))),
                    },
                    TableColumnDefinition {
                        name: "created_by".to_owned(),
                        scalar_type: ScalarType::Uuid,
                        nullable: false,
                        generated_uuid_identity: false,
                        default: Some(TableColumnDefault::CurrentPrincipalId),
                    },
                ],
                retention_deadline_us: 9_999,
                audit_event: audit,
            },
            None,
            None,
            &[],
            &[],
            &[prdb_engine::authorization::Expr::GreaterEqual(
                Box::new(prdb_engine::authorization::Expr::Field(1)),
                Box::new(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Text("a".to_owned()),
                ))),
            )],
            &[Some("ledger_label_minimum".to_owned())],
            &[],
            &[],
        )
        .map_err(|error| format!("table creation failed: {error:?}"))?;
    assert_eq!(created.column_ids.len(), 3);
    assert_eq!(created.commit_sequence, 2);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let check_constraint =
        prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, created.table_id, 1)?
            .into_iter()
            .next()
            .ok_or("missing named CHECK constraint")?;
    let check_object = catalog
        .get(id_catalog_key(2, check_constraint).as_slice())?
        .ok_or("missing named CHECK object")?;
    assert_eq!(
        decode_registered_envelope(check_object.value())?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("CHECK object name missing")?
            .payload,
        b"ledger_label_minimum"
    );
    let keyed_invocation = id(0xe1);
    let mut keyed_audit = identity_audit_event(0xe1, actor, id(0xe2));
    keyed_audit.correlation_id = keyed_invocation;
    let _keyed = storage
        .create_table_with_named_constraints_and_foreign_keys(
            CreateTableRequest {
                invocation_id: keyed_invocation,
                authenticated_actor_id: actor,
                schema_id,
                name: "keyed_ledger".to_owned(),
                columns: vec![
                    TableColumnDefinition {
                        name: "id".to_owned(),
                        scalar_type: ScalarType::Uuid,
                        nullable: false,
                        generated_uuid_identity: false,
                        default: None,
                    },
                    TableColumnDefinition {
                        name: "parent_id".to_owned(),
                        scalar_type: ScalarType::Uuid,
                        nullable: true,
                        generated_uuid_identity: false,
                        default: None,
                    },
                ],
                retention_deadline_us: 9_999,
                audit_event: keyed_audit,
            },
            Some(&["id".to_owned()]),
            Some("keyed_ledger_pk".to_owned()),
            &[vec!["parent_id".to_owned()]],
            &[Some("keyed_ledger_parent_key".to_owned())],
            &[],
            &[],
            &[
                prdb_engine::storage_manager::CreateTableForeignKeyDefinition {
                    source_columns: vec!["parent_id".to_owned()],
                    target_constraint_id: [0; 16],
                    target_columns: Some(vec!["id".to_owned()]),
                },
            ],
            &[Some("keyed_ledger_parent_fk".to_owned())],
        )
        .map_err(|error| format!("named key table creation failed: {error:?}"))?;
    let keyed_read = database.begin_read()?;
    let keyed_catalog = keyed_read.open_table(CATALOG)?;
    let mut keyed_constraint_names = Vec::new();
    for entry in keyed_catalog.iter()? {
        let (key, value) = entry?;
        let key = key.value();
        if key.len() != 19 || key[..2] != 2u16.to_be_bytes() {
            continue;
        }
        let object = decode_registered_envelope(value.value())?;
        if object.family_code == 2
            && object
                .fields
                .iter()
                .any(|field| field.tag == 1 && field.payload.as_slice() == 6u16.to_le_bytes())
            && let Some(name) = object.fields.iter().find(|field| field.tag == 3)
        {
            keyed_constraint_names.push(name.payload.clone());
        }
    }
    for expected_name in [
        b"keyed_ledger_pk".as_slice(),
        b"keyed_ledger_parent_key".as_slice(),
        b"keyed_ledger_parent_fk".as_slice(),
    ] {
        assert!(
            keyed_constraint_names
                .iter()
                .any(|name| name == expected_name)
        );
    }
    let table_record = catalog
        .get(id_catalog_key(5, created.table_id).as_slice())?
        .ok_or("missing table record")?;
    let table = decode_registered_envelope(table_record.value())?;
    assert_eq!(table.family_code, 5);
    for (ordinal, column_id) in created.column_ids.iter().enumerate() {
        let column_record = catalog
            .get(id_catalog_key(6, *column_id).as_slice())?
            .ok_or("missing column record")?;
        let column = decode_registered_envelope(column_record.value())?;
        assert_eq!(column.family_code, 6);
        if ordinal > 0 {
            let expression_id: [u8; 16] = column
                .fields
                .iter()
                .find(|field| field.tag == 7)
                .ok_or("missing column default reference")?
                .payload
                .as_slice()
                .try_into()?;
            let expression = catalog
                .get(id_catalog_key(7, expression_id).as_slice())?
                .ok_or("missing default expression")?;
            let expression = decode_registered_envelope(expression.value())?;
            assert_eq!(expression.family_code, 7);
        }
        assert_eq!(
            u32::from_le_bytes(
                column
                    .fields
                    .iter()
                    .find(|field| field.tag == 2)
                    .unwrap()
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            ordinal as u32
        );
    }
    let mut check_constraints = 0;
    for entry in catalog.iter()? {
        let (key, value) = entry?;
        let key = key.value();
        if key.len() != 19 || key[..2] != 8u16.to_be_bytes() || key[2] != 1 {
            continue;
        }
        let constraint = decode_registered_envelope(value.value())?;
        let constraint_table: [u8; 16] = constraint
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .ok_or("missing check table id")?
            .payload
            .as_slice()
            .try_into()?;
        let kind: [u8; 2] = constraint
            .fields
            .iter()
            .find(|field| field.tag == 2)
            .ok_or("missing check kind")?
            .payload
            .as_slice()
            .try_into()?;
        if constraint_table != created.table_id || u16::from_le_bytes(kind) != 4 {
            continue;
        }
        check_constraints += 1;
        let expression_id: [u8; 16] = constraint
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .ok_or("missing check expression id")?
            .payload
            .as_slice()
            .try_into()?;
        assert!(
            catalog
                .get(id_catalog_key(7, expression_id).as_slice())?
                .is_some()
        );
        assert!(
            catalog
                .get(
                    dependency_write(expression_id, created.column_ids[1], 2)
                        .key
                        .as_slice()
                )?
                .is_some()
        );
    }
    assert_eq!(check_constraints, 1);
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );

    let duplicate_invocation = id(0xd9);
    let mut duplicate_audit = identity_audit_event(0xd9, actor, id(0xda));
    duplicate_audit.correlation_id = duplicate_invocation;
    let duplicate = storage.create_table(CreateTableRequest {
        invocation_id: duplicate_invocation,
        authenticated_actor_id: actor,
        schema_id,
        name: "ledger".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Uuid,
            nullable: false,
            generated_uuid_identity: true,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: duplicate_audit,
    });
    assert!(matches!(
        duplicate,
        Err(TableLifecycleError::Commit(CommitError::InvalidCatalog))
    ));
    assert_eq!(
        lookup_outcome(&database, duplicate_invocation, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}
