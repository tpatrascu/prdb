use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::commit::{CatalogWrite, CommitError, RowWrite, commit_rows};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{CATALOG, ROWS};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::ReadableDatabase;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_authorizes_object_grants_and_linearizes_revocation_races()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_owner = id(0x74);
    let grant_manager = id(0x75);
    let denied_user = id(0x76);
    let owner_grantee = id(0x77);
    let manager_grantee = id(0x78);
    let denied_grantee = id(0x79);
    let admin_role = builtin_owner_id();
    let records = vec![
        principal_write_with_name(table_owner, 1, 1, "table_owner"),
        object_write_with_name(table_owner, 8, "table_owner"),
        principal_write_with_name(grant_manager, 1, 1, "grant_manager"),
        object_write_with_name(grant_manager, 8, "grant_manager"),
        principal_write_with_name(denied_user, 1, 1, "grant_denied"),
        object_write_with_name(denied_user, 8, "grant_denied"),
        principal_write_with_name(owner_grantee, 1, 1, "owner_grantee"),
        object_write_with_name(owner_grantee, 8, "owner_grantee"),
        principal_write_with_name(manager_grantee, 1, 1, "manager_grantee"),
        object_write_with_name(manager_grantee, 8, "manager_grantee"),
        principal_write_with_name(denied_grantee, 1, 1, "denied_grantee"),
        object_write_with_name(denied_grantee, 8, "denied_grantee"),
        grant_write(grant_manager, id(16), 11, 1, id(16), admin_role),
        dependency_write(grant_manager, id(16), 5),
    ];
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let old_table_object = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(id_catalog_key(2, id(16)).as_slice())?
        .ok_or("missing table object")?
        .value()
        .to_vec();
    let mut owner_table = object_write_with_owner(id(16), 3, table_owner);
    owner_table.expected_value = Some(old_table_object);
    let mut setup_writes = records;
    setup_writes.push(owner_table);
    let mut setup_audit = audit_event(100);
    setup_audit.actor_id = Some(admin_role);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(100), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let bump_grantee_epoch =
        |grantee_id, epoch, name: &str| -> Result<_, Box<dyn std::error::Error>> {
            let mut write = principal_write_with_name(grantee_id, 1, epoch, name);
            let read = database.begin_read()?;
            write.expected_value = Some(
                read.open_table(CATALOG)?
                    .get(id_catalog_key(10, grantee_id).as_slice())?
                    .ok_or("missing grant grantee")?
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
                .ok_or("missing grant record")?
                .value()
                .to_vec(),
        );
        write.payload = None;
        Ok(write)
    };

    let owner_select = grant_write(owner_grantee, id(16), 2, 1, id(16), table_owner);
    let mut owner_audit = audit_event(101);
    owner_audit.actor_id = Some(table_owner);
    owner_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_object_grant_ddl(
            id(101),
            table_owner,
            9_999,
            &[
                owner_select.clone(),
                dependency_write(owner_grantee, id(16), 5),
                bump_grantee_epoch(owner_grantee, 2, "owner_grantee")?,
            ],
            &[owner_audit],
        )?,
        2
    );

    let manager_update = grant_write(manager_grantee, id(16), 4, 1, id(16), grant_manager);
    let mut manager_audit = audit_event(102);
    manager_audit.actor_id = Some(grant_manager);
    manager_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_object_grant_ddl(
            id(102),
            grant_manager,
            9_999,
            &[
                manager_update.clone(),
                dependency_write(manager_grantee, id(16), 5),
                bump_grantee_epoch(manager_grantee, 2, "manager_grantee")?,
            ],
            &[manager_audit],
        )?,
        3
    );

    let denied_insert = grant_write(denied_grantee, id(16), 3, 1, id(16), denied_user);
    let mut denied_audit = audit_event(103);
    denied_audit.actor_id = Some(denied_user);
    denied_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_object_grant_ddl(
            id(103),
            denied_user,
            9_999,
            &[
                denied_insert.clone(),
                bump_grantee_epoch(denied_grantee, 2, "denied_grantee")?,
            ],
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let mut revoke_audit = audit_event(104);
    revoke_audit.actor_id = Some(table_owner);
    revoke_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_object_grant_ddl(
            id(104),
            table_owner,
            9_999,
            &[
                remove_catalog_record(owner_select.clone())?,
                remove_catalog_record(dependency_write(owner_grantee, id(16), 5))?,
                bump_grantee_epoch(owner_grantee, 3, "owner_grantee")?,
            ],
            &[revoke_audit],
        )?,
        4
    );

    use prdb_engine::authorization::{
        AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Effect,
        Expr, Grant, Operation, Policy, Subject,
    };
    use prdb_engine::tuple::FieldPatch;

    let row_id = id(0x81);
    let row_key = table_row_key(id(16), row_id);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[0x82; 16]];
    commit_rows(
        &database,
        id(0x80),
        manager_grantee,
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text("before revoke".to_owned()))],
            )?),
        }],
    )
    .map_err(|error| format!("seed grant-race row: {error:?}"))?;

    let grant_bytes = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(manager_update.key.as_slice())?
        .ok_or("missing update grant before staged write")?
        .value()
        .to_vec();
    let context = AuthorizationContext {
        principal_id: manager_grantee,
        role_ids: Default::default(),
        scope: [
            (Operation::Usage, AccessTarget::Schema(id(16))),
            (Operation::Select, AccessTarget::Table(id(16))),
            (Operation::Update, AccessTarget::Table(id(16))),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: id(16),
                    column_id: column_ids[0],
                },
            ),
            (
                Operation::Update,
                AccessTarget::Column {
                    table_id: id(16),
                    column_id: column_ids[0],
                },
            ),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Usage,
            target: AccessTarget::Schema(id(16)),
        },
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Select,
            target: AccessTarget::Table(id(16)),
        },
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Update,
            target: AccessTarget::Table(id(16)),
        },
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
        },
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
        },
    ];
    let mut policies = Vec::new();
    for operation in [Operation::Select, Operation::Update] {
        let policy = Policy {
            subjects: vec![Subject::Principal(manager_grantee)],
            operation,
            target: AccessTarget::Table(id(16)),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: (operation == Operation::Update)
                .then_some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        };
        policies.push(policy.clone());
        policies.push(Policy {
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
            ..policy
        });
    }
    let dependency = AuthorityDependency {
        key: manager_update.key.clone(),
        expected_value: Some(grant_bytes),
    };
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: id(16),
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: std::slice::from_ref(&dependency),
    };
    let mut writer = storage.begin_invocation(id(0x83), manager_grantee, 9_999, 1, 1024)?;
    writer
        .stage_patch_authorized(
            prdb_engine::overlay::RowId {
                table: id(16),
                row: row_id,
            },
            &projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text("after grant race".to_owned())),
            }],
        )
        .map_err(|error| format!("stage grant-race update: {error:?}"))?;

    let mut race_revoke_audit = audit_event(0x84);
    race_revoke_audit.actor_id = Some(grant_manager);
    race_revoke_audit.action = AuditAction::AuthorityChange;
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(2));
    let (write_result, revoke_result) = std::thread::scope(|scope| {
        let write_barrier = barrier.clone();
        let write_thread = scope.spawn(move || {
            write_barrier.wait();
            writer.commit()
        });
        barrier.wait();
        let revoke_result = storage.commit_authorized_object_grant_ddl(
            id(0x84),
            grant_manager,
            9_999,
            &[
                remove_catalog_record(manager_update.clone())?,
                remove_catalog_record(dependency_write(manager_grantee, id(16), 5))?,
                bump_grantee_epoch(manager_grantee, 3, "manager_grantee")?,
            ],
            &[race_revoke_audit],
        );
        let write_result = write_thread.join().expect("grant writer thread panicked");
        Ok::<_, Box<dyn std::error::Error>>((write_result, revoke_result))
    })?;
    let write_status = match write_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        Err(error) => panic!("unexpected grant-writer race result: {error:?}"),
    };
    let revoke_sequence =
        revoke_result.map_err(|error| format!("revoke grant for staged writer: {error:?}"))?;
    assert_eq!(commit_sequence(&database)?, revoke_sequence);
    assert_eq!(
        revoke_sequence,
        before_race + u64::from(write_status == OutcomeStatus::Committed) + 1
    );
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &RowRecord::decode(
                database
                    .begin_read()?
                    .open_table(ROWS)?
                    .get(&row_key[..])?
                    .ok_or("row disappeared after grant revoke")?
                    .value(),
            )?
            .payload,
        )?
        .values,
        [Some(ScalarValue::Text(
            if write_status == OutcomeStatus::Committed {
                "after grant race"
            } else {
                "before revoke"
            }
            .to_owned()
        ))]
    );
    assert_eq!(
        lookup_outcome(&database, id(0x83), manager_grantee, 1)?
            .ok_or("missing stale grant writer outcome")?
            .status,
        write_status
    );

    assert_eq!(commit_sequence(&database)?, revoke_sequence);
    for (invocation_id, principal_id, status) in [
        (id(101), table_owner, OutcomeStatus::Committed),
        (id(102), grant_manager, OutcomeStatus::Committed),
        (id(103), denied_user, OutcomeStatus::Aborted),
        (id(104), table_owner, OutcomeStatus::Committed),
        (id(0x80), manager_grantee, OutcomeStatus::Committed),
        (id(0x84), grant_manager, OutcomeStatus::Committed),
        (id(0x83), manager_grantee, write_status),
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
    assert!(catalog.get(owner_select.key.as_slice())?.is_none());
    assert!(catalog.get(manager_update.key.as_slice())?.is_none());
    assert!(catalog.get(denied_insert.key.as_slice())?.is_none());
    assert!(
        catalog
            .get(dependency_write(owner_grantee, id(16), 5).key.as_slice())?
            .is_none()
    );
    Ok(())
}

pub(super) fn object_grant_batch_racing_manage_grants_revocation_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, ObjectGrantEntry, ObjectGrantPrivilege, ObjectGrantTarget,
        SetObjectGrantBatchRequest, SetObjectGrantEntriesBatchRequest, SetObjectGrantRequest,
        TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let database_admin = builtin_owner_id();
    let table_owner = id(0x74);
    let grant_manager = id(0x75);
    let schema_admin = id(0x76);
    let grantee = id(0x78);
    let denied_grantee = id(0x79);
    let grant_first_grantee = id(0x7a);
    let invocation_id = id(0x84);
    let revoke_id = id(0x85);
    let mut setup_audit = audit_event(100);
    setup_audit.actor_id = Some(database_admin);
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(
            id(100),
            database_admin,
            9_999,
            &[
                principal_write_with_name(table_owner, 1, 1, "batch_table_owner"),
                object_write_with_name(table_owner, 8, "batch_table_owner"),
                principal_write_with_name(grant_manager, 1, 1, "batch_grant_manager"),
                object_write_with_name(grant_manager, 8, "batch_grant_manager"),
                principal_write_with_name(schema_admin, 1, 1, "batch_schema_admin"),
                object_write_with_name(schema_admin, 8, "batch_schema_admin"),
                membership_write(schema_admin, database_admin),
                dependency_write(schema_admin, database_admin, 5),
                principal_write_with_name(grantee, 1, 1, "batch_grantee"),
                object_write_with_name(grantee, 8, "batch_grantee"),
                principal_write_with_name(denied_grantee, 1, 1, "batch_denied_grantee"),
                object_write_with_name(denied_grantee, 8, "batch_denied_grantee"),
                principal_write_with_name(grant_first_grantee, 1, 1, "batch_grant_first_grantee"),
                object_write_with_name(grant_first_grantee, 8, "batch_grant_first_grantee"),
            ],
            &[setup_audit],
        )
        .map_err(|error| format!("initialize batch grant authority: {error:?}"))?;
    let schema_invocation = id(0x80);
    let mut schema_audit = identity_audit_event(0x80, schema_admin, table_owner);
    schema_audit.correlation_id = schema_invocation;
    schema_audit.action = AuditAction::Ddl;
    let schema = storage
        .create_schema(
            schema_invocation,
            schema_admin,
            "batch_grant_schema",
            table_owner,
            9_999,
            schema_audit,
        )
        .map_err(|error| format!("create grant-batch schema: {error:?}"))?;
    let table_invocation = id(0x81);
    let mut table_audit = identity_audit_event(0x81, table_owner, id(0x82));
    table_audit.correlation_id = table_invocation;
    let table = storage
        .create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: table_owner,
            schema_id: schema.schema_id,
            name: "batch_grant_table".to_owned(),
            columns: vec![
                TableColumnDefinition {
                    name: "first_value".to_owned(),
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                },
                TableColumnDefinition {
                    name: "second_value".to_owned(),
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                },
            ],
            retention_deadline_us: 9_999,
            audit_event: table_audit,
        })
        .map_err(|error| format!("create grant-batch table: {error:?}"))?;
    let table_id = table.table_id;
    let first_column_id = table.column_ids[0];
    let second_column_id = table.column_ids[1];
    let mut manager_grant_audit = identity_audit_event(0x82, table_owner, grant_manager);
    manager_grant_audit.correlation_id = id(0x83);
    manager_grant_audit.action = AuditAction::AuthorityChange;
    storage
        .set_object_grant(SetObjectGrantRequest {
            invocation_id: id(0x83),
            authenticated_actor_id: table_owner,
            grantee_id: grant_manager,
            object_id: table_id,
            privilege: ObjectGrantPrivilege::ManageGrants,
            target: ObjectGrantTarget::Object,
            grant: true,
            retention_deadline_us: 9_999,
            audit_event: manager_grant_audit,
        })
        .map_err(|error| format!("grant MANAGE_GRANTS for batch race: {error:?}"))?;

    let barrier = Arc::new(Barrier::new(2));
    let (batch_result, revoke_result) = std::thread::scope(|scope| {
        let batch_barrier = barrier.clone();
        let batch_storage = &storage;
        let batch_thread = scope.spawn(move || {
            let mut audit = identity_audit_event(0x86, grant_manager, grantee);
            audit.correlation_id = invocation_id;
            audit.action = AuditAction::AuthorityChange;
            batch_barrier.wait();
            batch_storage.set_object_grants_batch(SetObjectGrantBatchRequest {
                invocation_id,
                authenticated_actor_id: grant_manager,
                grantee_id: grantee,
                object_id: table_id,
                privileges: vec![ObjectGrantPrivilege::Alter, ObjectGrantPrivilege::Index],
                target: ObjectGrantTarget::Object,
                grant: true,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });

        let mut audit = identity_audit_event(0x87, table_owner, grant_manager);
        audit.correlation_id = revoke_id;
        audit.action = AuditAction::AuthorityChange;
        barrier.wait();
        let revoke = storage.set_object_grant(SetObjectGrantRequest {
            invocation_id: revoke_id,
            authenticated_actor_id: table_owner,
            grantee_id: grant_manager,
            object_id: table_id,
            privilege: ObjectGrantPrivilege::ManageGrants,
            target: ObjectGrantTarget::Object,
            grant: false,
            retention_deadline_us: 9_999,
            audit_event: audit,
        });
        let batch = batch_thread
            .join()
            .expect("object-grant batch thread panicked");
        (batch, revoke)
    });

    let batch_committed = match batch_result {
        Ok(_) => true,
        Err(CommitError::DdlAuthorizationDenied | CommitError::Conflict) => false,
        Err(error) => return Err(format!("unexpected batch race result: {error:?}").into()),
    };
    let revoke_sequence =
        revoke_result.map_err(|error| format!("revoke MANAGE_GRANTS during batch: {error:?}"))?;
    assert_eq!(commit_sequence(&database)?, revoke_sequence);

    let alter_key = grant_write(grantee, table_id, 8, 1, table_id, grant_manager).key;
    let index_key = grant_write(grantee, table_id, 9, 1, table_id, grant_manager).key;
    let dependency_key = dependency_write(grantee, table_id, 5).key;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert_eq!(
        catalog.get(alter_key.as_slice())?.is_some(),
        batch_committed
    );
    assert_eq!(
        catalog.get(index_key.as_slice())?.is_some(),
        batch_committed
    );
    assert_eq!(
        catalog.get(dependency_key.as_slice())?.is_some(),
        batch_committed
    );
    assert!(
        catalog
            .get(
                grant_write(grant_manager, table_id, 11, 1, table_id, database_admin)
                    .key
                    .as_slice()
            )?
            .is_none()
    );
    let grantee_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(10, grantee).as_slice())?
            .ok_or("missing batch grantee principal")?
            .value(),
    )?;
    let grantee_epoch = u64::from_le_bytes(
        grantee_record
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .ok_or("missing grantee epoch")?
            .payload
            .as_slice()
            .try_into()?,
    );
    assert_eq!(grantee_epoch, if batch_committed { 2 } else { 1 });
    drop(catalog);
    drop(read);
    assert_eq!(
        lookup_outcome(&database, invocation_id, grant_manager, 1)?
            .ok_or("missing object-grant batch outcome")?
            .status,
        if batch_committed {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        lookup_outcome(&database, revoke_id, table_owner, 1)?
            .ok_or("missing MANAGE_GRANTS revoke outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let mut regrant_audit = identity_audit_event(0x88, table_owner, grant_manager);
    regrant_audit.correlation_id = id(0x88);
    regrant_audit.action = AuditAction::AuthorityChange;
    let regrant_sequence = storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: id(0x88),
        authenticated_actor_id: table_owner,
        grantee_id: grant_manager,
        object_id: table_id,
        privilege: ObjectGrantPrivilege::ManageGrants,
        target: ObjectGrantTarget::Object,
        grant: true,
        retention_deadline_us: 9_999,
        audit_event: regrant_audit,
    })?;
    let grant_first_batch_id = id(0xd0);
    let mut grant_first_batch_audit =
        identity_audit_event(0xd1, grant_manager, grant_first_grantee);
    grant_first_batch_audit.correlation_id = grant_first_batch_id;
    grant_first_batch_audit.action = AuditAction::AuthorityChange;
    let grant_first_batch_sequence =
        storage.set_object_grants_batch(SetObjectGrantBatchRequest {
            invocation_id: grant_first_batch_id,
            authenticated_actor_id: grant_manager,
            grantee_id: grant_first_grantee,
            object_id: table_id,
            privileges: vec![ObjectGrantPrivilege::Select, ObjectGrantPrivilege::Update],
            target: ObjectGrantTarget::Object,
            grant: true,
            retention_deadline_us: 9_999,
            audit_event: grant_first_batch_audit,
        })?;
    assert_eq!(grant_first_batch_sequence, regrant_sequence + 1);
    let mut second_revoke_audit = identity_audit_event(0x89, table_owner, grant_manager);
    second_revoke_audit.correlation_id = id(0x89);
    second_revoke_audit.action = AuditAction::AuthorityChange;
    let second_revoke_sequence = storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: id(0x89),
        authenticated_actor_id: table_owner,
        grantee_id: grant_manager,
        object_id: table_id,
        privilege: ObjectGrantPrivilege::ManageGrants,
        target: ObjectGrantTarget::Object,
        grant: false,
        retention_deadline_us: 9_999,
        audit_event: second_revoke_audit,
    })?;
    assert_eq!(second_revoke_sequence, grant_first_batch_sequence + 1);
    let grant_first_read = database.begin_read()?;
    let grant_first_catalog = grant_first_read.open_table(CATALOG)?;
    for privilege in [2, 4] {
        let grant_key = grant_write(
            grant_first_grantee,
            table_id,
            privilege,
            1,
            table_id,
            grant_manager,
        )
        .key;
        assert!(
            grant_first_catalog.get(grant_key.as_slice())?.is_some(),
            "revoking MANAGE_GRANTS must not remove grants committed earlier"
        );
    }
    let grant_first_dependency_key = dependency_write(grant_first_grantee, table_id, 5).key;
    assert!(
        grant_first_catalog
            .get(grant_first_dependency_key.as_slice())?
            .is_some()
    );
    let grant_first_principal = decode_registered_envelope(
        grant_first_catalog
            .get(id_catalog_key(10, grant_first_grantee).as_slice())?
            .ok_or("missing grant-first grantee principal")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            grant_first_principal
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("missing grant-first grantee epoch")?
                .payload
                .as_slice()
                .try_into()?
        ),
        2
    );
    drop(grant_first_catalog);
    drop(grant_first_read);
    assert_eq!(
        lookup_outcome(&database, grant_first_batch_id, grant_manager, 1)?
            .ok_or("missing grant-first batch outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let denied_batch_id = id(0x8a);
    let mut denied_batch_audit = identity_audit_event(0x8b, grant_manager, denied_grantee);
    denied_batch_audit.correlation_id = denied_batch_id;
    denied_batch_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.set_object_grants_batch(SetObjectGrantBatchRequest {
            invocation_id: denied_batch_id,
            authenticated_actor_id: grant_manager,
            grantee_id: denied_grantee,
            object_id: table_id,
            privileges: vec![ObjectGrantPrivilege::Insert, ObjectGrantPrivilege::Update],
            target: ObjectGrantTarget::Object,
            grant: true,
            retention_deadline_us: 9_999,
            audit_event: denied_batch_audit,
        }),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, second_revoke_sequence);
    assert_eq!(
        lookup_outcome(&database, denied_batch_id, grant_manager, 1)?
            .ok_or("missing revocation-first batch outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let denied_insert_key =
        grant_write(denied_grantee, table_id, 3, 1, table_id, grant_manager).key;
    let denied_update_key =
        grant_write(denied_grantee, table_id, 4, 1, table_id, grant_manager).key;
    let denied_dependency_key = dependency_write(denied_grantee, table_id, 5).key;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(denied_insert_key.as_slice())?.is_none());
    assert!(catalog.get(denied_update_key.as_slice())?.is_none());
    assert!(catalog.get(denied_dependency_key.as_slice())?.is_none());
    let denied_grantee_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(10, denied_grantee).as_slice())?
            .ok_or("missing revocation-first grantee principal")?
            .value(),
    )?;
    let denied_grantee_epoch = u64::from_le_bytes(
        denied_grantee_record
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .ok_or("missing revocation-first grantee epoch")?
            .payload
            .as_slice()
            .try_into()?,
    );
    assert_eq!(denied_grantee_epoch, 1);
    drop(catalog);
    drop(read);

    let mixed_scope_batch_id = id(0x8c);
    let mut mixed_scope_batch_audit = identity_audit_event(0x8d, table_owner, denied_grantee);
    mixed_scope_batch_audit.correlation_id = mixed_scope_batch_id;
    mixed_scope_batch_audit.action = AuditAction::AuthorityChange;
    let mixed_scope_sequence = storage.set_object_grants_batch(SetObjectGrantBatchRequest {
        invocation_id: mixed_scope_batch_id,
        authenticated_actor_id: table_owner,
        grantee_id: denied_grantee,
        object_id: table_id,
        privileges: vec![ObjectGrantPrivilege::Select, ObjectGrantPrivilege::Update],
        target: ObjectGrantTarget::Columns(vec![first_column_id, second_column_id]),
        grant: true,
        retention_deadline_us: 9_999,
        audit_event: mixed_scope_batch_audit,
    })?;
    assert_eq!(mixed_scope_sequence, second_revoke_sequence + 1);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for column_id in [first_column_id, second_column_id] {
        for privilege in [2, 4] {
            let column_grant = grant_write(
                denied_grantee,
                table_id,
                privilege,
                2,
                column_id,
                table_owner,
            )
            .key;
            assert!(catalog.get(column_grant.as_slice())?.is_some());
        }
        let column_grant_dependency = dependency_write(denied_grantee, column_id, 5).key;
        assert!(catalog.get(column_grant_dependency.as_slice())?.is_some());
    }
    for privilege in [2, 4] {
        let table_grant = grant_write(
            denied_grantee,
            table_id,
            privilege,
            1,
            table_id,
            table_owner,
        )
        .key;
        assert!(
            catalog.get(table_grant.as_slice())?.is_none(),
            "an explicit column batch must not expand into table-wide grants"
        );
    }
    let updated_grantee = decode_registered_envelope(
        catalog
            .get(id_catalog_key(10, denied_grantee).as_slice())?
            .ok_or("missing mixed-scope grantee principal")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            updated_grantee
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("missing mixed-scope grantee epoch")?
                .payload
                .as_slice()
                .try_into()?
        ),
        2
    );
    drop(catalog);
    drop(read);
    assert_eq!(
        lookup_outcome(&database, mixed_scope_batch_id, table_owner, 1)?
            .ok_or("missing mixed-scope grant batch outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let mixed_target_entries = vec![
        ObjectGrantEntry {
            privilege: ObjectGrantPrivilege::Alter,
            target: ObjectGrantTarget::Object,
        },
        ObjectGrantEntry {
            privilege: ObjectGrantPrivilege::References,
            target: ObjectGrantTarget::Column(first_column_id),
        },
    ];
    let mixed_target_grant_id = id(0x90);
    let mut mixed_target_grant_audit = identity_audit_event(0x91, table_owner, denied_grantee);
    mixed_target_grant_audit.correlation_id = mixed_target_grant_id;
    mixed_target_grant_audit.action = AuditAction::AuthorityChange;
    let mixed_target_grant_sequence =
        storage.set_object_grant_entries_batch(SetObjectGrantEntriesBatchRequest {
            invocation_id: mixed_target_grant_id,
            authenticated_actor_id: table_owner,
            grantee_id: denied_grantee,
            object_id: table_id,
            entries: mixed_target_entries.clone(),
            grant: true,
            retention_deadline_us: 9_999,
            audit_event: mixed_target_grant_audit,
        })?;
    assert_eq!(mixed_target_grant_sequence, mixed_scope_sequence + 1);
    let mixed_target_grant_read = database.begin_read()?;
    let mixed_target_grant_catalog = mixed_target_grant_read.open_table(CATALOG)?;
    for (privilege, scope, target_id) in [(8, 1, table_id), (6, 2, first_column_id)] {
        let grant_key = grant_write(
            denied_grantee,
            table_id,
            privilege,
            scope,
            target_id,
            table_owner,
        )
        .key;
        assert!(
            mixed_target_grant_catalog
                .get(grant_key.as_slice())?
                .is_some()
        );
    }
    for target_id in [table_id, first_column_id] {
        let dependency_key = dependency_write(denied_grantee, target_id, 5).key;
        assert!(
            mixed_target_grant_catalog
                .get(dependency_key.as_slice())?
                .is_some()
        );
    }
    let mixed_target_grant_principal = decode_registered_envelope(
        mixed_target_grant_catalog
            .get(id_catalog_key(10, denied_grantee).as_slice())?
            .ok_or("missing mixed-target grant principal")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            mixed_target_grant_principal
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("missing mixed-target grant epoch")?
                .payload
                .as_slice()
                .try_into()?
        ),
        3
    );
    drop(mixed_target_grant_catalog);
    drop(mixed_target_grant_read);
    assert_eq!(
        lookup_outcome(&database, mixed_target_grant_id, table_owner, 1)?
            .ok_or("missing mixed-target grant outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let mixed_target_revoke_id = id(0x92);
    let mut mixed_target_revoke_audit = identity_audit_event(0x93, table_owner, denied_grantee);
    mixed_target_revoke_audit.correlation_id = mixed_target_revoke_id;
    mixed_target_revoke_audit.action = AuditAction::AuthorityChange;
    let mixed_target_revoke_sequence =
        storage.set_object_grant_entries_batch(SetObjectGrantEntriesBatchRequest {
            invocation_id: mixed_target_revoke_id,
            authenticated_actor_id: table_owner,
            grantee_id: denied_grantee,
            object_id: table_id,
            entries: mixed_target_entries,
            grant: false,
            retention_deadline_us: 9_999,
            audit_event: mixed_target_revoke_audit,
        })?;
    assert_eq!(
        mixed_target_revoke_sequence,
        mixed_target_grant_sequence + 1
    );
    let mixed_target_revoke_read = database.begin_read()?;
    let mixed_target_revoke_catalog = mixed_target_revoke_read.open_table(CATALOG)?;
    for (privilege, scope, target_id) in [(8, 1, table_id), (6, 2, first_column_id)] {
        let grant_key = grant_write(
            denied_grantee,
            table_id,
            privilege,
            scope,
            target_id,
            table_owner,
        )
        .key;
        assert!(
            mixed_target_revoke_catalog
                .get(grant_key.as_slice())?
                .is_none()
        );
    }
    assert!(
        mixed_target_revoke_catalog
            .get(dependency_write(denied_grantee, table_id, 5).key.as_slice())?
            .is_none()
    );
    assert!(
        mixed_target_revoke_catalog
            .get(
                dependency_write(denied_grantee, first_column_id, 5)
                    .key
                    .as_slice()
            )?
            .is_some()
    );
    let mixed_target_revoke_principal = decode_registered_envelope(
        mixed_target_revoke_catalog
            .get(id_catalog_key(10, denied_grantee).as_slice())?
            .ok_or("missing mixed-target revoke principal")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            mixed_target_revoke_principal
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("missing mixed-target revoke epoch")?
                .payload
                .as_slice()
                .try_into()?
        ),
        4
    );
    drop(mixed_target_revoke_catalog);
    drop(mixed_target_revoke_read);
    assert_eq!(
        lookup_outcome(&database, mixed_target_revoke_id, table_owner, 1)?
            .ok_or("missing mixed-target revoke outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let partial_revoke_id = id(0x8e);
    let mut partial_revoke_audit = identity_audit_event(0x8f, table_owner, denied_grantee);
    partial_revoke_audit.correlation_id = partial_revoke_id;
    partial_revoke_audit.action = AuditAction::AuthorityChange;
    let partial_revoke_sequence = storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: partial_revoke_id,
        authenticated_actor_id: table_owner,
        grantee_id: denied_grantee,
        object_id: table_id,
        privilege: ObjectGrantPrivilege::Select,
        target: ObjectGrantTarget::Column(first_column_id),
        grant: false,
        retention_deadline_us: 9_999,
        audit_event: partial_revoke_audit,
    })?;
    assert_eq!(partial_revoke_sequence, mixed_target_revoke_sequence + 1);
    let partial_revoke_read = database.begin_read()?;
    let partial_revoke_catalog = partial_revoke_read.open_table(CATALOG)?;
    let removed_select =
        grant_write(denied_grantee, table_id, 2, 2, first_column_id, table_owner).key;
    assert!(
        partial_revoke_catalog
            .get(removed_select.as_slice())?
            .is_none()
    );
    for (column_id, privilege) in [
        (first_column_id, 4),
        (second_column_id, 2),
        (second_column_id, 4),
    ] {
        let grant_key = grant_write(
            denied_grantee,
            table_id,
            privilege,
            2,
            column_id,
            table_owner,
        )
        .key;
        let grant_bytes = partial_revoke_catalog
            .get(grant_key.as_slice())?
            .ok_or("partial column revoke removed a sibling grant")?
            .value()
            .to_vec();
        let grant_record = decode_registered_envelope(&grant_bytes)?;
        assert_eq!(
            grant_record
                .fields
                .iter()
                .find(|field| field.tag == 1)
                .ok_or("surviving grant lost its grantor provenance")?
                .payload,
            table_owner
        );
    }
    let retained_first_column_dependency = dependency_write(denied_grantee, first_column_id, 5).key;
    assert!(
        partial_revoke_catalog
            .get(retained_first_column_dependency.as_slice())?
            .is_some()
    );
    let partial_revoke_principal = decode_registered_envelope(
        partial_revoke_catalog
            .get(id_catalog_key(10, denied_grantee).as_slice())?
            .ok_or("missing partially revoked grantee principal")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            partial_revoke_principal
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("missing partial-revoke principal epoch")?
                .payload
                .as_slice()
                .try_into()?
        ),
        5
    );
    drop(partial_revoke_catalog);
    drop(partial_revoke_read);
    assert_eq!(
        lookup_outcome(&database, partial_revoke_id, table_owner, 1)?
            .ok_or("missing partial column revoke outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let mut entries_race_regrant_audit = identity_audit_event(0xe0, table_owner, grant_manager);
    entries_race_regrant_audit.correlation_id = id(0xe0);
    entries_race_regrant_audit.action = AuditAction::AuthorityChange;
    storage.set_object_grant(SetObjectGrantRequest {
        invocation_id: id(0xe0),
        authenticated_actor_id: table_owner,
        grantee_id: grant_manager,
        object_id: table_id,
        privilege: ObjectGrantPrivilege::ManageGrants,
        target: ObjectGrantTarget::Object,
        grant: true,
        retention_deadline_us: 9_999,
        audit_event: entries_race_regrant_audit,
    })?;

    let entries_race_batch_id = id(0xe1);
    let entries_race_revoke_id = id(0xe2);
    let entries_race = vec![
        ObjectGrantEntry {
            privilege: ObjectGrantPrivilege::Alter,
            target: ObjectGrantTarget::Object,
        },
        ObjectGrantEntry {
            privilege: ObjectGrantPrivilege::References,
            target: ObjectGrantTarget::Column(second_column_id),
        },
    ];
    let entries_race_before = commit_sequence(&database)?;
    let entries_race_barrier = Arc::new(Barrier::new(2));
    let (entries_batch_result, entries_revoke_result) = std::thread::scope(|scope| {
        let batch_barrier = Arc::clone(&entries_race_barrier);
        let batch_storage = &storage;
        let entries = entries_race.clone();
        let batch = scope.spawn(move || {
            let mut audit = identity_audit_event(0xe3, grant_manager, grant_first_grantee);
            audit.correlation_id = entries_race_batch_id;
            audit.action = AuditAction::AuthorityChange;
            batch_barrier.wait();
            batch_storage.set_object_grant_entries_batch(SetObjectGrantEntriesBatchRequest {
                invocation_id: entries_race_batch_id,
                authenticated_actor_id: grant_manager,
                grantee_id: grant_first_grantee,
                object_id: table_id,
                entries,
                grant: true,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        let mut audit = identity_audit_event(0xe4, table_owner, grant_manager);
        audit.correlation_id = entries_race_revoke_id;
        audit.action = AuditAction::AuthorityChange;
        entries_race_barrier.wait();
        let revoke = storage.set_object_grant(SetObjectGrantRequest {
            invocation_id: entries_race_revoke_id,
            authenticated_actor_id: table_owner,
            grantee_id: grant_manager,
            object_id: table_id,
            privilege: ObjectGrantPrivilege::ManageGrants,
            target: ObjectGrantTarget::Object,
            grant: false,
            retention_deadline_us: 9_999,
            audit_event: audit,
        });
        (
            batch.join().expect("mixed-entry grant thread panicked"),
            revoke,
        )
    });
    let entries_batch_committed = match entries_batch_result {
        Ok(_) => true,
        Err(CommitError::DdlAuthorizationDenied | CommitError::Conflict) => false,
        Err(error) => return Err(format!("unexpected mixed-entry race result: {error:?}").into()),
    };
    let entries_revoke_sequence = entries_revoke_result?;
    assert_eq!(
        entries_revoke_sequence,
        entries_race_before + 1 + u64::from(entries_batch_committed)
    );
    assert_eq!(commit_sequence(&database)?, entries_revoke_sequence);
    assert_eq!(
        lookup_outcome(&database, entries_race_batch_id, grant_manager, 1)?
            .ok_or("missing mixed-entry grant outcome")?
            .status,
        if entries_batch_committed {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(
        lookup_outcome(&database, entries_race_revoke_id, table_owner, 1)?
            .ok_or("missing mixed-entry MANAGE_GRANTS revoke outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let entries_race_read = database.begin_read()?;
    let entries_race_catalog = entries_race_read.open_table(CATALOG)?;
    let alter_key = grant_write(grant_first_grantee, table_id, 8, 1, table_id, grant_manager).key;
    let references_key = grant_write(
        grant_first_grantee,
        table_id,
        6,
        2,
        second_column_id,
        grant_manager,
    )
    .key;
    let references_dependency = dependency_write(grant_first_grantee, second_column_id, 5).key;
    for key in [&alter_key, &references_key, &references_dependency] {
        assert_eq!(
            entries_race_catalog.get(key.as_slice())?.is_some(),
            entries_batch_committed,
            "mixed-scope grants and their dependency must publish all-or-none"
        );
    }
    if entries_batch_committed {
        for grant_key in [&alter_key, &references_key] {
            let grant = decode_registered_envelope(
                entries_race_catalog
                    .get(grant_key.as_slice())?
                    .ok_or("committed mixed-scope grant missing")?
                    .value(),
            )?;
            assert_eq!(
                grant
                    .fields
                    .iter()
                    .find(|field| field.tag == 1)
                    .ok_or("mixed-scope grantor provenance missing")?
                    .payload,
                grant_manager
            );
        }
    }
    let entries_grantee = decode_registered_envelope(
        entries_race_catalog
            .get(id_catalog_key(10, grant_first_grantee).as_slice())?
            .ok_or("mixed-entry race grantee disappeared")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            entries_grantee
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("mixed-entry race grantee epoch missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        if entries_batch_committed { 3 } else { 2 }
    );
    drop(entries_race_catalog);
    drop(entries_race_read);
    assert!(prdb_engine::startup::validate_persisted_records(&database).is_ok());
    Ok(())
}

pub(super) fn role_membership_revocation_aborts_a_staged_role_authorized_writer()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{
        AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Effect,
        Expr, Grant, Operation, Policy, Subject,
    };
    use prdb_engine::storage_manager::RoleMembershipChange;
    use prdb_engine::tuple::FieldPatch;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let user_id = id(0x91);
    let role_id = id(0x92);
    let invocation_id = id(0x93);
    let membership_invocation_id = id(0x94);
    let admin_role = builtin_owner_id();
    let row_id = id(0x95);
    let row_key = table_row_key(id(16), row_id);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[0x96; 16]];

    let mut setup_audit = audit_event(0x97);
    setup_audit.actor_id = Some(admin_role);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x97),
        admin_role,
        9_999,
        &[
            principal_write_with_name(user_id, 1, 1, "membership_writer"),
            object_write_with_name(user_id, 8, "membership_writer"),
            credential_verifier_write(id(0x9a), user_id, &[0x9a; 32], 1, i64::MAX, None, 1),
            membership_write(user_id, admin_role),
            dependency_write(user_id, admin_role, 5),
            principal_write_with_name(role_id, 2, 1, "membership_editor"),
            object_write_with_name(role_id, 9, "membership_editor"),
            membership_write(user_id, role_id),
            dependency_write(user_id, role_id, 5),
            grant_write(role_id, id(16), 4, 1, id(16), admin_role),
            dependency_write(role_id, id(16), 5),
        ],
        &[setup_audit],
    )?;

    let rename_invocation_id = id(0x9b);
    let mut rename_audit = identity_audit_event(0x9c, user_id, role_id);
    rename_audit.correlation_id = rename_invocation_id;
    let rename_sequence = storage.rename_identity(
        rename_invocation_id,
        user_id,
        role_id,
        "membership_editor_renamed",
        9_999,
        rename_audit,
    )?;
    assert_eq!(rename_sequence, 2);

    commit_rows(
        &database,
        id(0x98),
        user_id,
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text(
                    "before membership revoke".to_owned(),
                ))],
            )?),
        }],
    )?;

    let grant_key = grant_write(role_id, id(16), 4, 1, id(16), admin_role).key;
    let grant_bytes = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(grant_key.as_slice())?
        .ok_or("missing role UPDATE grant")?
        .value()
        .to_vec();
    let (mut writer, role_ids) = storage
        .begin_authenticated_invocation(invocation_id, user_id, 9_999, 1, 1024)
        .map_err(|error| format!("admit membership writer: {error:?}"))?;
    assert!(role_ids.contains(&role_id));
    assert!(role_ids.contains(&admin_role));
    let context = AuthorizationContext {
        principal_id: user_id,
        role_ids,
        scope: [
            (Operation::Usage, AccessTarget::Schema(id(16))),
            (Operation::Select, AccessTarget::Table(id(16))),
            (Operation::Update, AccessTarget::Table(id(16))),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: id(16),
                    column_id: column_ids[0],
                },
            ),
            (
                Operation::Update,
                AccessTarget::Column {
                    table_id: id(16),
                    column_id: column_ids[0],
                },
            ),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Usage,
            target: AccessTarget::Schema(id(16)),
        },
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Select,
            target: AccessTarget::Table(id(16)),
        },
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Update,
            target: AccessTarget::Table(id(16)),
        },
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
        },
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role(role_id)],
            operation: Operation::Select,
            target: AccessTarget::Table(id(16)),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role(role_id)],
            operation: Operation::Update,
            target: AccessTarget::Table(id(16)),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Role(role_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role(role_id)],
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: id(16),
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[AuthorityDependency {
            key: grant_key,
            expected_value: Some(grant_bytes),
        }],
    };
    writer
        .stage_patch_authorized(
            prdb_engine::overlay::RowId {
                table: id(16),
                row: row_id,
            },
            &projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text(
                    "stale after membership revoke".to_owned(),
                )),
            }],
        )
        .map_err(|error| format!("stage membership-authorized writer: {error:?}"))?;

    let mut revoke_audit = audit_event(0x99);
    revoke_audit.actor_id = Some(user_id);
    revoke_audit.correlation_id = membership_invocation_id;
    revoke_audit.action = AuditAction::AuthorityChange;
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, revoke_result) = std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer_task = scope.spawn(move || {
            writer_barrier.wait();
            writer.commit()
        });
        let revoke_barrier = barrier.clone();
        let revoke_storage = &storage;
        let revoke_task = scope.spawn(move || {
            revoke_barrier.wait();
            revoke_storage.set_role_membership(
                membership_invocation_id,
                user_id,
                RoleMembershipChange {
                    member_id: user_id,
                    role_id,
                    grant: false,
                },
                9_999,
                revoke_audit,
            )
        });
        barrier.wait();
        (writer_task.join().unwrap(), revoke_task.join().unwrap())
    });
    let writer_status = match writer_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        other => panic!("unexpected role-authorized writer result: {other:?}"),
    };
    let revoke_sequence =
        revoke_result.map_err(|error| format!("revoke role membership: {error:?}"))?;
    assert_eq!(
        revoke_sequence,
        if writer_status == OutcomeStatus::Committed {
            5
        } else {
            4
        }
    );

    let stored_row = database
        .begin_read()?
        .open_table(ROWS)?
        .get(&row_key[..])?
        .ok_or("row disappeared after membership revoke")?
        .value()
        .to_vec();
    assert_eq!(
        decode_tuple(&columns, 1, &RowRecord::decode(&stored_row)?.payload)?.values,
        [Some(ScalarValue::Text(
            if writer_status == OutcomeStatus::Committed {
                "stale after membership revoke"
            } else {
                "before membership revoke"
            }
            .to_owned()
        ))]
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, user_id, 1)?
            .ok_or("missing aborted writer outcome")?
            .status,
        writer_status,
    );
    assert_eq!(
        lookup_outcome(&database, membership_invocation_id, user_id, 1)?
            .ok_or("missing membership revoke outcome")?
            .status,
        OutcomeStatus::Committed,
    );
    assert_eq!(commit_sequence(&database)?, revoke_sequence);

    let regrant_invocation_id = id(0xa1);
    let mut regrant_audit = audit_event(0xa2);
    regrant_audit.actor_id = Some(user_id);
    regrant_audit.correlation_id = regrant_invocation_id;
    regrant_audit.action = AuditAction::AuthorityChange;
    let regrant_sequence = storage.set_role_membership(
        regrant_invocation_id,
        user_id,
        RoleMembershipChange {
            member_id: user_id,
            role_id,
            grant: true,
        },
        9_999,
        regrant_audit,
    )?;
    let rename_race_writer_id = id(0xa3);
    let rename_race_invocation_id = id(0xa4);
    let (mut rename_race_writer, rename_race_roles) =
        storage.begin_authenticated_invocation(rename_race_writer_id, user_id, 9_999, 1, 1024)?;
    assert!(rename_race_roles.contains(&role_id));
    let rename_race_context = AuthorizationContext {
        principal_id: user_id,
        role_ids: rename_race_roles,
        scope: context.scope.clone(),
    };
    let rename_race_projection = AuthorizedProjection {
        context: &rename_race_context,
        ..projection
    };
    rename_race_writer.stage_patch_authorized(
        prdb_engine::overlay::RowId {
            table: id(16),
            row: row_id,
        },
        &rename_race_projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Text("write racing role rename".to_owned())),
        }],
    )?;
    let mut rename_race_audit = identity_audit_event(0xa5, user_id, role_id);
    rename_race_audit.correlation_id = rename_race_invocation_id;
    let rename_race_barrier = Arc::new(Barrier::new(3));
    let (rename_writer_result, rename_result) = std::thread::scope(|scope| {
        let writer_barrier = rename_race_barrier.clone();
        let writer_task = scope.spawn(move || {
            writer_barrier.wait();
            rename_race_writer.commit()
        });
        let rename_barrier = rename_race_barrier.clone();
        let rename_storage = &storage;
        let rename_task = scope.spawn(move || {
            rename_barrier.wait();
            rename_storage.rename_identity(
                rename_race_invocation_id,
                user_id,
                role_id,
                "membership_editor_final",
                9_999,
                rename_race_audit,
            )
        });
        rename_race_barrier.wait();
        (writer_task.join().unwrap(), rename_task.join().unwrap())
    });
    let rename_writer_status = match rename_writer_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        other => panic!("unexpected role-rename writer result: {other:?}"),
    };
    let rename_sequence =
        rename_result.map_err(|error| format!("rename role during staged write: {error:?}"))?;
    assert!(rename_sequence >= regrant_sequence);
    assert_eq!(
        row(&database, &row_key)?
            .map(|row| decode_tuple(&columns, 1, &row.payload))
            .transpose()?
            .ok_or("missing role-authorized row after role rename race")?
            .values,
        [Some(ScalarValue::Text(
            if rename_writer_status == OutcomeStatus::Committed {
                "write racing role rename"
            } else if writer_status == OutcomeStatus::Committed {
                "stale after membership revoke"
            } else {
                "before membership revoke"
            }
            .to_owned()
        ))]
    );
    assert_eq!(
        lookup_outcome(&database, rename_race_writer_id, user_id, 1)?
            .ok_or("missing role-rename writer outcome")?
            .status,
        rename_writer_status
    );
    assert_eq!(
        lookup_outcome(&database, rename_race_invocation_id, user_id, 1)?
            .ok_or("missing role-rename outcome")?
            .status,
        OutcomeStatus::Committed
    );
    Ok(())
}
