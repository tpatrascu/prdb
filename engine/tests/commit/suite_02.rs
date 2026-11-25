use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::catalog_index::lookup_relation_id_by_name;
use prdb_engine::commit::{
    CatalogWrite, CommitError, IndexWrite, RowWrite, commit_catalog_and_audit, commit_rows,
    commit_rows_and_indexes,
};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ROWS};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::{ReadableDatabase, ReadableTable};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_drop_table_removes_owned_catalog_and_rows()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::Expr;
    use prdb_engine::catalog::{PolicyTarget, encode_policy_target};
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefault, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0x40);
    let grantee = id(0x47);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "table_drop_owner"),
        object_write_with_name(actor, 8, "table_drop_owner"),
        principal_write_with_name(grantee, 1, 1, "table_drop_grantee"),
        object_write_with_name(grantee, 8, "table_drop_grantee"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x40, admin_role, id(0x4f));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x4f), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x41);
    let mut schema_audit = identity_audit_event(0x42, actor, id(0x43));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage
        .create_schema(
            schema_invocation,
            actor,
            "drop_table_schema",
            actor,
            9_999,
            schema_audit,
        )
        .map_err(|error| format!("create grant-batch schema: {error:?}"))?;
    let create_invocation = id(0x44);
    let mut create_audit = identity_audit_event(0x45, actor, id(0x46));
    create_audit.correlation_id = create_invocation;
    let created = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "drop_me".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "label".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: Some(TableColumnDefault::Literal(Some(ScalarValue::Text(
                "default".to_owned(),
            )))),
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    assert_eq!(
        storage.collect_committed_table_population(created.table_id)?,
        prdb_engine::statistics::StatisticsCollectionOutcome::Exact
    );
    let mut committed_population_key = b"statistics:committed-population:v1:".to_vec();
    committed_population_key.extend_from_slice(&created.table_id);
    assert!(
        database
            .begin_read()?
            .open_table(schema::STATS)?
            .get(committed_population_key.as_slice())?
            .is_some()
    );
    let table_grant = grant_write(grantee, created.table_id, 2, 1, created.table_id, actor);
    let grant_edge = dependency_write(grantee, created.table_id, 5);
    let mut advanced_grantee = principal_write_with_name(grantee, 1, 2, "table_drop_grantee");
    advanced_grantee.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(10, grantee).as_slice())?
            .ok_or("table-drop grant recipient missing")?
            .value()
            .to_vec(),
    );
    let grant_invocation = id(0x5a);
    let mut grant_audit = identity_audit_event(0x5b, actor, created.table_id);
    grant_audit.action = AuditAction::AuthorityChange;
    grant_audit.correlation_id = grant_invocation;
    storage.commit_authorized_object_grant_ddl(
        grant_invocation,
        actor,
        9_999,
        &[table_grant.clone(), grant_edge.clone(), advanced_grantee],
        &[grant_audit],
    )?;
    let policy_id = id(0x5c);
    let expression_id_for_policy = id(0x5d);
    let policy_key = id_catalog_key(13, policy_id);
    let policy_payload = encode_envelope(
        &family_schema(13).unwrap(),
        &policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: created.table_id.to_vec(),
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
                payload: expression_id_for_policy.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let policy_object = object_write_with_parent_and_owner_and_name(
        policy_id,
        10,
        None,
        actor,
        "drop_table_policy",
    );
    let policy_expression = expression_write(
        expression_id_for_policy,
        3,
        &[],
        &Expr::Literal(Some(ScalarValue::Boolean(true))),
    );
    let expression_object = object_write_with_parent_and_owner_and_name(
        expression_id_for_policy,
        5,
        None,
        actor,
        "drop_table_policy_expression",
    );
    let policy_target_payload = encode_policy_target(&PolicyTarget {
        policy_id,
        kind: 2,
        target_id: admin_role,
    })?;
    let policy_target = CatalogWrite {
        key: policy_target_payload[32..].to_vec(),
        expected_value: None,
        payload: Some(policy_target_payload),
    };
    let policy_expression_edge = dependency_write(policy_id, expression_id_for_policy, 2);
    let policy_target_edge = dependency_write(policy_id, admin_role, 5);
    let mut table_epoch_update =
        table_write_with_versions(created.table_id, schema.schema_id, 1, 2);
    table_epoch_update.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, created.table_id).as_slice())?
            .ok_or("table missing before policy setup")?
            .value()
            .to_vec(),
    );
    let policy_invocation = id(0x5e);
    let mut policy_audit = identity_audit_event(0x5f, actor, policy_id);
    policy_audit.correlation_id = policy_invocation;
    policy_audit.target_ids = vec![policy_id];
    storage.commit_authorized_policy_ddl(
        policy_invocation,
        actor,
        9_999,
        &[
            CatalogWrite {
                key: policy_key.clone(),
                expected_value: None,
                payload: Some(policy_payload),
            },
            policy_object,
            policy_expression,
            expression_object,
            policy_target.clone(),
            policy_expression_edge.clone(),
            policy_target_edge.clone(),
            table_epoch_update,
        ],
        &[policy_audit],
    )?;
    let check_constraint_id = id(0x60);
    let check_expression_id = id(0x61);
    let check_constraint_key = id_catalog_key(8, check_constraint_id);
    let check_constraint_payload = encode_envelope(
        &family_schema(8).unwrap(),
        &check_constraint_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: created.table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 4u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: check_expression_id.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
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
    let check_constraint_object = object_write_with_parent_and_owner_and_name(
        check_constraint_id,
        6,
        None,
        actor,
        "drop_table_check",
    );
    let check_expression_key = id_catalog_key(7, check_expression_id);
    let check_expression = CatalogWrite {
        key: check_expression_key.clone(),
        expected_value: None,
        payload: Some(encode_envelope(
            &family_schema(7).unwrap(),
            &check_expression_key,
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
                    payload: prdb_engine::expression::encode_expression(&Expr::Literal(Some(
                        ScalarValue::Boolean(true),
                    )))?,
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Ids,
                    payload: 0u32.to_le_bytes().to_vec(),
                },
            ],
        )?),
    };
    let check_expression_object = object_write_with_parent_and_owner_and_name(
        check_expression_id,
        5,
        None,
        actor,
        "drop_table_check_expression",
    );
    let check_expression_edge = dependency_write(check_constraint_id, check_expression_id, 2);
    let mut table_constraint_epoch =
        table_write_with_versions(created.table_id, schema.schema_id, 1, 3);
    table_constraint_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, created.table_id).as_slice())?
            .ok_or("table missing before CHECK setup")?
            .value()
            .to_vec(),
    );
    let constraint_invocation = id(0x62);
    let mut constraint_audit = identity_audit_event(0x63, actor, check_constraint_id);
    constraint_audit.correlation_id = constraint_invocation;
    constraint_audit.target_ids = vec![check_constraint_id];
    storage.commit_authorized_constraint_ddl(
        constraint_invocation,
        actor,
        9_999,
        &[
            CatalogWrite {
                key: check_constraint_key.clone(),
                expected_value: None,
                payload: Some(check_constraint_payload),
            },
            check_constraint_object,
            check_expression,
            check_expression_object,
            check_expression_edge.clone(),
            table_constraint_epoch,
        ],
        &[constraint_audit],
    )?;
    let payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Text("stored".to_owned()))],
    )?;
    let row_writes = (1u64..=65_537)
        .map(|row_number| {
            let mut row_id = [0; 16];
            row_id[8..].copy_from_slice(&row_number.to_be_bytes());
            RowWrite {
                key: table_row_key(created.table_id, row_id),
                expected_version: None,
                payload: Some(payload.clone()),
            }
        })
        .collect::<Vec<_>>();
    let row_key = row_writes[0].key;
    let read = database.begin_read()?;
    let column_record = read
        .open_table(CATALOG)?
        .get(id_catalog_key(6, created.column_ids[0]).as_slice())?
        .ok_or("created column missing")?;
    let column = decode_registered_envelope(column_record.value())?;
    let expression_id: [u8; 16] = column
        .fields
        .iter()
        .find(|field| field.tag == 7)
        .ok_or("default expression missing")?
        .payload
        .as_slice()
        .try_into()?;
    drop(read);
    let before_drop_limit = commit_rows(&database, id(0x54), actor, 9_999, &row_writes[..65_536])?;
    let over_limit_sequence = commit_rows(
        &database,
        id(0x55),
        actor,
        9_999,
        &[row_writes[65_536].clone()],
    )?;

    let denied_invocation = id(0x48);
    let mut denied_audit = identity_audit_event(0x4b, id(0x4c), id(0x4e));
    denied_audit.correlation_id = denied_invocation;
    let denied = storage.drop_table(
        denied_invocation,
        id(0x4c),
        created.table_id,
        9_999,
        denied_audit,
    );
    assert!(matches!(denied, Err(TableLifecycleError::Unauthorized)));

    let over_limit_drop_invocation = id(0x56);
    let mut over_limit_drop_audit = identity_audit_event(0x57, actor, created.table_id);
    over_limit_drop_audit.correlation_id = over_limit_drop_invocation;
    assert_eq!(
        storage.drop_table(
            over_limit_drop_invocation,
            actor,
            created.table_id,
            9_999,
            over_limit_drop_audit,
        ),
        Err(TableLifecycleError::Commit(
            CommitError::TableDropLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, over_limit_sequence);
    assert_eq!(
        lookup_outcome(&database, over_limit_drop_invocation, actor, 1)?
            .ok_or("missing table-drop row-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert!(
        database
            .begin_read()?
            .open_table(schema::STATS)?
            .get(committed_population_key.as_slice())?
            .is_some(),
        "an aborted DROP TABLE must preserve its population record"
    );
    let last_row_key = row_writes[65_536].key;
    assert_eq!(
        commit_rows(
            &database,
            id(0x58),
            actor,
            9_999,
            &[RowWrite {
                key: last_row_key,
                expected_version: Some(over_limit_sequence),
                payload: None,
            }],
        )?,
        before_drop_limit + 2
    );

    let stale_writer_invocation = id(0x52);
    let stale_row_id = id(0x53);
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(created.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_payload = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Text(
            "staged before table drop".to_owned(),
        ))],
    )?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: created.table_id,
            row: stale_row_id,
        },
        &stale_payload,
    )?;
    let retained_snapshot = database.begin_read()?;
    let retained_name_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_relation_id_by_name(&retained_name_snapshot, schema.schema_id, "drop_me")?,
        Some(created.table_id)
    );

    let drop_invocation = id(0x50);
    let mut drop_audit = identity_audit_event(0x49, actor, id(0x4a));
    drop_audit.correlation_id = drop_invocation;
    let dropped = storage
        .drop_table(drop_invocation, actor, created.table_id, 9_999, drop_audit)
        .map_err(|error| format!("table drop failed: {error:?}"))?;
    assert_eq!(dropped.table_id, created.table_id);
    assert!(
        database
            .begin_read()?
            .open_table(schema::STATS)?
            .get(committed_population_key.as_slice())?
            .is_none(),
        "DROP TABLE must remove its protected population record"
    );
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(created.table_id, stale_row_id))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale table-drop writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let retained_catalog = retained_snapshot.open_table(CATALOG)?;
    assert!(
        retained_catalog
            .get(id_catalog_key(5, created.table_id).as_slice())?
            .is_some()
    );
    let table_object_key = id_catalog_key(2, created.table_id);
    let table_record_key = id_catalog_key(5, created.table_id);
    let retired_table_object_bytes = retained_catalog
        .get(table_object_key.as_slice())?
        .ok_or("retained reader lost its pre-drop table object")?
        .value()
        .to_vec();
    let retired_table_record_bytes = retained_catalog
        .get(table_record_key.as_slice())?
        .ok_or("retained reader lost its pre-drop table record")?
        .value()
        .to_vec();
    drop(retained_catalog);
    let current_read = database.begin_read()?;
    let current_catalog = current_read.open_table(CATALOG)?;
    assert!(
        current_catalog
            .get(id_catalog_key(17, created.table_id).as_slice())?
            .is_some(),
        "DROP TABLE must permanently reserve its retired object ID"
    );
    for retired_id in [
        created.column_ids[0],
        expression_id,
        policy_id,
        expression_id_for_policy,
        check_constraint_id,
        check_expression_id,
    ] {
        let reservation = current_catalog
            .get(id_catalog_key(17, retired_id).as_slice())?
            .ok_or("DROP TABLE must reserve every retired owned object ID")?;
        assert!(
            decode_registered_envelope(reservation.value())?
                .fields
                .is_empty()
        );
        drop(reservation);
    }
    let reuse_invocation = id(0x64);
    let mut reuse_audit = identity_audit_event(0x65, actor, created.table_id);
    reuse_audit.correlation_id = reuse_invocation;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            reuse_invocation,
            actor,
            9_999,
            &[
                CatalogWrite {
                    key: table_object_key,
                    expected_value: None,
                    payload: Some(retired_table_object_bytes),
                },
                CatalogWrite {
                    key: table_record_key,
                    expected_value: None,
                    payload: Some(retired_table_record_bytes),
                },
            ],
            &[reuse_audit],
        ),
        Err(CommitError::InvalidCatalog),
        "a dropped table ID must not be reusable by a later catalog commit"
    );
    drop(current_catalog);
    let retained_rows = prdb_engine::snapshot::RowSnapshot::new(retained_snapshot);
    let retained_row = RowRecord::decode(
        &retained_rows
            .get(&row_key)?
            .ok_or("retained reader lost its pre-drop row")?,
    )?;
    let retained_tuple = decode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        }],
        1,
        &retained_row.payload,
    )?;
    assert_eq!(
        retained_tuple.values,
        vec![Some(ScalarValue::Text("stored".to_owned()))]
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(2, created.table_id),
        id_catalog_key(5, created.table_id),
        id_catalog_key(2, created.column_ids[0]),
        id_catalog_key(6, created.column_ids[0]),
        id_catalog_key(2, expression_id),
        id_catalog_key(7, expression_id),
        table_grant.key,
        grant_edge.key,
        policy_key,
        id_catalog_key(2, policy_id),
        id_catalog_key(7, expression_id_for_policy),
        policy_target.key,
        policy_expression_edge.key,
        policy_target_edge.key,
        check_constraint_key,
        id_catalog_key(2, check_constraint_id),
        id_catalog_key(7, check_expression_id),
        id_catalog_key(2, check_expression_id),
        check_expression_edge.key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "record remained: {key:?}"
        );
    }
    let grantee_key = id_catalog_key(10, grantee);
    let grantee_record = decode_registered_envelope(
        catalog
            .get(grantee_key.as_slice())?
            .ok_or("table-drop grant recipient missing after cleanup")?
            .value(),
    )?;
    let grantee_epoch = grantee_record
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("table-drop grant recipient epoch missing")?;
    assert_eq!(
        u64::from_le_bytes(grantee_epoch.payload.as_slice().try_into()?),
        3
    );
    assert!(read.open_table(ROWS)?.get(&row_key[..])?.is_none());
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(read);

    let recreate_invocation = id(0x65);
    let mut recreate_audit = identity_audit_event(0x66, actor, id(0x67));
    recreate_audit.correlation_id = recreate_invocation;
    let recreated = storage.create_table(CreateTableRequest {
        invocation_id: recreate_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "drop_me".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "label".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: recreate_audit,
    })?;
    assert_ne!(recreated.table_id, created.table_id);
    assert_eq!(
        lookup_relation_id_by_name(&retained_name_snapshot, schema.schema_id, "drop_me")?,
        Some(created.table_id),
        "the pre-drop snapshot must retain the original relation identity"
    );
    let current_snapshot = database.begin_read()?;
    assert_eq!(
        lookup_relation_id_by_name(&current_snapshot, schema.schema_id, "drop_me")?,
        Some(recreated.table_id)
    );
    assert!(row(&database, &table_row_key(created.table_id, id(0x53)))?.is_none());
    assert_eq!(
        lookup_outcome(&database, recreate_invocation, actor, 1)?
            .ok_or("missing table-recreation outcome")?
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_drop_table_removes_owned_primary_and_unique_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x6e);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x6f, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x6f),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "unique_drop_owner"),
            object_write_with_name(actor, 8, "unique_drop_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x70);
    let mut schema_audit = identity_audit_event(0x71, actor, id(0x72));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "unique_drop_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x73);
    let mut create_audit = identity_audit_event(0x74, actor, id(0x75));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "unique_drop_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let not_null_invocation = id(0x80);
    let mut not_null_audit = identity_audit_event(0x81, actor, table.column_ids[0]);
    not_null_audit.correlation_id = not_null_invocation;
    storage.set_column_nullable(
        not_null_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        false,
        9_999,
        not_null_audit,
    )?;
    let not_null_constraint_id = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let prefix = 8u16.to_be_bytes();
        let mut constraint_id = None;
        for entry in catalog.range(prefix.as_slice()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            let record = decode_registered_envelope(value.value())?;
            if record
                .fields
                .iter()
                .any(|field| field.tag == 1 && field.payload.as_slice() == table.table_id)
                && record
                    .fields
                    .iter()
                    .any(|field| field.tag == 2 && field.payload.as_slice() == 1u16.to_le_bytes())
                && record.fields.iter().any(|field| {
                    field.tag == 3
                        && field.payload[4..]
                            .chunks_exact(16)
                            .any(|id| id == table.column_ids[0])
                })
            {
                constraint_id = Some(<[u8; 16]>::try_from(&key.value()[3..19])?);
                break;
            }
        }
        constraint_id.ok_or("missing generated NOT NULL constraint")?
    };

    let unique_constraint_id = id(0x76);
    let unique_index_id = id(0x77);
    let primary_constraint_id = id(0x7e);
    let primary_index_id = id(0x7f);
    let unique_constraint = constraint_write(
        unique_constraint_id,
        table.table_id,
        3,
        &table.column_ids,
        None,
        &[],
        Some(unique_index_id),
    );
    let primary_constraint = constraint_write(
        primary_constraint_id,
        table.table_id,
        2,
        &table.column_ids,
        None,
        &[],
        Some(primary_index_id),
    );
    let (unique_index_key, unique_index_payload) = owned_unique_index_definition(
        unique_index_id,
        table.table_id,
        &table.column_ids,
        unique_constraint_id,
    );
    let (primary_index_key, primary_index_payload) = owned_unique_index_definition(
        primary_index_id,
        table.table_id,
        &table.column_ids,
        primary_constraint_id,
    );
    let stale_constraint_invocation = id(0x82);
    let stale_constraint_row = id(0x83);
    let mut stale_constraint_writer =
        storage.begin_invocation(stale_constraint_invocation, actor, 9_999, 1, 1024)?;
    let stale_constraint_schema =
        stale_constraint_writer.load_catalog_table_schema(table.table_id)?;
    stale_constraint_writer.load_catalog_indexes(&stale_constraint_schema)?;
    let stale_constraint_tuple = encode_tuple(
        &stale_constraint_schema.columns,
        stale_constraint_schema.schema_version,
        &[Some(ScalarValue::Integer(51))],
    )?;
    stale_constraint_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: stale_constraint_row,
        },
        &stale_constraint_tuple,
    )?;
    let table_key = id_catalog_key(5, table.table_id);
    let current_table = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(table_key.as_slice())?
        .ok_or("unique table record missing before constraint add")?
        .value()
        .to_vec();
    let mut table_epoch = table_write_with_versions(table.table_id, schema.schema_id, 1, 3);
    table_epoch.expected_value = Some(current_table);
    let constraint_invocation = id(0x78);
    let mut constraint_audit = identity_audit_event(0x79, actor, unique_constraint_id);
    constraint_audit.correlation_id = constraint_invocation;
    storage.commit_authorized_constraint_ddl(
        constraint_invocation,
        actor,
        9_999,
        &[
            unique_constraint,
            object_write(unique_constraint_id, 6),
            CatalogWrite {
                key: unique_index_key.clone(),
                expected_value: None,
                payload: Some(unique_index_payload),
            },
            object_write(unique_index_id, 7),
            dependency_write(unique_constraint_id, unique_index_id, 4),
            primary_constraint,
            object_write(primary_constraint_id, 6),
            CatalogWrite {
                key: primary_index_key.clone(),
                expected_value: None,
                payload: Some(primary_index_payload),
            },
            object_write(primary_index_id, 7),
            dependency_write(primary_constraint_id, primary_index_id, 4),
            table_epoch,
        ],
        &[constraint_audit],
    )?;
    assert!(matches!(
        stale_constraint_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(
        row(
            &database,
            &table_row_key(table.table_id, stale_constraint_row)
        )?
        .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, stale_constraint_invocation, actor, 1)?
            .ok_or("missing stale PRIMARY KEY/UNIQUE writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut insert = storage.begin_invocation(id(0x7a), actor, 9_999, 1, 1024)?;
    let table_schema = insert.load_catalog_table_schema(table.table_id)?;
    insert.load_catalog_indexes(&table_schema)?;
    let tuple = encode_tuple(
        &table_schema.columns,
        table_schema.schema_version,
        &[Some(ScalarValue::Integer(42))],
    )?;
    let row_id = id(0x7b);
    insert.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: row_id,
        },
        &tuple,
    )?;
    insert.commit()?;
    let row_key = table_row_key(table.table_id, row_id);
    let (forward_count_before, reverse_count_before) = {
        let read = database.begin_read()?;
        let forward = read.open_table(schema::INDEXES)?;
        let reverse = read.open_table(schema::ROW_INDEXES)?;
        (forward.iter()?.count(), reverse.iter()?.count())
    };
    assert_eq!(forward_count_before, 2);
    assert_eq!(reverse_count_before, 2);

    let drop_invocation = id(0x7c);
    let mut drop_audit = identity_audit_event(0x7d, actor, table.table_id);
    drop_audit.correlation_id = drop_invocation;
    storage.drop_table(drop_invocation, actor, table.table_id, 9_999, drop_audit)?;
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing primary/unique table-drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, unique_constraint_id),
        id_catalog_key(2, unique_constraint_id),
        unique_index_key,
        id_catalog_key(2, unique_index_id),
        dependency_write(unique_constraint_id, unique_index_id, 4).key,
        id_catalog_key(8, primary_constraint_id),
        id_catalog_key(2, primary_constraint_id),
        primary_index_key,
        id_catalog_key(2, primary_index_id),
        dependency_write(primary_constraint_id, primary_index_id, 4).key,
        id_catalog_key(8, not_null_constraint_id),
        id_catalog_key(2, not_null_constraint_id),
        id_catalog_key(6, table.column_ids[0]),
        id_catalog_key(2, table.column_ids[0]),
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "owned primary/unique record remained: {key:?}"
        );
    }
    assert!(read.open_table(ROWS)?.get(&row_key[..])?.is_none());
    assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 0);
    assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 0);
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn unique_constraint_backfill_racing_duplicate_insert_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let actor = id(0xbc);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xbd, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xbd),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "unique_constraint_race_owner"),
            object_write_with_name(actor, 8, "unique_constraint_race_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0xbe);
    let mut schema_audit = identity_audit_event(0xbf, actor, id(0xc0));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "unique_race_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0xc1);
    let mut create_audit = identity_audit_event(0xc2, actor, id(0xc3));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "unique_race_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;

    let existing_invocation = id(0xc4);
    let existing_row_id = id(0xc5);
    let mut existing_writer =
        storage.begin_invocation(existing_invocation, actor, 9_999, 2, 1024)?;
    let table_schema = existing_writer.load_catalog_table_schema(table.table_id)?;
    existing_writer.load_catalog_indexes(&table_schema)?;
    let duplicate_tuple = encode_tuple(
        &table_schema.columns,
        table_schema.schema_version,
        &[Some(ScalarValue::Integer(37))],
    )?;
    existing_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: existing_row_id,
        },
        &duplicate_tuple,
    )?;
    existing_writer.commit()?;

    let writer_invocation = id(0xc6);
    let duplicate_row_id = id(0xc7);
    let mut pending_writer = storage.begin_invocation(writer_invocation, actor, 9_999, 2, 1024)?;
    let pending_schema = pending_writer.load_catalog_table_schema(table.table_id)?;
    pending_writer.load_catalog_indexes(&pending_schema)?;
    let pending_tuple = encode_tuple(
        &pending_schema.columns,
        pending_schema.schema_version,
        &[Some(ScalarValue::Integer(37))],
    )?;
    pending_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: duplicate_row_id,
        },
        &pending_tuple,
    )?;

    let constraint_id = id(0xc8);
    let index_id = id(0xc9);
    let constraint = constraint_write(
        constraint_id,
        table.table_id,
        3,
        &table.column_ids,
        None,
        &[],
        Some(index_id),
    );
    let (index_key, index_payload) =
        owned_unique_index_definition(index_id, table.table_id, &table.column_ids, constraint_id);
    let table_key = id_catalog_key(5, table.table_id);
    let table_bytes = {
        let read = database.begin_read()?;
        read.open_table(CATALOG)?
            .get(table_key.as_slice())?
            .ok_or("table record missing before unique constraint race")?
            .value()
            .to_vec()
    };
    let current_record = decode_registered_envelope(&table_bytes)?;
    let current_epoch = u64::from_le_bytes(
        current_record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("table write epoch missing before unique constraint race")?
            .payload
            .as_slice()
            .try_into()?,
    );
    let mut table_epoch = table_write_with_versions(
        table.table_id,
        schema.schema_id,
        table_schema.schema_version,
        current_epoch + 1,
    );
    table_epoch.expected_value = Some(table_bytes);
    let constraint_invocation = id(0xca);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, constraint_result) = std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            pending_writer.commit()
        });
        let constraint_barrier = barrier.clone();
        let constraint_storage = &storage;
        let constraint = scope.spawn(move || {
            let mut audit = identity_audit_event(0xcb, actor, constraint_id);
            audit.correlation_id = constraint_invocation;
            constraint_barrier.wait();
            constraint_storage.commit_authorized_constraint_ddl(
                constraint_invocation,
                actor,
                9_999,
                &[
                    constraint,
                    object_write(constraint_id, 6),
                    CatalogWrite {
                        key: index_key,
                        expected_value: None,
                        payload: Some(index_payload),
                    },
                    object_write(index_id, 7),
                    dependency_write(constraint_id, index_id, 4),
                    table_epoch,
                ],
                &[audit],
            )
        });
        barrier.wait();
        (writer.join().unwrap(), constraint.join().unwrap())
    });

    let (writer_status, constraint_status) = match (writer_result, constraint_result) {
        (Ok(_), Err(CommitError::UniqueViolation)) => {
            (OutcomeStatus::Committed, OutcomeStatus::Aborted)
        }
        (Err(TransactionError::Commit(CommitError::Conflict)), Ok(_)) => {
            (OutcomeStatus::Aborted, OutcomeStatus::Committed)
        }
        (writer, constraint) => {
            panic!(
                "unexpected duplicate-insert/unique-constraint race: {writer:?} / {constraint:?}"
            )
        }
    };
    assert_eq!(commit_sequence(&database)?, before_race + 1);
    assert_eq!(
        lookup_outcome(&database, writer_invocation, actor, 1)?
            .ok_or("missing duplicate-writer race outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, constraint_invocation, actor, 1)?
            .ok_or("missing unique-constraint race outcome")?
            .status,
        constraint_status
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let rows = read.open_table(ROWS)?;
    let indexes = read.open_table(schema::INDEXES)?;
    let reverse_indexes = read.open_table(schema::ROW_INDEXES)?;
    if constraint_status == OutcomeStatus::Committed {
        assert!(
            catalog
                .get(id_catalog_key(8, constraint_id).as_slice())?
                .is_some()
        );
        assert!(
            rows.get(table_row_key(table.table_id, duplicate_row_id).as_slice())?
                .is_none()
        );
        assert_eq!(indexes.iter()?.count(), 1);
        assert_eq!(reverse_indexes.iter()?.count(), 1);
    } else {
        assert!(
            catalog
                .get(id_catalog_key(8, constraint_id).as_slice())?
                .is_none()
        );
        assert!(
            rows.get(table_row_key(table.table_id, duplicate_row_id).as_slice())?
                .is_some()
        );
        assert_eq!(indexes.iter()?.count(), 0);
        assert_eq!(reverse_indexes.iter()?.count(), 0);
    }
    drop(reverse_indexes);
    drop(indexes);
    drop(rows);
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_drop_table_racing_invocation_leaves_no_orphan_row()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x31);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x32, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x32),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_race_owner"),
            object_write_with_name(actor, 8, "drop_race_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x33);
    let mut schema_audit = identity_audit_event(0x34, actor, id(0x35));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "drop_race_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x36);
    let mut create_audit = identity_audit_event(0x37, actor, id(0x38));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "racing_drop".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;

    let writer_invocation = id(0x39);
    let staged_row_id = id(0x3a);
    let mut writer = storage.begin_invocation(writer_invocation, actor, 9_999, 1, 1024)?;
    let table_schema = writer.load_catalog_table_schema(table.table_id)?;
    writer.load_catalog_indexes(&table_schema)?;
    let payload = encode_tuple(
        &table_schema.columns,
        table_schema.schema_version,
        &[Some(ScalarValue::Text("racing commit".to_owned()))],
    )?;
    writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: staged_row_id,
        },
        &payload,
    )?;

    let barrier = Arc::new(Barrier::new(3));
    let drop_invocation = id(0x3b);
    let (commit_result, drop_result) = std::thread::scope(|scope| {
        let commit_barrier = barrier.clone();
        let commit = scope.spawn(move || {
            commit_barrier.wait();
            writer.commit()
        });
        let drop_barrier = barrier.clone();
        let drop_storage = &storage;
        let drop = scope.spawn(move || {
            let mut audit = identity_audit_event(0x3c, actor, table.table_id);
            audit.correlation_id = drop_invocation;
            drop_barrier.wait();
            drop_storage.drop_table(drop_invocation, actor, table.table_id, 9_999, audit)
        });
        barrier.wait();
        (commit.join().unwrap(), drop.join().unwrap())
    });

    let writer_status = match commit_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(TransactionError::Commit(CommitError::Conflict)) => OutcomeStatus::Aborted,
        other => panic!("unexpected table-drop race writer result: {other:?}"),
    };
    assert!(
        drop_result.is_ok(),
        "table drop lost race unexpectedly: {drop_result:?}"
    );
    assert_eq!(
        lookup_outcome(&database, writer_invocation, actor, 1)?
            .ok_or("missing racing writer outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing racing table-drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(row(&database, &table_row_key(table.table_id, staged_row_id))?.is_none());
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .is_none()
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_drop_table_byte_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefinition, TableLifecycleError,
    };
    use prdb_engine::tuple::MAX_TUPLE_BYTES;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x93);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x94, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x94),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_byte_limit_owner"),
            object_write_with_name(actor, 8, "drop_byte_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x95);
    let mut schema_audit = identity_audit_event(0x96, actor, id(0x97));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "drop_byte_limit_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x98);
    let mut create_audit = identity_audit_event(0x99, actor, id(0x9a));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "bounded_drop_bytes".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Bytea,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    }];
    let empty_tuple = encode_tuple(&columns, 1, &[Some(ScalarValue::Bytea(Vec::new()))])?;
    let full_tuple = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(vec![
            b'x';
            MAX_TUPLE_BYTES
                - empty_tuple.len()
        ]))],
    )?;
    let one_byte_over_tuple = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(vec![
            b'y';
            1_281 - empty_tuple.len()
        ]))],
    )?;
    let exact_boundary_tuple = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(vec![
            b'z';
            1_280 - empty_tuple.len()
        ]))],
    )?;
    assert_eq!(full_tuple.len(), MAX_TUPLE_BYTES);
    assert_eq!(one_byte_over_tuple.len(), 1_281);
    assert_eq!(exact_boundary_tuple.len(), 1_280);
    assert_eq!(
        64 * full_tuple.len() + one_byte_over_tuple.len(),
        64 * 1024 * 1024 + 1
    );
    assert_eq!(
        64 * full_tuple.len() + exact_boundary_tuple.len(),
        64 * 1024 * 1024
    );
    let mut rows = (1u64..=64)
        .map(|row_number| {
            let mut row_id = [0; 16];
            row_id[8..].copy_from_slice(&row_number.to_be_bytes());
            RowWrite {
                key: table_row_key(table.table_id, row_id),
                expected_version: None,
                payload: Some(full_tuple.clone()),
            }
        })
        .collect::<Vec<_>>();
    let mut final_row_id = [0; 16];
    final_row_id[8..].copy_from_slice(&65u64.to_be_bytes());
    let final_row_key = table_row_key(table.table_id, final_row_id);
    rows.push(RowWrite {
        key: final_row_key,
        expected_version: None,
        payload: Some(one_byte_over_tuple),
    });
    let first_row_key = rows[0].key;
    let rows_invocation = id(0x9b);
    let before_drop = commit_rows(&database, rows_invocation, actor, 9_999, &rows)?;
    let over_limit_invocation = id(0x9c);
    let mut over_limit_audit = identity_audit_event(0x9d, actor, table.table_id);
    over_limit_audit.correlation_id = over_limit_invocation;
    assert_eq!(
        storage.drop_table(
            over_limit_invocation,
            actor,
            table.table_id,
            9_999,
            over_limit_audit,
        ),
        Err(TableLifecycleError::Commit(
            CommitError::TableDropLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, before_drop);
    assert_eq!(
        lookup_outcome(&database, over_limit_invocation, actor, 1)?
            .ok_or("missing table-drop byte-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert!(row(&database, &final_row_key)?.is_some());
    let exact_row_update = RowWrite {
        key: final_row_key,
        expected_version: Some(before_drop),
        payload: Some(exact_boundary_tuple),
    };
    let update_result = commit_rows(&database, id(0x9e), actor, 9_999, &[exact_row_update]);
    assert_eq!(update_result, Ok(before_drop + 1));
    let boundary_invocation = id(0x9f);
    let mut boundary_audit = identity_audit_event(0xa0, actor, table.table_id);
    boundary_audit.correlation_id = boundary_invocation;
    let drop_result = storage.drop_table(
        boundary_invocation,
        actor,
        table.table_id,
        9_999,
        boundary_audit,
    );
    assert!(
        drop_result.is_ok(),
        "exact byte-boundary drop returned {drop_result:?}"
    );
    assert_eq!(drop_result.unwrap().table_id, table.table_id);
    assert_eq!(commit_sequence(&database)?, before_drop + 2);
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact table-drop byte-boundary outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(row(&database, &first_row_key)?.is_none());
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_drop_table_index_byte_limit_boundary_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;
    use prdb_engine::tuple::MAX_TUPLE_BYTES;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x92);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x93, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x93),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_index_byte_limit_owner"),
            object_write_with_name(actor, 8, "drop_index_byte_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;

    let columns = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    }];
    let row_count = 32u64;
    let budget = 64 * 1024 * 1024;
    let full_tuple = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(vec![b'x'; MAX_TUPLE_BYTES - 25]))],
    )?;
    let full_key = {
        let mut row_id = [0; 16];
        row_id[8..].copy_from_slice(&1u64.to_be_bytes());
        typed_bytea_index_key(id(0x44), &table_row_key(id(16), row_id), &full_tuple).key
    };
    let locator_bytes = [0; 32];
    let full_contribution = full_tuple.len() + full_key.len() + locator_bytes.len();
    let preceding_bytes = (row_count as usize - 1) * full_contribution;
    assert!(preceding_bytes < budget);

    let make_final_tuple = |target: usize, zero_count: usize| {
        let value_length = (target - 92 - zero_count) / 2;
        let mut value = vec![b'y'; value_length];
        value[..zero_count].fill(0);
        encode_tuple(&columns, 1, &[Some(ScalarValue::Bytea(value))]).unwrap()
    };
    let final_over_tuple = make_final_tuple(budget + 1 - preceding_bytes, 1);
    let final_exact_tuple = make_final_tuple(budget - preceding_bytes, 0);

    let make_write = |row_number: u64, payload: Vec<u8>, expected_version| {
        let mut row_id = [0; 16];
        row_id[8..].copy_from_slice(&row_number.to_be_bytes());
        let row_key = table_row_key(id(16), row_id);
        let index = typed_bytea_index_key(id(0x44), &row_key, &payload);
        (
            RowWrite {
                key: row_key,
                expected_version,
                payload: Some(payload),
            },
            IndexWrite {
                key: index.key,
                row_key,
                expected_version,
                present: true,
                unique: false,
                null_exempt: false,
            },
        )
    };
    let mut rows = Vec::with_capacity(row_count as usize);
    let mut indexes = Vec::with_capacity(row_count as usize);
    for row_number in 1..row_count {
        let (row, index) = make_write(row_number, full_tuple.clone(), None);
        rows.push(row);
        indexes.push(index);
    }
    let (final_row, final_index) = make_write(row_count, final_over_tuple, None);
    let final_row_key = final_row.key;
    rows.push(final_row);
    indexes.push(final_index);
    let accounted_bytes = |row_payload: &[u8], index_key: &[u8]| {
        preceding_bytes + row_payload.len() + index_key.len() + locator_bytes.len()
    };
    assert_eq!(
        accounted_bytes(
            rows.last().unwrap().payload.as_deref().unwrap(),
            indexes.last().unwrap().key.as_slice(),
        ),
        budget + 1
    );
    let seed_sequence =
        commit_rows_and_indexes(&database, id(0x94), actor, 9_999, &rows, &indexes)?;
    let over_limit_invocation = id(0x95);
    let mut over_limit_audit = identity_audit_event(0x96, actor, id(16));
    over_limit_audit.correlation_id = over_limit_invocation;
    assert_eq!(
        storage.drop_table(
            over_limit_invocation,
            actor,
            id(16),
            9_999,
            over_limit_audit
        ),
        Err(TableLifecycleError::Commit(
            CommitError::IndexDdlLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, seed_sequence);
    assert_eq!(
        lookup_outcome(&database, over_limit_invocation, actor, 1)?
            .ok_or("missing table-drop index-byte-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert!(row(&database, &final_row_key)?.is_some());

    let (updated_row, mut new_index) =
        make_write(row_count, final_exact_tuple, Some(seed_sequence));
    let old_key = indexes.last().unwrap().key.clone();
    let updated_index = IndexWrite {
        key: old_key,
        row_key: final_row_key,
        expected_version: Some(seed_sequence),
        present: false,
        unique: false,
        null_exempt: false,
    };
    new_index.expected_version = None;
    assert_ne!(updated_index.key, new_index.key);
    let exact_sequence = commit_rows_and_indexes(
        &database,
        id(0x97),
        actor,
        9_999,
        &[updated_row],
        &[updated_index, new_index],
    )?;
    let exact_invocation = id(0x98);
    let mut exact_audit = identity_audit_event(0x99, actor, id(16));
    exact_audit.correlation_id = exact_invocation;
    let dropped = storage.drop_table(exact_invocation, actor, id(16), 9_999, exact_audit);
    assert!(
        dropped.is_ok(),
        "exact index byte-boundary drop returned {dropped:?}"
    );
    assert_eq!(commit_sequence(&database)?, exact_sequence + 1);
    assert_eq!(
        lookup_outcome(&database, exact_invocation, actor, 1)?
            .ok_or("missing exact table-drop index-byte-boundary outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(row(&database, &final_row_key)?.is_none());
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}
