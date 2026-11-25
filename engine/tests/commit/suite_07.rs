use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::commit::{
    CatalogWrite, CommitError, RowWrite, commit_catalog_and_audit, commit_rows,
};
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::schema::CATALOG;
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::ReadableDatabase;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_authorizes_constraint_ddl_by_table_alter()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(82);
    let schema_id = id(80);
    let column_id = id(84);
    let constraint_id = id(0xa1);
    let table_owner = id(0xa2);
    let outsider = id(0xa3);
    let admin = builtin_owner_id();

    let mut setup = foreign_key_catalog_writes(false, true);
    let foreign_key_key = id_catalog_key(8, id(86));
    let foreign_key_object_key = id_catalog_key(2, id(86));
    let target_edge = dependency_write(id(86), id(85), 3);
    setup.retain(|write| {
        write.key != foreign_key_key
            && write.key != foreign_key_object_key
            && write.key != target_edge.key
    });
    setup.extend([
        principal_write_with_name(table_owner, 1, 1, "constraint_owner"),
        object_write_with_name(table_owner, 8, "constraint_owner"),
        principal_write_with_name(outsider, 1, 1, "constraint_outsider"),
        object_write_with_name(outsider, 8, "constraint_outsider"),
    ]);
    let mut setup_audit = identity_audit_event(0xa4, admin, table_id);
    setup_audit.target_ids = vec![table_id, table_owner, outsider];
    assert_eq!(
        commit_catalog_and_audit(&database, id(0xa4), admin, 9_999, &setup, &[setup_audit],)?,
        1
    );
    let alter_grant = grant_write(table_owner, table_id, 8, 1, table_id, admin);
    let old_principal = principal_write_with_name(table_owner, 1, 1, "constraint_owner");
    let mut bumped_principal = principal_write_with_name(table_owner, 1, 2, "constraint_owner");
    bumped_principal.expected_value = old_principal.payload;
    let mut grant_audit = identity_audit_event(0xa9, admin, table_id);
    grant_audit.target_ids = vec![table_id, table_owner];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(0xa9),
            admin,
            9_999,
            &[
                alter_grant,
                dependency_write(table_owner, table_id, 5),
                bumped_principal,
            ],
            &[grant_audit],
        )?,
        2
    );
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let mut column_not_null = column_write_not_null(column_id, table_id, "object_84");
    column_not_null.expected_value = column_write(column_id, table_id, "object_84").payload;
    let constraint = constraint_write(constraint_id, table_id, 1, &[column_id], None, &[], None);
    let table_epoch = |next_epoch| {
        let mut write = table_write_with_versions(table_id, schema_id, 1, next_epoch);
        write.expected_value =
            table_write_with_versions(table_id, schema_id, 1, next_epoch - 1).payload;
        write
    };
    let add_changes = [
        constraint.clone(),
        object_write(constraint_id, 6),
        column_not_null.clone(),
        table_epoch(2),
    ];
    let row_key = table_row_key(table_id, id(0xac));
    let null_row = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[None],
    )?;
    assert_eq!(
        commit_rows(
            &database,
            id(0xab),
            admin,
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(null_row),
            }],
        )?,
        3
    );
    let denied_audit = identity_audit_event(0xa5, outsider, constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xa5),
            outsider,
            9_999,
            &add_changes,
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 3);
    assert_eq!(
        lookup_outcome(&database, id(0xa5), outsider, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let invalid_audit = identity_audit_event(0xa6, table_owner, constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xa6),
            table_owner,
            9_999,
            &add_changes,
            &[invalid_audit],
        ),
        Err(CommitError::NotNullViolation)
    );
    assert_eq!(commit_sequence(&database)?, 3);
    assert_eq!(
        lookup_outcome(&database, id(0xa6), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let non_null_row = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"ready".to_vec()))],
    )?;
    assert_eq!(
        commit_rows(
            &database,
            id(0xad),
            admin,
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(3),
                payload: Some(non_null_row),
            }],
        )?,
        4
    );
    let mut owner_audit = identity_audit_event(0xae, table_owner, constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xae),
            table_owner,
            9_999,
            &add_changes,
            &[owner_audit.clone()],
        )?,
        5
    );
    StorageManager::new(&database, Duration::from_millis(100))?;

    let mut removed_constraint = constraint;
    removed_constraint.expected_value = removed_constraint.payload.clone();
    removed_constraint.payload = None;
    let mut removed_object = object_write(constraint_id, 6);
    removed_object.expected_value = removed_object.payload.clone();
    removed_object.payload = None;
    let mut column_nullable = column_write(column_id, table_id, "object_84");
    column_nullable.expected_value = column_not_null.payload.clone();
    let drop_changes = [
        removed_constraint,
        removed_object,
        column_nullable,
        identity_tombstone_write(constraint_id),
        table_epoch(3),
    ];
    owner_audit.event_id = id(0xaf);
    owner_audit.correlation_id = id(0xb0);
    owner_audit.target_ids = vec![constraint_id];
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xaf),
            table_owner,
            9_999,
            &drop_changes,
            &[owner_audit],
        )?,
        6
    );
    StorageManager::new(&database, Duration::from_millis(100))?;

    let check_constraint_id = id(0xb1);
    let check_expression_id = id(0xb2);
    let check_expression_key = id_catalog_key(7, check_expression_id);
    let check_expression_payload = encode_envelope(
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
                payload: prdb_engine::expression::encode_expression(
                    &prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(true))),
                )?,
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let check_constraint_key = id_catalog_key(8, check_constraint_id);
    let check_constraint_payload = encode_envelope(
        &family_schema(8).unwrap(),
        &check_constraint_key,
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
    let check_add = [
        CatalogWrite {
            key: check_constraint_key.clone(),
            expected_value: None,
            payload: Some(check_constraint_payload.clone()),
        },
        object_write(check_constraint_id, 6),
        CatalogWrite {
            key: check_expression_key.clone(),
            expected_value: None,
            payload: Some(check_expression_payload.clone()),
        },
        object_write(check_expression_id, 5),
        dependency_write(check_constraint_id, check_expression_id, 2),
        table_epoch(4),
    ];
    let mut contextual_expression_fields =
        decode_registered_envelope(&check_expression_payload)?.fields;
    contextual_expression_fields
        .iter_mut()
        .find(|field| field.tag == 5)
        .ok_or("CHECK expression field is missing")?
        .payload =
        prdb_engine::expression::encode_expression(&prdb_engine::authorization::Expr::Equal(
            Box::new(prdb_engine::authorization::Expr::Literal(Some(
                ScalarValue::Uuid([9; 16]),
            ))),
            Box::new(prdb_engine::authorization::Expr::CurrentPrincipalId),
        ))?;
    let contextual_expression_payload = encode_envelope(
        &family_schema(7).unwrap(),
        &check_expression_key,
        &contextual_expression_fields,
    )?;
    let mut contextual_check_add = check_add.clone();
    contextual_check_add
        .iter_mut()
        .find(|write| write.key == check_expression_key)
        .ok_or("CHECK expression write is missing")?
        .payload = Some(contextual_expression_payload);
    let contextual_check_audit = identity_audit_event(0xb6, table_owner, check_constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xb6),
            table_owner,
            9_999,
            &contextual_check_add,
            &[contextual_check_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 6);
    assert_eq!(
        lookup_outcome(&database, id(0xb6), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let mut false_expression_fields = decode_registered_envelope(&check_expression_payload)?.fields;
    false_expression_fields
        .iter_mut()
        .find(|field| field.tag == 5)
        .ok_or("CHECK expression field is missing")?
        .payload = prdb_engine::expression::encode_expression(
        &prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(false))),
    )?;
    let false_expression_payload = encode_envelope(
        &family_schema(7).unwrap(),
        &check_expression_key,
        &false_expression_fields,
    )?;
    let mut failing_check_add = check_add.clone();
    failing_check_add
        .iter_mut()
        .find(|write| write.key == check_expression_key)
        .ok_or("CHECK expression write is missing")?
        .payload = Some(false_expression_payload);
    let failing_check_audit = identity_audit_event(0xb5, table_owner, check_constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xb5),
            table_owner,
            9_999,
            &failing_check_add,
            &[failing_check_audit],
        ),
        Err(CommitError::CheckViolation)
    );
    assert_eq!(commit_sequence(&database)?, 6);
    assert_eq!(
        lookup_outcome(&database, id(0xb5), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let stale_check_invocation = id(0xb7);
    let stale_check_row = id(0xb8);
    let mut stale_check_writer =
        storage.begin_invocation(stale_check_invocation, admin, 9_999, 1, 1024)?;
    let stale_check_schema = stale_check_writer.load_catalog_table_schema(table_id)?;
    stale_check_writer.load_catalog_indexes(&stale_check_schema)?;
    let stale_check_tuple = encode_tuple(
        &stale_check_schema.columns,
        stale_check_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"pinned before CHECK".to_vec()))],
    )?;
    stale_check_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table_id,
            row: stale_check_row,
        },
        &stale_check_tuple,
    )?;
    let check_audit = identity_audit_event(0xb3, table_owner, check_constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xb3),
            table_owner,
            9_999,
            &check_add,
            &[check_audit],
        )?,
        7
    );
    assert!(matches!(
        stale_check_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(table_id, stale_check_row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_check_invocation, admin, 1)?
            .ok_or("missing stale CHECK constraint writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    StorageManager::new(&database, Duration::from_millis(100))?;

    let check_drop = [
        CatalogWrite {
            key: check_constraint_key,
            expected_value: Some(check_constraint_payload),
            payload: None,
        },
        CatalogWrite {
            key: id_catalog_key(2, check_constraint_id),
            expected_value: object_write(check_constraint_id, 6).payload,
            payload: None,
        },
        CatalogWrite {
            key: check_expression_key,
            expected_value: Some(check_expression_payload),
            payload: None,
        },
        CatalogWrite {
            key: id_catalog_key(2, check_expression_id),
            expected_value: object_write(check_expression_id, 5).payload,
            payload: None,
        },
        CatalogWrite {
            key: dependency_write(check_constraint_id, check_expression_id, 2).key,
            expected_value: dependency_write(check_constraint_id, check_expression_id, 2).payload,
            payload: None,
        },
        identity_tombstone_write(check_constraint_id),
        identity_tombstone_write(check_expression_id),
        table_epoch(5),
    ];
    let check_drop_audit = identity_audit_event(0xb4, table_owner, check_constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xb4),
            table_owner,
            9_999,
            &check_drop,
            &[check_drop_audit],
        )?,
        8
    );
    StorageManager::new(&database, Duration::from_millis(100))?;

    let unique_constraint_id = id(0xb7);
    let unique_index_id = id(0xb8);
    let unique_constraint = constraint_write(
        unique_constraint_id,
        table_id,
        3,
        &[column_id],
        None,
        &[],
        Some(unique_index_id),
    );
    let (unique_index_key, unique_index_payload) = owned_unique_index_definition(
        unique_index_id,
        table_id,
        &[column_id],
        unique_constraint_id,
    );
    let unique_add = [
        unique_constraint.clone(),
        object_write(unique_constraint_id, 6),
        CatalogWrite {
            key: unique_index_key.clone(),
            expected_value: None,
            payload: Some(unique_index_payload.clone()),
        },
        object_write(unique_index_id, 7),
        dependency_write(unique_constraint_id, unique_index_id, 4),
        table_epoch(6),
    ];
    let unique_audit = identity_audit_event(0xba, table_owner, unique_constraint_id);
    let unique_result = storage.commit_authorized_constraint_ddl(
        id(0xba),
        table_owner,
        9_999,
        &unique_add,
        &[unique_audit],
    );
    assert_eq!(unique_result, Ok(9));
    assert_eq!(commit_sequence(&database)?, 9);
    StorageManager::new(&database, Duration::from_millis(100))?;

    let mut unique_constraint_drop = unique_constraint;
    unique_constraint_drop.expected_value = unique_constraint_drop.payload.clone();
    unique_constraint_drop.payload = None;
    let mut unique_constraint_object_drop = object_write(unique_constraint_id, 6);
    unique_constraint_object_drop.expected_value = unique_constraint_object_drop.payload.clone();
    unique_constraint_object_drop.payload = None;
    let unique_index_drop = CatalogWrite {
        key: unique_index_key,
        expected_value: Some(unique_index_payload),
        payload: None,
    };
    let mut unique_index_object_drop = object_write(unique_index_id, 7);
    unique_index_object_drop.expected_value = unique_index_object_drop.payload.clone();
    unique_index_object_drop.payload = None;
    let unique_dependency = dependency_write(unique_constraint_id, unique_index_id, 4);
    let unique_drop = [
        unique_constraint_drop,
        unique_constraint_object_drop,
        unique_index_drop,
        unique_index_object_drop,
        CatalogWrite {
            key: unique_dependency.key,
            expected_value: unique_dependency.payload,
            payload: None,
        },
        identity_tombstone_write(unique_constraint_id),
        identity_tombstone_write(unique_index_id),
        table_epoch(7),
    ];
    let denied_drop_audit = identity_audit_event(0xbc, outsider, unique_constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xbc),
            outsider,
            9_999,
            &unique_drop,
            &[denied_drop_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 9);
    assert_eq!(
        lookup_outcome(&database, id(0xbc), outsider, 1)?
            .ok_or(CommitError::Conflict)?
            .status,
        OutcomeStatus::Aborted
    );
    let unique_drop_audit = identity_audit_event(0xbb, table_owner, unique_constraint_id);
    let unique_drop_result = storage.commit_authorized_constraint_ddl(
        id(0xbb),
        table_owner,
        9_999,
        &unique_drop,
        &[unique_drop_audit],
    );
    assert_eq!(unique_drop_result, Ok(10));
    StorageManager::new(&database, Duration::from_millis(100))?;

    let not_null_constraint_id = id(0xbd);
    let not_null = constraint_write(
        not_null_constraint_id,
        table_id,
        1,
        &[column_id],
        None,
        &[],
        None,
    );
    let not_null_add = [
        not_null,
        object_write(not_null_constraint_id, 6),
        column_not_null.clone(),
        table_epoch(8),
    ];
    let not_null_audit = identity_audit_event(0xbe, table_owner, not_null_constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xbe),
            table_owner,
            9_999,
            &not_null_add,
            &[not_null_audit],
        )?,
        11
    );
    StorageManager::new(&database, Duration::from_millis(100))?;

    let primary_constraint_id = id(0xbf);
    let primary_index_id = id(0xc0);
    let primary_constraint = constraint_write(
        primary_constraint_id,
        table_id,
        2,
        &[column_id],
        None,
        &[],
        Some(primary_index_id),
    );
    let (primary_index_key, primary_index_payload) = owned_unique_index_definition(
        primary_index_id,
        table_id,
        &[column_id],
        primary_constraint_id,
    );
    let primary_add = [
        primary_constraint.clone(),
        object_write(primary_constraint_id, 6),
        CatalogWrite {
            key: primary_index_key.clone(),
            expected_value: None,
            payload: Some(primary_index_payload.clone()),
        },
        object_write(primary_index_id, 7),
        dependency_write(primary_constraint_id, primary_index_id, 4),
        table_epoch(9),
    ];
    let primary_audit = identity_audit_event(0xc1, table_owner, primary_constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xc1),
            table_owner,
            9_999,
            &primary_add,
            &[primary_audit],
        )?,
        12
    );
    StorageManager::new(&database, Duration::from_millis(100))?;

    let mut primary_constraint_drop = primary_constraint;
    primary_constraint_drop.expected_value = primary_constraint_drop.payload.clone();
    primary_constraint_drop.payload = None;
    let mut primary_constraint_object_drop = object_write(primary_constraint_id, 6);
    primary_constraint_object_drop.expected_value = primary_constraint_object_drop.payload.clone();
    primary_constraint_object_drop.payload = None;
    let primary_index_drop = CatalogWrite {
        key: primary_index_key,
        expected_value: Some(primary_index_payload),
        payload: None,
    };
    let mut primary_index_object_drop = object_write(primary_index_id, 7);
    primary_index_object_drop.expected_value = primary_index_object_drop.payload.clone();
    primary_index_object_drop.payload = None;
    let primary_dependency = dependency_write(primary_constraint_id, primary_index_id, 4);
    let primary_drop = [
        primary_constraint_drop,
        primary_constraint_object_drop,
        primary_index_drop,
        primary_index_object_drop,
        CatalogWrite {
            key: primary_dependency.key,
            expected_value: primary_dependency.payload,
            payload: None,
        },
        identity_tombstone_write(primary_constraint_id),
        identity_tombstone_write(primary_index_id),
        table_epoch(10),
    ];
    let primary_drop_audit = identity_audit_event(0xc2, table_owner, primary_constraint_id);
    assert_eq!(
        storage.commit_authorized_constraint_ddl(
            id(0xc2),
            table_owner,
            9_999,
            &primary_drop,
            &[primary_drop_audit],
        )?,
        13
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn catalog_commit_checks_dependency_endpoints_in_the_final_catalog()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_object = object_write(id(101), 3);
    let table = table_write(id(101), builtin_id(1));
    let column_object = object_write_with_parent(id(104), 4, Some(id(101)));
    let column = column_write(id(104), id(101), "object_104");
    let edge = dependency_write(id(104), id(101), 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(51),
            id(9),
            9_999,
            &[
                edge.clone(),
                table_object.clone(),
                table,
                column_object,
                column,
            ],
            &[audit_event(51)],
        )?,
        1
    );

    let delete_parent = CatalogWrite {
        key: table_object.key.clone(),
        expected_value: table_object.payload.clone(),
        payload: None,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(52),
            id(9),
            9_999,
            &[delete_parent],
            &[audit_event(52)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(52), id(9), 9_998)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(read.open_table(CATALOG)?.get(&edge.key[..])?.is_some());
    assert!(
        read.open_table(CATALOG)?
            .get(&table_object.key[..])?
            .is_some()
    );
    Ok(())
}

pub(super) fn catalog_commit_rejects_table_parent_schema_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let actual_schema_id = builtin_id(1);
    let wrong_schema_id = builtin_id(2);
    let table_id = id(0xc4);
    let column_id = id(0xc5);
    let invocation_id = id(0xc6);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            invocation_id,
            id(9),
            9_999,
            &[
                object_write_with_parent(table_id, 3, Some(wrong_schema_id)),
                table_write(table_id, actual_schema_id),
                dependency_write(table_id, wrong_schema_id, 1),
                object_write(column_id, 4),
                column_write(column_id, table_id, "object_197"),
            ],
            &[audit_event(0xc6)],
        ),
        Err(CommitError::InvalidCatalog),
        "table object's parent schema must match its typed table schema"
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        lookup_outcome(&database, invocation_id, id(9), 1)?
            .ok_or("missing malformed table-parent outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(5, table_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(6, column_id).as_slice())?
            .is_none()
    );
    Ok(())
}

pub(super) fn catalog_commit_rejects_column_parent_mismatch_with_typed_table()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(0xc1);
    let declared_parent_id = id(0xc2);
    let column_id = id(0xc3);
    let invalid_writes = [
        object_write(table_id, 3),
        table_write(table_id, builtin_id(1)),
        object_write(declared_parent_id, 3),
        table_write(declared_parent_id, builtin_id(1)),
        object_write_with_parent(column_id, 4, Some(declared_parent_id)),
        column_write(column_id, table_id, "mismatched_parent"),
        dependency_write(column_id, declared_parent_id, 1),
    ];

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(53),
            id(9),
            9_999,
            &invalid_writes,
            &[audit_event(53)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        lookup_outcome(&database, id(53), id(9), 1)?
            .ok_or("missing rejected-parent outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for write in invalid_writes {
        assert!(catalog.get(write.key.as_slice())?.is_none());
    }
    Ok(())
}

pub(super) fn catalog_commit_requires_owned_child_parent_edge_consistency()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let parent_id = id(30);
    let child_id = id(31);
    let parent = object_write(parent_id, 1);
    let parent_schema = schema_write(parent_id);
    let child = object_write_with_parent(child_id, 3, Some(parent_id));
    let child_table = table_write(child_id, parent_id);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(60),
            id(9),
            9_999,
            &[
                parent.clone(),
                parent_schema.clone(),
                child.clone(),
                child_table.clone(),
            ],
            &[audit_event(60)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let edge = dependency_write(child_id, parent_id, 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(61),
            id(9),
            9_999,
            &[
                parent.clone(),
                parent_schema.clone(),
                child.clone(),
                child_table.clone(),
                edge.clone(),
            ],
            &[audit_event(61)],
        ),
        Ok(1),
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(62),
            id(9),
            9_999,
            &[CatalogWrite {
                key: edge.key.clone(),
                expected_value: edge.payload.clone(),
                payload: None,
            }],
            &[audit_event(62)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let cyclic_parent = object_write_with_parent(parent_id, 1, Some(child_id));
    let cyclic_edge = dependency_write(parent_id, child_id, 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(63),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: cyclic_parent.key,
                    expected_value: parent.payload,
                    payload: cyclic_parent.payload,
                },
                cyclic_edge,
            ],
            &[audit_event(63)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    let next_parent_id = id(34);
    let next_parent = object_write(next_parent_id, 1);
    let next_parent_schema = schema_write(next_parent_id);
    let reparented_child = object_write_with_parent(child_id, 3, Some(next_parent_id));
    let mut reparented_table = table_write_with_versions(child_id, next_parent_id, 2, 2);
    reparented_table.expected_value = child_table.payload.clone();
    let next_edge = dependency_write(child_id, next_parent_id, 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(65),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: child.key,
                    expected_value: child.payload,
                    payload: reparented_child.payload,
                },
                reparented_table,
                CatalogWrite {
                    key: edge.key,
                    expected_value: edge.payload,
                    payload: None,
                },
                next_parent,
                next_parent_schema,
                next_edge,
            ],
            &[audit_event(65)],
        ),
        Ok(2)
    );
    assert_eq!(commit_sequence(&database)?, 2);
    Ok(())
}

pub(super) fn storage_manager_drop_table_restricts_external_foreign_key_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xb4);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "external_fk_drop_admin"),
        object_write_with_name(actor, 8, "external_fk_drop_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xb0, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb1), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let before_drop = commit_sequence(&database)?;
    let drop_invocation = id(0xb2);
    let mut drop_audit = identity_audit_event(0xb3, actor, id(81));
    drop_audit.correlation_id = drop_invocation;
    assert_eq!(
        storage.drop_table(drop_invocation, actor, id(81), 9_999, drop_audit),
        Err(TableLifecycleError::Commit(
            CommitError::TableHasDependencies
        ))
    );

    assert_eq!(commit_sequence(&database)?, before_drop);
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing rejected table-drop outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(5, id(81)),
        id_catalog_key(5, id(82)),
        id_catalog_key(8, id(85)),
        id_catalog_key(8, id(86)),
        dependency_write(id(86), id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_some(),
            "record removed: {key:?}"
        );
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn foreign_key_creation_racing_referenced_table_drop_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xb5);
    let foreign_key_key = id_catalog_key(8, id(86));
    let foreign_key_object_key = id_catalog_key(2, id(86));
    let target_edge = dependency_write(id(86), id(85), 3);
    let target_edge_key = target_edge.key.clone();
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != foreign_key_key
                && write.key != foreign_key_object_key
                && write.key != target_edge.key
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "foreign_key_race_admin"),
        object_write_with_name(actor, 8, "foreign_key_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xb6, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb7), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut foreign_key_definition = foreign_key_catalog_writes(true, true)
        .into_iter()
        .find(|write| write.key == foreign_key_key)
        .ok_or("missing foreign-key definition")?;
    foreign_key_definition.expected_value = None;
    let foreign_key_object = object_write(id(86), 6);
    let mut source_table_update = table_write_with_versions(id(82), id(80), 1, 2);
    source_table_update.expected_value = table_write(id(82), id(80)).payload;
    let mut target_table_update = table_write_with_versions(id(81), id(80), 1, 2);
    target_table_update.expected_value = table_write(id(81), id(80)).payload;
    let fk_invocation = id(0xb8);
    let drop_invocation = id(0xb9);
    let barrier = Arc::new(Barrier::new(3));
    let (fk_result, drop_result) = std::thread::scope(|scope| {
        let fk_barrier = barrier.clone();
        let fk_storage = &storage;
        let fk = scope.spawn(move || {
            let mut audit = identity_audit_event(0xba, actor, id(86));
            audit.correlation_id = fk_invocation;
            fk_barrier.wait();
            fk_storage.commit_authorized_foreign_key_ddl(
                fk_invocation,
                actor,
                9_999,
                &[
                    foreign_key_definition,
                    foreign_key_object,
                    target_edge,
                    source_table_update,
                    target_table_update,
                ],
                &[audit],
            )
        });
        let drop_barrier = barrier.clone();
        let drop_storage = &storage;
        let drop = scope.spawn(move || {
            let mut audit = identity_audit_event(0xbb, actor, id(81));
            audit.correlation_id = drop_invocation;
            drop_barrier.wait();
            drop_storage.drop_table(drop_invocation, actor, id(81), 9_999, audit)
        });
        barrier.wait();
        (fk.join().unwrap(), drop.join().unwrap())
    });

    let (fk_status, drop_status) = match (fk_result, drop_result) {
        (Ok(_), Err(TableLifecycleError::Commit(CommitError::TableHasDependencies))) => {
            (OutcomeStatus::Committed, OutcomeStatus::Aborted)
        }
        (
            Err(
                CommitError::Conflict
                | CommitError::InvalidCatalog
                | CommitError::DdlAuthorizationDenied,
            ),
            Ok(_),
        ) => (OutcomeStatus::Aborted, OutcomeStatus::Committed),
        (fk, drop) => panic!("unexpected FK-create/table-drop race: {fk:?} / {drop:?}"),
    };
    assert_eq!(commit_sequence(&database)?, 2);
    assert_eq!(
        lookup_outcome(&database, fk_invocation, actor, 1)?
            .ok_or("missing FK race outcome")?
            .status,
        fk_status
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing table-drop race outcome")?
            .status,
        drop_status
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    if drop_status == OutcomeStatus::Committed {
        assert!(catalog.get(id_catalog_key(5, id(81)).as_slice())?.is_none());
        assert!(catalog.get(foreign_key_key.as_slice())?.is_none());
        assert!(catalog.get(target_edge_key.as_slice())?.is_none());
        assert!(catalog.get(id_catalog_key(5, id(82)).as_slice())?.is_some());
    } else {
        assert!(catalog.get(id_catalog_key(5, id(81)).as_slice())?.is_some());
        assert!(catalog.get(foreign_key_key.as_slice())?.is_some());
        assert!(catalog.get(target_edge_key.as_slice())?.is_some());
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn foreign_key_creation_racing_referenced_primary_key_drop_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xb0);
    let foreign_key_key = id_catalog_key(8, id(86));
    let foreign_key_object_key = id_catalog_key(2, id(86));
    let target_edge = dependency_write(id(86), id(85), 3);
    let target_edge_key = target_edge.key.clone();
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| write.key != foreign_key_key && write.key != foreign_key_object_key)
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_pk_drop_race_admin"),
        object_write_with_name(actor, 8, "fk_pk_drop_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0xb1, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb1), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut foreign_key_definition = foreign_key_catalog_writes(true, true)
        .into_iter()
        .find(|write| write.key == foreign_key_key)
        .ok_or("missing foreign-key definition")?;
    foreign_key_definition.expected_value = None;
    let foreign_key_object = object_write(id(86), 6);
    let mut source_table_update = table_write_with_versions(id(82), id(80), 1, 2);
    source_table_update.expected_value = table_write(id(82), id(80)).payload;
    let mut target_table_update = table_write_with_versions(id(81), id(80), 1, 2);
    target_table_update.expected_value = table_write(id(81), id(80)).payload;
    let fk_invocation = id(0xb2);
    let drop_invocation = id(0xb3);
    let barrier = Arc::new(Barrier::new(3));
    let (fk_result, drop_result) = std::thread::scope(|scope| {
        let fk_barrier = barrier.clone();
        let fk_storage = &storage;
        let fk = scope.spawn(move || {
            let mut audit = identity_audit_event(0xb4, actor, id(86));
            audit.correlation_id = fk_invocation;
            fk_barrier.wait();
            fk_storage.commit_authorized_foreign_key_ddl(
                fk_invocation,
                actor,
                9_999,
                &[
                    foreign_key_definition,
                    foreign_key_object,
                    target_edge,
                    source_table_update,
                    target_table_update,
                ],
                &[audit],
            )
        });
        let drop_barrier = barrier.clone();
        let drop_storage = &storage;
        let drop = scope.spawn(move || {
            let mut audit = identity_audit_event(0xb5, actor, id(85));
            audit.correlation_id = drop_invocation;
            drop_barrier.wait();
            drop_storage.drop_primary_key(drop_invocation, actor, id(85), 9_999, audit)
        });
        barrier.wait();
        (fk.join().unwrap(), drop.join().unwrap())
    });

    let (fk_status, drop_status) = match (fk_result, drop_result) {
        (
            Ok(_),
            Err(TableLifecycleError::Commit(CommitError::InvalidCatalog | CommitError::Conflict)),
        ) => (OutcomeStatus::Committed, OutcomeStatus::Aborted),
        (Err(CommitError::InvalidCatalog | CommitError::Conflict), Ok(_)) => {
            (OutcomeStatus::Aborted, OutcomeStatus::Committed)
        }
        (fk, drop) => panic!("unexpected FK-create/PRIMARY KEY-drop race: {fk:?} / {drop:?}"),
    };
    assert_eq!(commit_sequence(&database)?, 2);
    assert_eq!(
        lookup_outcome(&database, fk_invocation, actor, 1)?
            .ok_or("missing FK/key-drop race outcome")?
            .status,
        fk_status
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing key-drop race outcome")?
            .status,
        drop_status
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    if fk_status == OutcomeStatus::Committed {
        for key in [
            id_catalog_key(8, id(85)),
            id_catalog_key(9, id(87)),
            foreign_key_key.clone(),
            target_edge_key,
        ] {
            assert!(catalog.get(key.as_slice())?.is_some(), "missing {key:?}");
        }
    } else {
        for key in [id_catalog_key(8, id(85)), id_catalog_key(9, id(87))] {
            assert!(catalog.get(key.as_slice())?.is_none(), "unexpected {key:?}");
        }
        assert!(catalog.get(foreign_key_key.as_slice())?.is_none());
        assert!(catalog.get(target_edge_key.as_slice())?.is_none());
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn foreign_key_removal_racing_referenced_table_drop_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xf1);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "foreign_key_remove_race_admin"),
        object_write_with_name(actor, 8, "foreign_key_remove_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xf2, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xf3), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let fk_invocation = id(0xf4);
    let table_invocation = id(0xf5);
    let barrier = Arc::new(Barrier::new(3));
    let (fk_result, table_result) = std::thread::scope(|scope| {
        let fk_barrier = barrier.clone();
        let fk_storage = &storage;
        let fk = scope.spawn(move || {
            let mut audit = identity_audit_event(0xf6, actor, id(86));
            audit.correlation_id = fk_invocation;
            fk_barrier.wait();
            fk_storage.drop_foreign_key(fk_invocation, actor, id(86), 9_999, audit)
        });
        let table_barrier = barrier.clone();
        let table_storage = &storage;
        let table = scope.spawn(move || {
            let mut audit = identity_audit_event(0xf7, actor, id(81));
            audit.correlation_id = table_invocation;
            table_barrier.wait();
            table_storage.drop_table(table_invocation, actor, id(81), 9_999, audit)
        });
        barrier.wait();
        (fk.join().unwrap(), table.join().unwrap())
    });

    assert!(fk_result.is_ok(), "FK drop failed: {fk_result:?}");
    let table_status = match table_result {
        Ok(dropped) => {
            assert_eq!(dropped.table_id, id(81));
            OutcomeStatus::Committed
        }
        Err(TableLifecycleError::Commit(CommitError::TableHasDependencies)) => {
            OutcomeStatus::Aborted
        }
        Err(TableLifecycleError::Commit(CommitError::Conflict)) => OutcomeStatus::Aborted,
        other => return Err(format!("unexpected target-table drop result: {other:?}").into()),
    };
    let fk_outcome = lookup_outcome(&database, fk_invocation, actor, 1)?
        .ok_or("missing FK-removal race outcome")?;
    assert_eq!(fk_outcome.status, OutcomeStatus::Committed);
    let table_outcome = lookup_outcome(&database, table_invocation, actor, 1)?
        .ok_or("missing table-drop race outcome")?;
    assert_eq!(table_outcome.status, table_status);
    assert_eq!(
        commit_sequence(&database)?,
        1 + u64::from(table_status == OutcomeStatus::Committed) + 1
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(id_catalog_key(8, id(86)).as_slice())?.is_none());
    assert_eq!(
        catalog.get(id_catalog_key(5, id(81)).as_slice())?.is_some(),
        table_status != OutcomeStatus::Committed
    );
    assert!(catalog.get(id_catalog_key(5, id(82)).as_slice())?.is_some());
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_drops_foreign_key_and_advances_both_table_epochs()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xcc);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "foreign_key_drop_admin"),
        object_write_with_name(actor, 8, "foreign_key_drop_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xcd, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xce), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut stale_source_writer = storage.begin_invocation(id(0xd1), actor, 9_999, 1, 1024)?;
    let stale_source_schema = stale_source_writer.load_catalog_table_schema(id(82))?;
    stale_source_writer.load_catalog_indexes(&stale_source_schema)?;
    let stale_source_tuple = encode_tuple(
        &stale_source_schema.columns,
        stale_source_schema.schema_version,
        &[None],
    )?;
    stale_source_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xd2),
        },
        &stale_source_tuple,
    )?;
    let mut stale_target_writer = storage.begin_invocation(id(0xd3), actor, 9_999, 1, 1024)?;
    let stale_target_schema = stale_target_writer.load_catalog_table_schema(id(81))?;
    stale_target_writer.load_catalog_indexes(&stale_target_schema)?;
    let stale_target_tuple = encode_tuple(
        &stale_target_schema.columns,
        stale_target_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"staged before FK drop".to_vec()))],
    )?;
    stale_target_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(81),
            row: id(0xd4),
        },
        &stale_target_tuple,
    )?;

    let invocation_id = id(0xcf);
    let mut audit = identity_audit_event(0xd0, actor, id(86));
    audit.correlation_id = invocation_id;
    let pinned_reader = database.begin_read()?;
    assert_eq!(
        storage.drop_foreign_key(invocation_id, actor, id(86), 9_999, audit)?,
        2
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing foreign-key drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(matches!(
        stale_source_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(matches!(
        stale_target_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert_eq!(
        prdb_engine::catalog_index::scan_constraint_ids_for_table(&pinned_reader, id(82), 5)?,
        vec![id(86)]
    );
    assert!(
        prdb_engine::catalog_index::scan_constraint_ids_for_table(&read, id(82), 5)?.is_empty()
    );
    let pinned_catalog = pinned_reader.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, id(86)),
        id_catalog_key(2, id(86)),
        dependency_write(id(86), id(85), 3).key,
    ] {
        assert!(pinned_catalog.get(key.as_slice())?.is_some());
    }
    drop(pinned_catalog);
    for key in [
        id_catalog_key(8, id(86)),
        id_catalog_key(2, id(86)),
        dependency_write(id(86), id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "record remains: {key:?}"
        );
    }
    let retired_foreign_key = catalog
        .get(id_catalog_key(17, id(86)).as_slice())?
        .ok_or("missing retired foreign-key identifier")?;
    assert!(
        decode_registered_envelope(retired_foreign_key.value())?
            .fields
            .is_empty()
    );
    drop(retired_foreign_key);
    for table_id in [id(81), id(82)] {
        let key = id_catalog_key(5, table_id);
        let bytes = catalog
            .get(key.as_slice())?
            .ok_or("participating table disappeared during FK drop")?
            .value()
            .to_vec();
        assert_eq!(
            u64::from_le_bytes(
                decode_registered_envelope(&bytes)?
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("missing table write epoch")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            2
        );
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn catalog_rejects_foreign_key_deletion_without_identity_tombstone()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xda);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_tombstone_guard_admin"),
        object_write_with_name(actor, 8, "fk_tombstone_guard_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xdb, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xdc), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let fk_key = id_catalog_key(8, id(86));
    let object_key = id_catalog_key(2, id(86));
    let edge_key = dependency_write(id(86), id(85), 3).key;
    let mut raw_drop_writes = Vec::new();
    for mut write in foreign_key_catalog_writes(true, true) {
        if write.key == fk_key || write.key == object_key || write.key == edge_key {
            write.expected_value = write.payload.take();
            write.payload = None;
            raw_drop_writes.push(write);
        }
    }
    for table_id in [id(81), id(82)] {
        raw_drop_writes.push(catalog_expect(
            table_write_with_versions(table_id, id(80), 1, 2),
            table_write(table_id, id(80))
                .payload
                .ok_or("missing fixture table definition")?,
        ));
    }
    let invocation_id = id(0xdd);
    let mut audit = identity_audit_event(0xde, actor, id(86));
    audit.correlation_id = invocation_id;
    assert_eq!(
        storage.commit_catalog_and_audit(invocation_id, actor, 9_999, &raw_drop_writes, &[audit],),
        Err(CommitError::InvalidCatalog)
    );
    assert!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing rejected raw FK drop outcome")?
            .status
            == OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(fk_key.as_slice())?.is_some());
    assert!(catalog.get(object_key.as_slice())?.is_some());
    assert!(
        catalog
            .get(id_catalog_key(17, id(86)).as_slice())?
            .is_none()
    );
    Ok(())
}

pub(super) fn storage_manager_drops_self_referential_foreign_key_once_per_table()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe1);
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        constraint_write(id(86), id(81), 5, &[id(83)], Some(id(85)), &[id(83)], None),
        object_write(id(86), 6),
        dependency_write(id(86), id(85), 3),
        principal_write_with_name(actor, 1, 1, "self_fk_remove_admin"),
        object_write_with_name(actor, 8, "self_fk_remove_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xe2, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xe2), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let invocation_id = id(0xe3);
    let mut audit = identity_audit_event(0xe4, actor, id(86));
    audit.correlation_id = invocation_id;
    assert_eq!(
        storage.drop_foreign_key(invocation_id, actor, id(86), 9_999, audit)?,
        2
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(id_catalog_key(8, id(86)).as_slice())?.is_none());
    let table = catalog
        .get(id_catalog_key(5, id(81)).as_slice())?
        .ok_or("self-referencing table disappeared during FK drop")?
        .value()
        .to_vec();
    let epoch = decode_registered_envelope(&table)?
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .ok_or("missing self-referencing table write epoch")?
        .payload
        .as_slice()
        .try_into()?;
    assert_eq!(u64::from_le_bytes(epoch), 2);
    let other_table = catalog
        .get(id_catalog_key(5, id(82)).as_slice())?
        .ok_or("unrelated table disappeared during FK drop")?
        .value()
        .to_vec();
    let other_epoch = decode_registered_envelope(&other_table)?
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .ok_or("missing unrelated table write epoch")?
        .payload
        .as_slice()
        .try_into()?;
    assert_eq!(u64::from_le_bytes(other_epoch), 1);
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn storage_manager_creates_foreign_key_and_advances_both_table_epochs()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe5);
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_create_admin"),
        object_write_with_name(actor, 8, "fk_create_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0xe6, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xe6), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let invocation_id = id(0xe7);
    let mut audit = identity_audit_event(0xe8, actor, id(82));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_named_foreign_key(
            prdb_engine::storage_manager::CreateForeignKeyRequest {
                invocation_id,
                authenticated_actor_id: actor,
                source_table_id: id(82),
                source_column_ids: vec![id(84)],
                target_constraint_id: id(85),
                retention_deadline_us: 9_999,
                audit_event: audit,
            },
            "orders_customer_fk".to_owned(),
        )
        .map_err(|error| format!("foreign-key create failed: {error:?}"))?;
    assert_ne!(created.foreign_key_id, [0; 16]);
    assert_eq!(created.commit_sequence, 2);
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing foreign-key create outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, created.foreign_key_id),
        id_catalog_key(2, created.foreign_key_id),
        dependency_write(created.foreign_key_id, id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_some(),
            "missing record: {key:?}"
        );
    }
    let foreign_key_object = catalog
        .get(id_catalog_key(2, created.foreign_key_id).as_slice())?
        .ok_or("foreign-key object missing")?;
    assert_eq!(
        decode_registered_envelope(foreign_key_object.value())?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("foreign-key object name missing")?
            .payload,
        b"orders_customer_fk"
    );
    drop(foreign_key_object);
    for table_id in [id(81), id(82)] {
        let key = id_catalog_key(5, table_id);
        let bytes = catalog
            .get(key.as_slice())?
            .ok_or("participating table disappeared during FK create")?
            .value()
            .to_vec();
        let epoch = decode_registered_envelope(&bytes)?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("missing participating table write epoch")?
            .payload
            .as_slice()
            .try_into()?;
        assert_eq!(u64::from_le_bytes(epoch), 2);
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}
