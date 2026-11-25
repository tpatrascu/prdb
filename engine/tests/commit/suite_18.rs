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
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::ReadableDatabase;
use std::sync::Arc;
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_drop_column_byte_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x9b);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x9c, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x9c),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "column_drop_bytes_owner"),
            object_write_with_name(actor, 8, "column_drop_bytes_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x9d);
    let mut schema_audit = identity_audit_event(0x9e, actor, id(0x9f));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_drop_bytes_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0xa0);
    let mut create_audit = identity_audit_event(0xa1, actor, id(0xa2));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "bounded_drop_bytes_table".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "removed".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "retained".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let old_columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
    ];
    let new_columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let retained_text = "r".repeat(400_000);
    let retained_tuple = encode_tuple(
        &new_columns,
        2,
        &[Some(ScalarValue::Text(retained_text.clone()))],
    )?;
    let empty_removed_tuple = encode_tuple(
        &old_columns,
        1,
        &[
            Some(ScalarValue::Text(String::new())),
            Some(ScalarValue::Text(retained_text.clone())),
        ],
    )?;
    let target_reject_bytes = 1024 * 1024;
    let removed_len = target_reject_bytes - retained_tuple.len() - empty_removed_tuple.len();
    let make_rows = |one_byte_over: bool| -> Result<Vec<RowWrite>, Box<dyn std::error::Error>> {
        (1u64..=64)
            .map(|row_number| {
                let extra = usize::from(one_byte_over && row_number == 64);
                let old_tuple = encode_tuple(
                    &old_columns,
                    1,
                    &[
                        Some(ScalarValue::Text("x".repeat(removed_len + extra))),
                        Some(ScalarValue::Text(retained_text.clone())),
                    ],
                )?;
                let combined = old_tuple.len() + retained_tuple.len();
                assert_eq!(combined, target_reject_bytes + extra);
                let mut row_suffix = [0; 16];
                row_suffix[8..].copy_from_slice(&row_number.to_be_bytes());
                Ok(RowWrite {
                    key: table_row_key(table.table_id, row_suffix),
                    expected_version: None,
                    payload: Some(old_tuple),
                })
            })
            .collect()
    };
    let rejected_rows = make_rows(true)?;
    let rejected_total = rejected_rows
        .iter()
        .map(|row| row.payload.as_ref().unwrap().len() + retained_tuple.len())
        .sum::<usize>();
    assert_eq!(rejected_total, 64 * 1024 * 1024 + 1);
    let rows_invocation = id(0xa3);
    let before_drop = commit_rows(&database, rows_invocation, actor, 9_999, &rejected_rows)?;
    let drop_invocation = id(0xa4);
    let mut drop_audit = identity_audit_event(0xa5, actor, id(0xa6));
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
            .ok_or("missing DROP COLUMN byte-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let remove_rows = rejected_rows
        .iter()
        .map(|row| RowWrite {
            key: row.key,
            expected_version: Some(before_drop),
            payload: None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        commit_rows(&database, id(0xa7), actor, 9_999, &remove_rows)?,
        before_drop + 1
    );
    let boundary_rows = make_rows(false)?;
    let boundary_total = boundary_rows
        .iter()
        .map(|row| row.payload.as_ref().unwrap().len() + retained_tuple.len())
        .sum::<usize>();
    assert_eq!(boundary_total, 64 * 1024 * 1024);
    let boundary_rows_invocation = id(0xa8);
    let before_boundary = commit_rows(
        &database,
        boundary_rows_invocation,
        actor,
        9_999,
        &boundary_rows,
    )?;
    let boundary_invocation = id(0xa9);
    let mut boundary_audit = identity_audit_event(0xaa, actor, id(0xab));
    boundary_audit.correlation_id = boundary_invocation;
    let dropped = storage.drop_column(
        boundary_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        9_999,
        boundary_audit,
    )?;
    assert_eq!(dropped.commit_sequence, before_boundary + 1);
    let mut first_row_suffix = [0; 16];
    first_row_suffix[8..].copy_from_slice(&1u64.to_be_bytes());
    let first_row_key = table_row_key(table.table_id, first_row_suffix);
    let retained_row = row(&database, &first_row_key)?.ok_or("row missing after DROP COLUMN")?;
    let decoded = decode_tuple(&new_columns, 2, &retained_row.payload)?;
    assert_eq!(decoded.values, vec![Some(ScalarValue::Text(retained_text))]);
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact-bound DROP COLUMN outcome")?
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn storage_manager_add_column_byte_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AddTableColumnRequest, CreateTableRequest, TableColumnDefault, TableColumnDefinition,
        TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x9b);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x9c, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x9c),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "column_bytes_owner"),
            object_write_with_name(actor, 8, "column_bytes_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x9d);
    let mut schema_audit = identity_audit_event(0x9e, actor, id(0x9f));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_bytes_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0xa0);
    let mut create_audit = identity_audit_event(0xa1, actor, id(0xa2));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "bounded_bytes_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;

    let mut rows = Vec::with_capacity(33);
    let mut combined_bytes = 0usize;
    for row_number in 1u64..=33 {
        let value_bytes = if row_number == 33 {
            1_016_779
        } else {
            1_016_763
        };
        let value = String::from_utf8(vec![b'x'; value_bytes])?;
        let payload = encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            }],
            1,
            &[Some(ScalarValue::Text(value))],
        )?;
        combined_bytes += 2 * payload.len() + 25;
        let mut row_suffix = [0; 16];
        row_suffix[8..].copy_from_slice(&row_number.to_be_bytes());
        rows.push(RowWrite {
            key: table_row_key(table.table_id, row_suffix),
            expected_version: None,
            payload: Some(payload),
        });
    }
    assert_eq!(combined_bytes, 64 * 1024 * 1024 + 1);
    let rows_invocation = id(0xa3);
    let before_add = commit_rows(&database, rows_invocation, actor, 9_999, &rows)?;
    let add_invocation = id(0xa4);
    let mut add_audit = identity_audit_event(0xa5, actor, id(0xa6));
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
            .ok_or("missing add-column byte-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let table_record = decode_registered_envelope(
        read.open_table(CATALOG)?
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing after add-column byte limit")?
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
    drop(read);

    let remove_rows_invocation = id(0xa7);
    let remove_rows = rows
        .iter()
        .map(|row| RowWrite {
            key: row.key,
            expected_version: Some(before_add),
            payload: None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        commit_rows(
            &database,
            remove_rows_invocation,
            actor,
            9_999,
            &remove_rows,
        )?,
        before_add + 1
    );

    let mut boundary_rows = Vec::with_capacity(34);
    let mut boundary_combined_bytes = 0usize;
    for row_number in 1u64..=34 {
        let value_bytes = if row_number > 32 { 986_858 } else { 986_857 };
        let value = String::from_utf8(vec![b'x'; value_bytes])?;
        let payload = encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            }],
            1,
            &[Some(ScalarValue::Text(value))],
        )?;
        boundary_combined_bytes += 2 * payload.len() + 26;
        let mut row_suffix = [0; 16];
        row_suffix[8..].copy_from_slice(&row_number.to_be_bytes());
        boundary_rows.push(RowWrite {
            key: table_row_key(table.table_id, row_suffix),
            expected_version: None,
            payload: Some(payload),
        });
    }
    assert_eq!(boundary_combined_bytes, 64 * 1024 * 1024);
    let boundary_rows_invocation = id(0xa8);
    let before_boundary_add = commit_rows(
        &database,
        boundary_rows_invocation,
        actor,
        9_999,
        &boundary_rows,
    )?;
    let boundary_invocation = id(0xa9);
    let mut boundary_audit = identity_audit_event(0xaa, actor, id(0xab));
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
            default: Some(TableColumnDefault::Literal(Some(ScalarValue::Text(
                "x".to_owned(),
            )))),
        },
        retention_deadline_us: 9_999,
        audit_event: boundary_audit,
    })?;
    assert_eq!(boundary_add.schema_version, 2);
    assert_eq!(boundary_add.commit_sequence, before_boundary_add + 1);
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact byte-bound add-column outcome")?
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

pub(super) fn catalog_commit_rejects_removing_a_mapped_builtin_type()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let type_key = id_catalog_key(4, builtin_id(101));
    let expected_value = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(type_key.as_slice())?
        .ok_or("missing bootstrap type fixture")?
        .value()
        .to_vec();
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(97),
            id(9),
            9_999,
            &[CatalogWrite {
                key: type_key,
                expected_value: Some(expected_value),
                payload: None,
            }],
            &[audit_event(97)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_rejects_unassigned_builtin_type_profiles()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let type_key = id_catalog_key(4, builtin_id(101));
    let expected_value = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(type_key.as_slice())?
        .ok_or("missing bootstrap type fixture")?
        .value()
        .to_vec();
    let mut unsupported = builtin_type_write_with_profile(builtin_id(101), 1, 0, 1, 1, 1, 2);
    unsupported.expected_value = Some(expected_value);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(102),
            id(9),
            10_004,
            &[unsupported],
            &[audit_event(102)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_rejects_duplicate_builtin_type_kinds()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(103),
            id(9),
            10_005,
            &[builtin_type_write(id(90), 1), object_write(id(90), 2)],
            &[audit_event(103)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_rejects_removing_the_bootstrap_descriptor()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let database_key = id_catalog_key(1, [0x33; 16]);
    let expected_value = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(database_key.as_slice())?
        .ok_or("missing bootstrap database fixture")?
        .value()
        .to_vec();
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(99),
            id(9),
            10_001,
            &[CatalogWrite {
                key: database_key,
                expected_value: Some(expected_value),
                payload: None,
            }],
            &[audit_event(99)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_rejects_renaming_the_mapped_admin_role()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_id = builtin_owner_id();
    let principal_key = id_catalog_key(10, admin_id);
    let object_key = id_catalog_key(2, admin_id);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let principal_value = catalog
        .get(principal_key.as_slice())?
        .ok_or("missing bootstrap principal fixture")?
        .value()
        .to_vec();
    let object_value = catalog
        .get(object_key.as_slice())?
        .ok_or("missing bootstrap object fixture")?
        .value()
        .to_vec();
    drop(catalog);
    drop(read);

    let mut principal = principal_write_with_name(admin_id, 2, 1, "other_role");
    principal.expected_value = Some(principal_value);
    let mut object = object_write_with_name(admin_id, 9, "other_role");
    object.expected_value = Some(object_value);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(98),
            id(9),
            10_000,
            &[principal, object],
            &[audit_event(98)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_requires_typed_rows_for_every_object()
-> Result<(), Box<dyn std::error::Error>> {
    let orphan_fixture = Fixture::new();
    let orphan_database = initialize(&orphan_fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &orphan_database,
            id(100),
            id(9),
            10_002,
            &[object_write(id(90), 1)],
            &[audit_event(100)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&orphan_database)?, 0);

    let deletion_fixture = Fixture::new();
    let deletion_database = initialize(&deletion_fixture.path())?;
    let schema_key = id_catalog_key(3, builtin_id(1));
    let expected_schema = deletion_database
        .begin_read()?
        .open_table(CATALOG)?
        .get(schema_key.as_slice())?
        .ok_or("missing bootstrap schema fixture")?
        .value()
        .to_vec();
    assert_eq!(
        commit_catalog_and_audit(
            &deletion_database,
            id(101),
            id(9),
            10_003,
            &[CatalogWrite {
                key: schema_key,
                expected_value: Some(expected_schema),
                payload: None,
            }],
            &[audit_event(101)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&deletion_database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_rejects_malformed_credential_verifier_profiles()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let key = id_catalog_key(16, id(71));
    let payload = encode_envelope(
        &family_schema(16).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: id(70).to_vec(),
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
                wire_type: WireType::Bytes,
                payload: b"synthetic-verifier".to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::I64,
                payload: 1i64.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::I64,
                payload: 2i64.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::Bool,
                payload: vec![0],
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::Text,
                payload: b"synthetic".to_vec(),
            },
            CatalogField {
                tag: 10,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let mut writes = vec![
        principal_write_with_name(id(70), 1, 1, "credential_user"),
        object_write_with_name(id(70), 8, "credential_user"),
    ];
    writes.push(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    });
    assert_eq!(
        commit_catalog_and_audit(&database, id(72), id(9), 9_999, &writes, &[audit_event(72)],),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn token_authenticated_catalog_scans_redact_cells_per_principal()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{Expr, SanitizedCell};
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientMode};
    use prdb_engine::supervisor::{
        StagingLimits, StagingStore, SubmissionMetadata, SubmissionMetadataValidator,
    };
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
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_id = id(0x51);
    let admin_role = builtin_owner_id();
    let admin_token_id = id(0x50);
    let admin_secret = [0x4f; 32];
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let retention_deadline_us = now + 3_600_000_000;

    let mut setup_audit = identity_audit_event(0x52, admin_role, admin_id);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x52),
        admin_role,
        retention_deadline_us,
        &[
            principal_write_with_name(admin_id, 1, 1, "cell_view_admin"),
            object_write_with_name(admin_id, 8, "cell_view_admin"),
            membership_write_with_grantor(admin_id, admin_role, admin_role),
            dependency_write(admin_id, admin_role, 5),
            credential_verifier_write(
                admin_token_id,
                admin_id,
                &admin_secret,
                now - 1_000_000,
                retention_deadline_us,
                None,
                1,
            ),
        ],
        &[setup_audit],
    )?;

    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptUpload))?;
    let mut admin_session = ClientConnectionSession::default();
    admin_session.accept_hello(ClientMode::Control, "cell-view-setup")?;
    admin_session.authenticate(&storage, admin_token_id, &admin_secret)?;
    let mut next_admin_request_id = 1u64;
    let mut admin_execute = |prepare_payload: &str| -> Result<String, Box<dyn std::error::Error>> {
        let prepared = gateway_service_admin_json(
            &storage,
            &staging,
            &mut admin_session,
            next_admin_request_id,
            prepare_payload,
            retention_deadline_us,
        )?;
        next_admin_request_id += 1;
        let operation_id = prepared
            .split("\"operation_id\":\"")
            .nth(1)
            .ok_or("missing ADMIN operation ID")?
            .get(..32)
            .ok_or("invalid ADMIN operation ID")?;
        if !prepared.ends_with("\",\"state\":\"prepared\"}") {
            return Err(std::io::Error::other(format!(
                "ADMIN operation was not prepared: {prepared}"
            ))
            .into());
        }
        let started = gateway_service_admin_json(
            &storage,
            &staging,
            &mut admin_session,
            next_admin_request_id,
            &format!("{{\"phase\":\"start\",\"operation_id\":\"{operation_id}\"}}"),
            retention_deadline_us,
        )?;
        next_admin_request_id += 1;
        if !started.contains("\"outcome\":\"committed\"") {
            return Err(std::io::Error::other(format!(
                "ADMIN operation did not commit: {started}"
            ))
            .into());
        }
        Ok(started)
    };
    let response_id = |response: &str, key: &str| -> Result<[u8; 16], Box<dyn std::error::Error>> {
        let wire = response
            .split(&format!("\"{key}\":\""))
            .nth(1)
            .ok_or_else(|| std::io::Error::other(format!("missing ADMIN {key}")))?
            .get(..32)
            .ok_or_else(|| std::io::Error::other(format!("invalid ADMIN {key}")))?;
        let mut id = [0; 16];
        for (index, byte) in id.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&wire[index * 2..index * 2 + 2], 16)?;
        }
        Ok(id)
    };
    let first_user_response = admin_execute(
        r#"{"phase":"prepare","operation_version":1,"operation":"users.create","arguments":{"name":"cell_view_reader"}}"#,
    )?;
    let first_user_id = response_id(&first_user_response, "user_id")?;
    let restricted_user_response = admin_execute(
        r#"{"phase":"prepare","operation_version":1,"operation":"users.create","arguments":{"name":"cell_view_restricted"}}"#,
    )?;
    let restricted_user_id = response_id(&restricted_user_response, "user_id")?;
    let first_token_response = admin_execute(&format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{}\",\"label\":\"cell view reader\",\"expires_at_us\":\"{retention_deadline_us}\"}}}}",
        first_user_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))?;
    let first_token_id = response_id(&first_token_response, "token_id")?;
    let first_token_secret = first_token_response
        .split("\"token_secret\":\"")
        .nth(1)
        .ok_or("missing issued first token secret")?
        .get(..44)
        .ok_or("invalid issued first token secret")?;
    let first_credentials_payload = format!(
        "{{\"token_id\":\"{}\",\"token_secret\":\"{first_token_secret}\"}}",
        first_token_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let first_credentials = prdb_engine::client_protocol::decode_client_credentials(
        first_credentials_payload.as_bytes(),
    )?;
    let first_secret = first_credentials.token_secret;
    drop(first_credentials);
    let restricted_token_response = admin_execute(&format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{}\",\"label\":\"cell view restricted\",\"expires_at_us\":\"{retention_deadline_us}\"}}}}",
        restricted_user_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))?;
    let restricted_token_id = response_id(&restricted_token_response, "token_id")?;
    let restricted_token_secret = restricted_token_response
        .split("\"token_secret\":\"")
        .nth(1)
        .ok_or("missing issued restricted token secret")?
        .get(..44)
        .ok_or("invalid issued restricted token secret")?;
    let restricted_credentials_payload = format!(
        "{{\"token_id\":\"{}\",\"token_secret\":\"{restricted_token_secret}\"}}",
        restricted_token_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let restricted_credentials = prdb_engine::client_protocol::decode_client_credentials(
        restricted_credentials_payload.as_bytes(),
    )?;
    let restricted_secret = restricted_credentials.token_secret;
    drop(restricted_credentials);

    let schema_response = admin_execute(&format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.create\",\"arguments\":{{\"name\":\"cell_views\",\"owner_id\":\"{}\"}}}}",
        admin_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))?;
    let schema_id = response_id(&schema_response, "schema_id")?;
    let schema_id_wire = schema_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let table_response = admin_execute(&format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.create\",\"arguments\":{{\"schema_id\":\"{schema_id_wire}\",\"name\":\"records\",\"columns\":[{{\"name\":\"secret\",\"type\":\"bytea\",\"nullable\":false,\"generated_uuid_identity\":false}},{{\"name\":\"optional_note\",\"type\":\"text\",\"nullable\":true,\"generated_uuid_identity\":false}}]}}}}"
    ))?;
    let table_id = response_id(&table_response, "table_id")?;
    let column_ids_wire = table_response
        .split("\"column_ids\":[\"")
        .nth(1)
        .ok_or("missing table column ID")?
        .split_once("\",\"")
        .ok_or("missing second table column ID")?;
    let decode_id = |wire: &str| -> Result<[u8; 16], Box<dyn std::error::Error>> {
        let wire = wire.get(..32).ok_or("invalid table column ID")?;
        let mut parsed = [0; 16];
        for (index, byte) in parsed.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&wire[index * 2..index * 2 + 2], 16)?;
        }
        Ok(parsed)
    };
    let column_id = decode_id(column_ids_wire.0)?;
    let nullable_column_id = decode_id(column_ids_wire.1)?;
    let id_wire = |value: [u8; 16]| {
        value
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    for principal_id in [first_user_id, restricted_user_id] {
        for (object_id, privilege, target_scope, target_id) in [
            (schema_id, "usage", "object", schema_id),
            (table_id, "select", "object", table_id),
            (table_id, "select", "column", column_id),
        ] {
            admin_execute(&format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"objects.grant\",\"arguments\":{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",\"privilege\":\"{privilege}\",\"target_scope\":\"{target_scope}\",\"target_id\":\"{}\"}}}}",
                id_wire(principal_id),
                id_wire(object_id),
                id_wire(target_id)
            ))?;
        }
    }
    admin_execute(&format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"objects.grant\",\"arguments\":{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",\"privilege\":\"select\",\"target_scope\":\"column\",\"target_id\":\"{}\"}}}}",
        id_wire(first_user_id),
        id_wire(table_id),
        id_wire(nullable_column_id)
    ))?;
    let expression = prdb_engine::expression::encode_expression(&Expr::Literal(Some(
        ScalarValue::Boolean(true),
    )))?
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect::<String>();
    for (recipient, kind, target_columns, name) in [
        (first_user_id, "row", String::new(), "reader_row"),
        (restricted_user_id, "row", String::new(), "restricted_row"),
        (
            first_user_id,
            "cell",
            format!("\"{}\"", id_wire(column_id)),
            "reader_cell",
        ),
        (
            first_user_id,
            "cell",
            format!("\"{}\"", id_wire(nullable_column_id)),
            "reader_null_cell",
        ),
    ] {
        admin_execute(&format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"policies.create\",\"arguments\":{{\"table_id\":\"{}\",\"name\":\"{name}\",\"kind\":\"{kind}\",\"policy_operation\":\"select\",\"effect\":\"allow\",\"recipients\":[\"user:{}\"],\"target_column_ids\":[{target_columns}],\"using_expression_eir1\":\"{expression}\",\"with_check_expression_eir1\":null}}}}",
            id_wire(table_id),
            id_wire(recipient)
        ))?;
    }

    let row_schema = [
        ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
    ];
    let short_row_payload = encode_tuple(
        &row_schema,
        1,
        &[Some(ScalarValue::Bytea(vec![0x53])), None],
    )?;
    let long_row_payload = encode_tuple(
        &row_schema,
        1,
        &[Some(ScalarValue::Bytea(vec![0x54; 2048])), None],
    )?;
    commit_rows(
        &database,
        id(0x80),
        admin_id,
        retention_deadline_us,
        &[
            RowWrite {
                key: table_row_key(table_id, id(0x7e)),
                expected_version: None,
                payload: Some(short_row_payload),
            },
            RowWrite {
                key: table_row_key(table_id, id(0x7f)),
                expected_version: None,
                payload: Some(long_row_payload),
            },
        ],
    )?;
    let index_response = admin_execute(&format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"indexes.create\",\"arguments\":{{\"table_id\":\"{}\",\"name\":\"records_secret_unique\",\"column_ids\":[\"{}\"],\"unique\":true}}}}",
        id_wire(table_id),
        id_wire(column_id)
    ))?;
    let index_id = response_id(&index_response, "index_id")?;
    let range_index_response = admin_execute(&format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"indexes.create\",\"arguments\":{{\"table_id\":\"{}\",\"name\":\"records_secret_range\",\"column_ids\":[\"{}\"],\"unique\":false}}}}",
        id_wire(table_id),
        id_wire(column_id)
    ))?;
    let range_index_id = response_id(&range_index_response, "index_id")?;

    let owner_objects = {
        let read = database.begin_read()?;
        prdb_engine::catalog_index::scan_object_ids_for_owner(&read, admin_id, 4096)?
    };
    assert!(
        owner_objects.contains(&table_id),
        "catalog fixture must make the database administrator the table owner"
    );
    let (mut owner_read, owner_roles) = storage.begin_token_authenticated_invocation(
        id(0x8d),
        admin_token_id,
        &admin_secret,
        retention_deadline_us,
        4,
        4096,
    )?;
    assert!(owner_roles.contains(&admin_role));
    let owner_row_result = owner_read.read_catalog_authorized_row(table_id, id(0x7e));
    assert!(
        matches!(&owner_row_result, Ok(None)),
        "owner without data grants must not read a stored row: {owner_row_result:?}"
    );
    owner_read.commit()?;
    assert_eq!(
        lookup_outcome(&database, id(0x8d), admin_id, 1)?
            .ok_or("owner row-read denial has no durable outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let (mut owner_scan, owner_roles) = storage.begin_token_authenticated_invocation(
        id(0x8e),
        admin_token_id,
        &admin_secret,
        retention_deadline_us,
        4,
        4096,
    )?;
    assert!(owner_roles.contains(&admin_role));
    let owner_scan_page = owner_scan.scan_catalog_authorized_rows(table_id, None, 8)?;
    assert!(owner_scan_page.rows.is_empty());
    owner_scan.commit()?;
    assert_eq!(
        lookup_outcome(&database, id(0x8e), admin_id, 1)?
            .ok_or("owner table-scan denial has no durable outcome")?
            .status,
        OutcomeStatus::Committed
    );

    admin_execute(&format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"objects.grant\",\"arguments\":{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",\"privilege\":\"insert\",\"target_scope\":\"object\",\"target_id\":\"{}\"}}}}",
        id_wire(first_user_id),
        id_wire(table_id),
        id_wire(table_id)
    ))?;
    for insert_column_id in [column_id, nullable_column_id] {
        admin_execute(&format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"objects.grant\",\"arguments\":{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",\"privilege\":\"insert\",\"target_scope\":\"column\",\"target_id\":\"{}\"}}}}",
            id_wire(first_user_id),
            id_wire(table_id),
            id_wire(insert_column_id)
        ))?;
    }
    for (kind, target_columns, name) in [
        ("row", String::new(), "overlay_insert_row"),
        (
            "cell",
            format!("\"{}\"", id_wire(column_id)),
            "overlay_insert_key",
        ),
        (
            "cell",
            format!("\"{}\"", id_wire(nullable_column_id)),
            "overlay_insert_nullable",
        ),
    ] {
        admin_execute(&format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"policies.create\",\"arguments\":{{\"table_id\":\"{}\",\"name\":\"{name}\",\"kind\":\"{kind}\",\"policy_operation\":\"insert\",\"effect\":\"allow\",\"recipients\":[\"user:{}\"],\"target_column_ids\":[{target_columns}],\"using_expression_eir1\":null,\"with_check_expression_eir1\":\"{expression}\"}}}}",
            id_wire(table_id),
            id_wire(first_user_id)
        ))?;
    }
    let (mut overlay_invocation, _) = storage.begin_token_authenticated_invocation(
        id(0x83),
        first_token_id,
        &first_secret,
        retention_deadline_us,
        4,
        4096,
    )?;
    {
        let mut executor = overlay_invocation.open_executor_metadata_connection();
        let (relation, _) = executor
            .resolve_relation("cell_views", "records")?
            .ok_or("authorized write relation was unavailable")?;
        executor
            .stage_insert(
                &relation,
                &[
                    prdb_engine::transaction::InsertInput::Value(Some(ScalarValue::Bytea(vec![
                        0x56,
                    ]))),
                    prdb_engine::transaction::InsertInput::Value(None),
                ],
            )
            .map_err(|error| format!("stage private index-overlay insert failed: {error:?}"))?;
    }
    let overlay_range = |value: Vec<u8>| {
        Some(prdb_engine::transaction::AuthorizedIndexRangeBound {
            value: ScalarValue::Bytea(value),
            inclusive: true,
        })
    };
    assert_eq!(
        executor_ipc_scan_index_range(
            &mut overlay_invocation,
            "cell_views",
            "records",
            range_index_id,
            overlay_range(vec![0x56]),
            overlay_range(vec![0x56]),
            vec![column_id],
        )?,
        prdb_engine::executor_ipc::ScanIndexRangeResponse::Rows(vec![vec![SanitizedCell::Value(
            ScalarValue::Bytea(vec![0x56])
        ),]]),
        "exact-key range lookup must see a staged row through its private index overlay"
    );
    overlay_invocation.abort()?;

    let (mut first_invocation, first_roles) = storage.begin_token_authenticated_invocation(
        id(0x81),
        first_token_id,
        &first_secret,
        retention_deadline_us,
        4,
        4096,
    )?;
    let (mut restricted_invocation, restricted_roles) = storage
        .begin_token_authenticated_invocation(
            id(0x82),
            restricted_token_id,
            &restricted_secret,
            retention_deadline_us,
            4,
            4096,
        )?;
    assert!(first_roles.is_empty());
    assert!(restricted_roles.is_empty());
    let first_plan_relation = first_invocation
        .open_planner_metadata_connection()
        .describe_relation_for_plan("cell_views", "records")?
        .ok_or("authorized planner relation was unavailable")?;
    assert_eq!(first_plan_relation.unique_access_paths.len(), 1);
    assert_eq!(
        first_plan_relation.unique_access_paths[0].index_id,
        index_id
    );
    assert_eq!(
        first_plan_relation.unique_access_paths[0].column_ids,
        vec![column_id]
    );
    assert_eq!(first_plan_relation.range_access_paths.len(), 1);
    assert_eq!(
        first_plan_relation.range_access_paths[0].index_id,
        range_index_id
    );
    assert_eq!(
        first_plan_relation.range_access_paths[0].column_ids,
        vec![column_id]
    );
    let plan_relation_payload = prdb_engine::planner_ipc::DescribePlanRelationResponse {
        relation: Some(first_plan_relation.clone()),
        proven_empty: true,
        visible_rows_estimate: Some(0),
    }
    .encode()?;
    assert_eq!(
        prdb_engine::planner_ipc::DescribePlanRelationResponse::decode(&plan_relation_payload)?,
        prdb_engine::planner_ipc::DescribePlanRelationResponse {
            relation: Some(first_plan_relation.clone()),
            proven_empty: true,
            visible_rows_estimate: Some(0),
        }
    );
    let mut executor_identity_expected = first_plan_relation.clone();
    executor_identity_expected.unique_access_paths.clear();
    executor_identity_expected.range_access_paths.clear();
    let executor_identity_payload = prdb_engine::planner_ipc::DescribePlanRelationResponse {
        relation: Some(first_plan_relation.clone()),
        proven_empty: true,
        visible_rows_estimate: Some(0),
    }
    .encode_executor_identity()?;
    assert_eq!(
        prdb_engine::planner_ipc::DescribePlanRelationResponse::decode(&executor_identity_payload)?,
        prdb_engine::planner_ipc::DescribePlanRelationResponse {
            relation: Some(executor_identity_expected),
            proven_empty: false,
            visible_rows_estimate: None,
        }
    );
    let marker_offset = plan_relation_payload
        .windows(2)
        .rposition(|window| window == [0x52, 0x01])
        .ok_or("planner response omitted range-path marker")?;
    let mut zero_range_count = plan_relation_payload.clone();
    zero_range_count[marker_offset + 2..marker_offset + 4].copy_from_slice(&[0, 0]);
    assert!(
        prdb_engine::planner_ipc::DescribePlanRelationResponse::decode(&zero_range_count).is_err()
    );
    let mut duplicate_path_id = plan_relation_payload.clone();
    duplicate_path_id[marker_offset + 4..marker_offset + 20].copy_from_slice(&index_id);
    assert!(
        prdb_engine::planner_ipc::DescribePlanRelationResponse::decode(&duplicate_path_id).is_err()
    );
    let restricted_plan_relation = restricted_invocation
        .open_planner_metadata_connection()
        .describe_relation_for_plan("cell_views", "records")?
        .ok_or("authorized restricted planner relation was unavailable")?;
    assert!(restricted_plan_relation.unique_access_paths.is_empty());
    assert!(restricted_plan_relation.range_access_paths.is_empty());
    let first_cells =
        executor_ipc_scan_cells(&mut first_invocation, "cell_views", "records", vec![])?;
    let restricted_cells =
        executor_ipc_scan_cells(&mut restricted_invocation, "cell_views", "records", vec![])?;
    assert_eq!(first_cells.len(), 2);
    let mut visible_lengths = first_cells
        .iter()
        .map(|row| match &row[0] {
            SanitizedCell::Value(ScalarValue::Bytea(bytes)) => bytes.len(),
            _ => panic!("reader must see both authorized values"),
        })
        .collect::<Vec<_>>();
    visible_lengths.sort_unstable();
    assert_eq!(visible_lengths, [1, 2048]);
    assert_eq!(
        first_cells.iter().map(|row| &row[1]).collect::<Vec<_>>(),
        vec![&SanitizedCell::Null, &SanitizedCell::Null],
        "a readable SQL NULL remains distinct from redaction"
    );
    assert_eq!(
        restricted_cells,
        vec![vec![SanitizedCell::Redacted, SanitizedCell::Redacted]; 2]
    );
    let reader_match = executor_ipc_probe_unique(
        &mut first_invocation,
        "cell_views",
        "records",
        index_id,
        ScalarValue::Bytea(vec![0x53]),
        vec![column_id],
    )?;
    let prdb_engine::executor_ipc::ProbeUniqueIndexResponse::Match(row) = reader_match else {
        return Err(format!("authorized unique probe returned {reader_match:?}").into());
    };
    assert_eq!(
        row.handle.to_bytes()[0],
        prdb_engine::metadata_discovery::EXECUTOR_ROW_HANDLE_TOKEN_KIND
    );
    assert_eq!(
        row.cells,
        vec![SanitizedCell::Value(ScalarValue::Bytea(vec![0x53]))]
    );
    assert_eq!(
        executor_ipc_probe_unique(
            &mut first_invocation,
            "cell_views",
            "records",
            index_id,
            ScalarValue::Bytea(vec![0x55; 9]),
            vec![column_id],
        )?,
        prdb_engine::executor_ipc::ProbeUniqueIndexResponse::NoMatch
    );
    let range_bound = |value: Vec<u8>| {
        Some(prdb_engine::transaction::AuthorizedIndexRangeBound {
            value: ScalarValue::Bytea(value),
            inclusive: true,
        })
    };
    assert_eq!(
        executor_ipc_scan_index_range(
            &mut first_invocation,
            "cell_views",
            "records",
            range_index_id,
            range_bound(vec![0x53]),
            range_bound(vec![0x53]),
            vec![column_id],
        )?,
        prdb_engine::executor_ipc::ScanIndexRangeResponse::Rows(vec![vec![SanitizedCell::Value(
            ScalarValue::Bytea(vec![0x53])
        ),]]),
    );
    assert_eq!(
        executor_ipc_scan_index_range(
            &mut first_invocation,
            "cell_views",
            "records",
            range_index_id,
            range_bound(vec![0x55; 9]),
            range_bound(vec![0x55; 9]),
            vec![column_id],
        )?,
        prdb_engine::executor_ipc::ScanIndexRangeResponse::Rows(vec![]),
    );
    assert_eq!(
        executor_ipc_scan_index_range(
            &mut first_invocation,
            "cell_views",
            "records",
            range_index_id,
            range_bound(vec![0x56]),
            range_bound(vec![0x56]),
            vec![column_id],
        )?,
        prdb_engine::executor_ipc::ScanIndexRangeResponse::Rows(vec![]),
        "aborted private index membership must not leak to another invocation"
    );
    for value in [vec![0x53], vec![0x55; 9]] {
        assert_eq!(
            executor_ipc_probe_unique(
                &mut restricted_invocation,
                "cell_views",
                "records",
                index_id,
                ScalarValue::Bytea(value.clone()),
                vec![column_id],
            )?,
            prdb_engine::executor_ipc::ProbeUniqueIndexResponse::Unavailable,
            "hidden unique keys must fall back identically for matching and absent values"
        );
        assert_eq!(
            executor_ipc_scan_index_range(
                &mut restricted_invocation,
                "cell_views",
                "records",
                range_index_id,
                range_bound(value.clone()),
                range_bound(value),
                vec![column_id],
            )?,
            prdb_engine::executor_ipc::ScanIndexRangeResponse::Unavailable,
            "hidden range keys must not distinguish matching and absent values"
        );
    }
    first_invocation.abort()?;
    restricted_invocation.abort()?;
    assert!(prdb_engine::startup::validate_persisted_records(&database).is_ok());
    Ok(())
}
