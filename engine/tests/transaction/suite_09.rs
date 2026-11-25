use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, SanitizedCell, Subject,
};
use prdb_engine::catalog::{CatalogField, WireType};
use prdb_engine::commit::{CatalogWrite, CommitError, commit_catalog_and_audit};
use prdb_engine::schema::CATALOG;
use prdb_engine::transaction::{InvocationTransaction, TransactionError};
use prdb_engine::tuple::{ScalarType, ScalarValue, encode_tuple};
use redb::ReadableDatabase;

use super::helpers::*;

pub(super) fn composite_index_prefix_scan_reconciles_snapshot_and_private_overlay()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [0x20; 16];
    let schema_id = [0x19; 16];
    let first_column = [0x21; 16];
    let second_column = [0x22; 16];
    let index_id = [0x23; 16];
    let row_id = |value| prdb_engine::overlay::RowId {
        table: table_id,
        row: [value; 16],
    };
    let table_key = catalog_id_key(5, table_id);
    let table = catalog_record_payload(
        5,
        &table_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: schema_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: [0x24; 16].to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let column_record = |column_id: [u8; 16], ordinal: u32, name: &[u8]| {
        let key = catalog_id_key(6, column_id);
        let payload = catalog_record_payload(
            6,
            &key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U32,
                    payload: ordinal.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: name.to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: [0x40; 16].to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Bytes,
                    payload: vec![0; 8],
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Bool,
                    payload: vec![1],
                },
                CatalogField {
                    tag: 8,
                    wire_type: WireType::U16,
                    payload: 0u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 9,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ],
        )?;
        Ok::<_, Box<dyn std::error::Error>>((key, payload))
    };
    let mut index_columns = 2u32.to_le_bytes().to_vec();
    index_columns.extend_from_slice(&first_column);
    index_columns.extend_from_slice(&second_column);
    let index_key = catalog_id_key(9, index_id);
    let index = catalog_record_payload(
        9,
        &index_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::Ids,
                payload: index_columns,
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bool,
                payload: vec![0],
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::Id,
                payload: index_id.to_vec(),
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 10,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let first = column_record(first_column, 0, b"region")?;
    let second = column_record(second_column, 1, b"code")?;
    let table_object = object_catalog_record_for(table_id, 3);
    let schema_object = object_catalog_record_for(schema_id, 1);
    let first_object = object_catalog_record_for(first_column, 4);
    let second_object = object_catalog_record_for(second_column, 4);
    let mut schema_tables = 1u32.to_le_bytes().to_vec();
    schema_tables.extend_from_slice(&table_id);
    let schema_key = catalog_id_key(3, schema_id);
    let schema = catalog_record_payload(
        3,
        &schema_key,
        &[CatalogField {
            tag: 1,
            wire_type: WireType::Ids,
            payload: schema_tables,
        }],
    )?;
    {
        let write = database.begin_write()?;
        let mut catalog = write.open_table(CATALOG)?;
        for (key, value) in [
            (table_key, table),
            (schema_key, schema),
            first,
            second,
            (index_key, index),
            table_object,
            schema_object,
            first_object,
            second_object,
        ] {
            catalog.insert(key.as_slice(), value.as_slice())?;
        }
        drop(catalog);
        write.commit()?;
    }

    let mut seed = InvocationTransaction::begin(&database, [0x25; 16], [9; 16], 9_999, 8, 4_096)?;
    let schema = seed
        .load_catalog_table_schema(table_id)
        .map_err(|error| format!("load seed schema: {error:?}"))?;
    seed.load_catalog_indexes(&schema)
        .map_err(|error| format!("load seed indexes: {error:?}"))?;
    let encode = |region: Option<&[u8]>, code: Option<&[u8]>| {
        encode_tuple(
            &schema.columns,
            schema.schema_version,
            &[
                region.map(|value| ScalarValue::Bytea(value.to_vec())),
                code.map(|value| ScalarValue::Bytea(value.to_vec())),
            ],
        )
    };
    for (row, region, code) in [
        (0x31, Some(b"north".as_slice()), Some(b"a".as_slice())),
        (0x32, Some(b"north".as_slice()), Some(b"b".as_slice())),
        (0x33, Some(b"south".as_slice()), Some(b"a".as_slice())),
        (0x34, None, Some(b"a".as_slice())),
    ] {
        seed.stage_put(row_id(row), &encode(region, code)?)
            .map_err(|error| format!("stage seed row {row:#x}: {error:?}"))?;
    }
    seed.commit()
        .map_err(|error| format!("commit seed rows: {error:?}"))?;

    let principal_id = [9; 16];
    let table_target = AccessTarget::Table(table_id);
    let first_target = AccessTarget::Column {
        table_id,
        column_id: first_column,
    };
    let second_target = AccessTarget::Column {
        table_id,
        column_id: second_column,
    };
    let context = AuthorizationContext {
        principal_id,
        role_ids: Default::default(),
        scope: [
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, table_target),
            (Operation::Select, first_target),
            (Operation::Select, second_target),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: table_target,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: first_target,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: second_target,
        },
    ];
    let policies = [table_target, first_target, second_target].map(|target| Policy {
        subjects: vec![Subject::Principal(principal_id)],
        operation: Operation::Select,
        target,
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    });
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
        dependencies: &[],
    };
    let mut invocation =
        InvocationTransaction::begin(&database, [0x26; 16], principal_id, 9_999, 8, 4_096)?;
    let key_values = [
        ScalarValue::Bytea(b"north".to_vec()),
        ScalarValue::Bytea(b"a".to_vec()),
    ];
    assert_eq!(
        invocation
            .scan_catalog_index_prefix(table_id, index_id, &key_values, &projection)
            .map_err(|error| format!("snapshot composite-prefix scan: {error:?}"))?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(vec![(
            row_id(0x31),
            vec![
                SanitizedCell::Value(ScalarValue::Bytea(b"north".to_vec())),
                SanitizedCell::Value(ScalarValue::Bytea(b"a".to_vec())),
            ],
        )]),
    );
    invocation
        .stage_put(row_id(0x35), &encode(Some(b"north"), Some(b"a"))?)
        .map_err(|error| format!("stage overlay insert: {error:?}"))?;
    invocation
        .stage_put(row_id(0x31), &encode(Some(b"north"), Some(b"z"))?)
        .map_err(|error| format!("stage overlay update: {error:?}"))?;
    assert!(matches!(
        invocation
            .scan_catalog_index_prefix(table_id, index_id, &key_values, &projection)
            .map_err(|error| format!("overlay composite-prefix scan: {error:?}"))?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
            if rows.len() == 1 && rows[0].0 == row_id(0x35)
    ));
    invocation
        .stage_delete(row_id(0x35))
        .map_err(|error| format!("stage overlay delete: {error:?}"))?;
    assert_eq!(
        invocation
            .scan_catalog_index_prefix(table_id, index_id, &key_values, &projection)
            .map_err(|error| format!("deleted composite-prefix scan: {error:?}"))?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(Vec::new()),
    );
    invocation.abort()?;
    Ok(())
}

pub(super) fn schema_loader_uses_each_columns_type_parameters()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [200; 16];
    let table_key = catalog_id_key(5, table_id);
    let field = |tag, wire_type, payload| CatalogField {
        tag,
        wire_type,
        payload,
    };
    let table = catalog_record_payload(
        5,
        &table_key,
        &[
            field(1, WireType::Id, vec![201; 16]),
            field(2, WireType::U64, 1u64.to_le_bytes().to_vec()),
            field(3, WireType::U64, 1u64.to_le_bytes().to_vec()),
            field(4, WireType::Id, vec![202; 16]),
            field(5, WireType::U16, 1u16.to_le_bytes().to_vec()),
        ],
    )?;
    let mut records = vec![(table_key, table)];
    for (type_id, kind, rule) in [([203; 16], 7u16, 2u16), ([204; 16], 5, 1)] {
        let key = catalog_id_key(4, type_id);
        let value = catalog_record_payload(
            4,
            &key,
            &[
                field(1, WireType::U16, kind.to_le_bytes().to_vec()),
                field(2, WireType::U16, rule.to_le_bytes().to_vec()),
                field(3, WireType::U32, 1u32.to_le_bytes().to_vec()),
                field(4, WireType::U32, 1u32.to_le_bytes().to_vec()),
                field(5, WireType::U32, 1u32.to_le_bytes().to_vec()),
                field(6, WireType::U32, 1u32.to_le_bytes().to_vec()),
            ],
        )?;
        records.push((key, value));
    }
    let column = |column_id: [u8; 16], ordinal: u32, type_id: [u8; 16], parameters: [u8; 8]| {
        let key = catalog_id_key(6, column_id);
        let value = catalog_record_payload(
            6,
            &key,
            &[
                field(1, WireType::Id, table_id.to_vec()),
                field(2, WireType::U32, ordinal.to_le_bytes().to_vec()),
                field(3, WireType::Text, format!("c{ordinal}").into_bytes()),
                field(4, WireType::Id, type_id.to_vec()),
                field(5, WireType::Bytes, parameters.to_vec()),
                field(6, WireType::Bool, vec![1]),
                field(8, WireType::U16, 0u16.to_le_bytes().to_vec()),
                field(9, WireType::U64, 1u64.to_le_bytes().to_vec()),
            ],
        )?;
        Ok::<_, Box<dyn std::error::Error>>((key, value))
    };
    let varchar_params = |limit: u32| {
        let mut p = [0; 8];
        p[4..].copy_from_slice(&limit.to_le_bytes());
        p
    };
    let decimal_params = |precision: u16, scale: u16| {
        let mut p = [0; 8];
        p[..2].copy_from_slice(&precision.to_le_bytes());
        p[2..4].copy_from_slice(&scale.to_le_bytes());
        p
    };
    for (column_id, ordinal, type_id, parameters) in [
        ([205; 16], 0, [203; 16], varchar_params(5)),
        ([206; 16], 1, [203; 16], varchar_params(20)),
        ([207; 16], 2, [204; 16], decimal_params(5, 2)),
        ([208; 16], 3, [204; 16], decimal_params(10, 4)),
    ] {
        records.push(column(column_id, ordinal, type_id, parameters)?);
    }
    let index_id = [211; 16];
    let index_key = catalog_id_key(9, index_id);
    let mut index_columns = 1u32.to_le_bytes().to_vec();
    index_columns.extend_from_slice(&[208; 16]);
    records.push((
        index_key.clone(),
        catalog_record_payload(
            9,
            &index_key,
            &[
                field(1, WireType::Id, table_id.to_vec()),
                field(2, WireType::Ids, index_columns),
                field(3, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(4, WireType::Bool, vec![1]),
                field(5, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(7, WireType::Id, vec![212; 16]),
                field(8, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(9, WireType::U32, 1u32.to_le_bytes().to_vec()),
                field(10, WireType::U64, 1u64.to_le_bytes().to_vec()),
            ],
        )?,
    ));
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(CATALOG)?;
            for (key, value) in &records {
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
        }
        write.commit()?;
    }
    let mut invocation =
        InvocationTransaction::begin(&database, [209; 16], [9; 16], 9_999, 4, 128)?;
    let schema = invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        invocation.load_catalog_indexes(&schema)?,
        [prdb_engine::transaction::CatalogIndexDefinition {
            index_id,
            table_id,
            column_ids: vec![[208; 16]],
            unique: true,
        }]
    );
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|c| c.scalar_type)
            .collect::<Vec<_>>(),
        vec![
            ScalarType::Varchar { max_chars: 5 },
            ScalarType::Varchar { max_chars: 20 },
            ScalarType::Decimal {
                precision: 5,
                scale: 2
            },
            ScalarType::Decimal {
                precision: 10,
                scale: 4
            },
        ]
    );
    let values = [
        None,
        Some(ScalarValue::Varchar("123456".into())),
        None,
        Some(ScalarValue::Decimal(1_000_000)),
    ];
    assert!(encode_tuple(&schema.columns, schema.schema_version, &values).is_ok());
    let oversized = [None, Some(ScalarValue::Varchar("x".repeat(21))), None, None];
    assert!(matches!(
        encode_tuple(&schema.columns, schema.schema_version, &oversized),
        Err(prdb_engine::tuple::TupleError::VarcharLengthExceeded)
    ));
    let changed_index = catalog_record_payload(
        9,
        &index_key,
        &[
            field(1, WireType::Id, table_id.to_vec()),
            {
                let mut columns = 1u32.to_le_bytes().to_vec();
                columns.extend_from_slice(&[208; 16]);
                field(2, WireType::Ids, columns)
            },
            field(3, WireType::U16, 1u16.to_le_bytes().to_vec()),
            field(4, WireType::Bool, vec![0]),
            field(5, WireType::U16, 1u16.to_le_bytes().to_vec()),
            field(7, WireType::Id, vec![212; 16]),
            field(8, WireType::U16, 1u16.to_le_bytes().to_vec()),
            field(9, WireType::U32, 1u32.to_le_bytes().to_vec()),
            field(10, WireType::U64, 1u64.to_le_bytes().to_vec()),
        ],
    )?;
    {
        let write = database.begin_write()?;
        write
            .open_table(CATALOG)?
            .insert(&index_key[..], &changed_index[..])?;
        write.commit()?;
    }
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));

    let malformed_index_id = [213; 16];
    let malformed_index_key = catalog_id_key(9, malformed_index_id);
    let mut foreign_columns = 1u32.to_le_bytes().to_vec();
    foreign_columns.extend_from_slice(&[255; 16]);
    let malformed_index = catalog_record_payload(
        9,
        &malformed_index_key,
        &[
            field(1, WireType::Id, table_id.to_vec()),
            field(2, WireType::Ids, foreign_columns),
            field(3, WireType::U16, 1u16.to_le_bytes().to_vec()),
            field(4, WireType::Bool, vec![0]),
            field(5, WireType::U16, 1u16.to_le_bytes().to_vec()),
            field(7, WireType::Id, vec![214; 16]),
            field(8, WireType::U16, 1u16.to_le_bytes().to_vec()),
            field(9, WireType::U32, 1u32.to_le_bytes().to_vec()),
            field(10, WireType::U64, 1u64.to_le_bytes().to_vec()),
        ],
    )?;
    {
        let write = database.begin_write()?;
        write
            .open_table(CATALOG)?
            .insert(&malformed_index_key[..], &malformed_index[..])?;
        write.commit()?;
    }
    let mut invalid_index =
        InvocationTransaction::begin(&database, [215; 16], [9; 16], 9_999, 4, 128)?;
    let valid_schema = invalid_index.load_catalog_table_schema(table_id)?;
    assert!(matches!(
        invalid_index.load_catalog_indexes(&valid_schema),
        Err(TransactionError::CorruptCatalog)
    ));

    let (key, value) = column([206; 16], 1, [203; 16], varchar_params(0))?;
    {
        let write = database.begin_write()?;
        {
            write
                .open_table(CATALOG)?
                .insert(key.as_slice(), value.as_slice())?;
        }
        write.commit()?;
    }
    let mut invalid = InvocationTransaction::begin(&database, [210; 16], [9; 16], 9_999, 4, 128)?;
    assert!(matches!(
        invalid.load_catalog_table_schema(table_id),
        Err(TransactionError::CorruptCatalog)
    ));
    Ok(())
}

pub(super) fn catalog_rejects_deleting_expression_used_by_existing_check()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [181; 16];
    let expression_id = [182; 16];
    let table_key = catalog_id_key(5, table_id);
    let table_payload = catalog_record_payload(
        5,
        &table_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: vec![183; 16],
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: vec![184; 16],
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let expression_key = catalog_id_key(7, expression_id);
    let expression_payload = expression_catalog_payload_with_purpose(
        expression_id,
        2,
        &Expr::Literal(Some(ScalarValue::Boolean(true))),
    )?;
    let boolean_type = boolean_type_catalog_write([45; 16])?;
    let boolean_type_object = object_catalog_write_for([45; 16], 2);
    let (constraint_key, constraint_payload) =
        check_constraint_catalog_record([185; 16], table_id, expression_id);
    let expression_dependency = dependency_catalog_write([185; 16], expression_id, 2)?;
    let table_object = object_catalog_write_for(table_id, 3);
    let expression_object = object_catalog_write_for(expression_id, 5);
    let constraint_object = object_catalog_write_for([185; 16], 6);
    let audit = |n| AuditEvent {
        event_id: [n; 16],
        actor_id: Some([9; 16]),
        correlation_id: [n + 1; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![table_id],
    };
    let writes = [
        CatalogWrite {
            key: table_key.clone(),
            expected_value: None,
            payload: Some(table_payload.clone()),
        },
        CatalogWrite {
            key: expression_key.clone(),
            expected_value: None,
            payload: Some(expression_payload.clone()),
        },
        boolean_type.clone(),
        boolean_type_object.clone(),
        CatalogWrite {
            key: constraint_key.clone(),
            expected_value: None,
            payload: Some(constraint_payload.clone()),
        },
        table_object.clone(),
        expression_object.clone(),
        constraint_object.clone(),
        expression_dependency.clone(),
    ];
    commit_catalog_and_audit(&database, [186; 16], [9; 16], 9_999, &writes, &[audit(187)])?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [189; 16],
            [9; 16],
            9_999,
            &[CatalogWrite {
                key: expression_key.clone(),
                expected_value: Some(expression_payload.clone()),
                payload: None
            }],
            &[audit(190)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    let read = database.begin_read()?;
    assert_eq!(
        read.open_table(CATALOG)?
            .get(expression_key.as_slice())?
            .unwrap()
            .value(),
        expression_payload
    );
    assert!(
        read.open_table(CATALOG)?
            .get(constraint_key.as_slice())?
            .is_some()
    );
    assert_eq!(commit_sequence(&database)?, 1);
    drop(read);
    // Simulate persisted corruption: the envelope is valid but names a
    // different expression from the catalog key at which it is stored.
    let wrong_expression = expression_catalog_payload_with_purpose(
        [229; 16],
        2,
        &Expr::Literal(Some(ScalarValue::Boolean(true))),
    )?;
    {
        let write = database.begin_write()?;
        {
            write
                .open_table(CATALOG)?
                .insert(expression_key.as_slice(), wrong_expression.as_slice())?;
        }
        write.commit()?;
    }
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [230; 16],
            [9; 16],
            9_999,
            &[CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_payload.clone()),
                payload: Some(table_payload.clone())
            }],
            &[audit(231)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    {
        let write = database.begin_write()?;
        {
            write
                .open_table(CATALOG)?
                .insert(expression_key.as_slice(), expression_payload.as_slice())?;
        }
        write.commit()?;
    }
    let deletes = [
        CatalogWrite {
            key: table_key,
            expected_value: Some(table_payload),
            payload: None,
        },
        CatalogWrite {
            key: expression_key,
            expected_value: Some(expression_payload),
            payload: None,
        },
        CatalogWrite {
            key: constraint_key,
            expected_value: Some(constraint_payload),
            payload: None,
        },
        CatalogWrite {
            key: table_object.key,
            expected_value: table_object.payload,
            payload: None,
        },
        CatalogWrite {
            key: expression_object.key,
            expected_value: expression_object.payload,
            payload: None,
        },
        CatalogWrite {
            key: constraint_object.key,
            expected_value: constraint_object.payload,
            payload: None,
        },
        CatalogWrite {
            key: expression_dependency.key,
            expected_value: expression_dependency.payload,
            payload: None,
        },
        CatalogWrite {
            key: boolean_type.key,
            expected_value: boolean_type.payload,
            payload: None,
        },
        CatalogWrite {
            key: boolean_type_object.key,
            expected_value: boolean_type_object.payload,
            payload: None,
        },
        CatalogWrite {
            key: catalog_id_key(17, table_id),
            expected_value: None,
            payload: Some(catalog_record_payload(
                17,
                &catalog_id_key(17, table_id),
                &[],
            )?),
        },
        catalog_tombstone_write(expression_id)?,
        catalog_tombstone_write([185; 16])?,
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [192; 16],
            [9; 16],
            9_999,
            &deletes,
            &[audit(193)]
        )?,
        2
    );
    Ok(())
}
