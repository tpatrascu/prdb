use prdb_engine::authorization::{
    AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation, Policy,
    SanitizedCell, Subject,
};
use prdb_engine::catalog::{CatalogField, WireType};
use prdb_engine::commit::{CommitError, RowWrite, commit_rows};
use prdb_engine::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use prdb_engine::index_key::compose_key;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::schema::{CATALOG, ENGINE_META, ROWS};
use prdb_engine::transaction::{InvocationTransaction, TransactionError};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::ReadableDatabase;
use std::time::Duration;

use super::helpers::*;

pub(super) fn authorized_composite_integer_varchar_unique_probe_matches_reference()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::AccessTarget;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [0x31; 16];
    let column_id = [0x67; 16];
    let varchar_column_id = [0x68; 16];
    let uuid_column_id = [0x69; 16];
    let date_column_id = [0x6a; 16];
    let boolean_column_id = [0x6b; 16];
    let index_id = [0x57; 16];
    let varchar_index_id = [0x58; 16];
    let uuid_index_id = [0x59; 16];
    let date_index_id = [0x5a; 16];
    let boolean_index_id = [0x5b; 16];
    let schema_id = [145; 16];
    let type_id = [0x41; 16];
    let varchar_type_id = [0x42; 16];
    let uuid_type_id = [0x43; 16];
    let date_type_id = [0x44; 16];
    let boolean_type_id = [0x45; 16];
    let index_key = catalog_id_key(9, index_id);
    let mut indexed_columns = 2u32.to_le_bytes().to_vec();
    indexed_columns.extend_from_slice(&column_id);
    indexed_columns.extend_from_slice(&varchar_column_id);
    let index_value = catalog_record_payload(
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
                payload: indexed_columns,
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bool,
                payload: vec![1],
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
    let (varchar_index_key, varchar_index_value) = index_catalog_record_with_constraint(
        varchar_index_id,
        table_id,
        varchar_column_id,
        1,
        true,
        None,
    )?;
    let (uuid_index_key, uuid_index_value) = index_catalog_record_with_constraint(
        uuid_index_id,
        table_id,
        uuid_column_id,
        1,
        true,
        None,
    )?;
    let (date_index_key, date_index_value) = index_catalog_record_with_constraint(
        date_index_id,
        table_id,
        date_column_id,
        1,
        true,
        None,
    )?;
    let (boolean_index_key, boolean_index_value) = index_catalog_record_with_constraint(
        boolean_index_id,
        table_id,
        boolean_column_id,
        1,
        true,
        None,
    )?;
    let table_key = catalog_id_key(5, table_id);
    let table_value = catalog_record_payload(
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
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let column_key = catalog_id_key(6, column_id);
    let column_value = catalog_record_payload(
        6,
        &column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 0u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: b"integer_key".to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: type_id.to_vec(),
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
    let varchar_column_key = catalog_id_key(6, varchar_column_id);
    let mut varchar_parameters = [0; 8];
    varchar_parameters[4..].copy_from_slice(&64u32.to_le_bytes());
    let varchar_column_value = catalog_record_payload(
        6,
        &varchar_column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: b"varchar_key".to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: varchar_type_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: varchar_parameters.to_vec(),
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
    let uuid_column_key = catalog_id_key(6, uuid_column_id);
    let uuid_column_value = catalog_record_payload(
        6,
        &uuid_column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 2u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: b"uuid_key".to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: uuid_type_id.to_vec(),
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
    let date_column_key = catalog_id_key(6, date_column_id);
    let date_column_value = catalog_record_payload(
        6,
        &date_column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 3u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: b"date_key".to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: date_type_id.to_vec(),
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
    let boolean_column_key = catalog_id_key(6, boolean_column_id);
    let boolean_column_value = catalog_record_payload(
        6,
        &boolean_column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 4u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: b"boolean_key".to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: boolean_type_id.to_vec(),
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
    let boolean_type = boolean_type_catalog_write(boolean_type_id)?;
    let type_key = catalog_id_key(4, type_id);
    let type_value = catalog_record_payload(
        4,
        &type_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 3u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let varchar_type_key = catalog_id_key(4, varchar_type_id);
    let varchar_type_value = catalog_record_payload(
        4,
        &varchar_type_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 7u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 2u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let uuid_type_key = catalog_id_key(4, uuid_type_id);
    let uuid_type_value = catalog_record_payload(
        4,
        &uuid_type_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 9u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U32,
                payload: 7u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let date_type_key = catalog_id_key(4, date_type_id);
    let date_type_value = catalog_record_payload(
        4,
        &date_type_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 10u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let (table_object_key, table_object_value) =
        object_catalog_record_with_name(table_id, 3, "integer_probe_table");
    let (column_object_key, column_object_value) =
        object_catalog_record_with_name(column_id, 4, "integer_probe_column");
    let (varchar_object_key, varchar_object_value) =
        object_catalog_record_with_name(varchar_column_id, 4, "varchar_probe_column");
    let (uuid_object_key, uuid_object_value) =
        object_catalog_record_with_name(uuid_column_id, 4, "uuid_probe_column");
    let (date_object_key, date_object_value) =
        object_catalog_record_with_name(date_column_id, 4, "date_probe_column");
    let (boolean_object_key, boolean_object_value) =
        object_catalog_record_with_name(boolean_column_id, 4, "boolean_probe_column");
    {
        let write = database.begin_write()?;
        let mut catalog = write.open_table(CATALOG)?;
        for (key, value) in [
            (table_key, table_value),
            (column_key, column_value),
            (varchar_column_key, varchar_column_value),
            (uuid_column_key, uuid_column_value),
            (date_column_key, date_column_value),
            (boolean_column_key, boolean_column_value),
            (type_key, type_value),
            (varchar_type_key, varchar_type_value),
            (uuid_type_key, uuid_type_value),
            (date_type_key, date_type_value),
            (
                boolean_type.key.clone(),
                boolean_type
                    .payload
                    .clone()
                    .ok_or("missing boolean type payload")?,
            ),
            (index_key, index_value),
            (varchar_index_key, varchar_index_value),
            (uuid_index_key, uuid_index_value),
            (date_index_key, date_index_value),
            (boolean_index_key, boolean_index_value),
            (table_object_key, table_object_value),
            (column_object_key, column_object_value),
            (varchar_object_key, varchar_object_value),
            (uuid_object_key, uuid_object_value),
            (date_object_key, date_object_value),
            (boolean_object_key, boolean_object_value),
        ] {
            catalog.insert(key.as_slice(), value.as_slice())?;
        }
        drop(catalog);
        write.commit()?;
    }

    let mut seed = InvocationTransaction::begin(&database, [0x32; 16], [9; 16], 9_999, 4, 4096)?;
    let schema = seed
        .load_catalog_table_schema(table_id)
        .map_err(|error| format!("load composite-prefix table schema: {error:?}"))?;
    seed.load_catalog_indexes(&schema)
        .map_err(|error| format!("load composite-prefix index: {error:?}"))?;
    let target_payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[
            Some(ScalarValue::Integer(42)),
            Some(ScalarValue::Varchar("match".to_owned())),
            Some(ScalarValue::Uuid([0xa1; 16])),
            Some(ScalarValue::Date(20_000)),
            Some(ScalarValue::Boolean(true)),
        ],
    )?;
    let other_payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[
            Some(ScalarValue::Integer(7)),
            Some(ScalarValue::Varchar("other".to_owned())),
            Some(ScalarValue::Uuid([0xa2; 16])),
            Some(ScalarValue::Date(20_001)),
            None,
        ],
    )?;
    seed.stage_put(
        prdb_engine::overlay::RowId {
            table: table_id,
            row: [0x33; 16],
        },
        &target_payload,
    )?;
    seed.stage_put(
        prdb_engine::overlay::RowId {
            table: table_id,
            row: [0x34; 16],
        },
        &other_payload,
    )?;
    seed.commit()
        .map_err(|error| format!("seed composite-prefix rows: {error:?}"))?;

    let mut invocation =
        InvocationTransaction::begin(&database, [0x35; 16], [9; 16], 9_999, 8, 4096)?;
    let schema = invocation.load_catalog_table_schema(table_id)?;
    let principal_id = [9; 16];
    let context = AuthorizationContext {
        principal_id,
        role_ids: std::collections::HashSet::new(),
        scope: [
            (Operation::Usage, AccessTarget::Schema(schema.schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: varchar_column_id,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: uuid_column_id,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: date_column_id,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: boolean_column_id,
                },
            ),
        ]
        .into_iter()
        .collect(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema.schema_id),
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: varchar_column_id,
            },
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: uuid_column_id,
            },
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: date_column_id,
            },
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: boolean_column_id,
            },
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: varchar_column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: uuid_column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: date_column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: boolean_column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
    ];
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
    let target_cells = [
        SanitizedCell::Value(ScalarValue::Integer(42)),
        SanitizedCell::Value(ScalarValue::Varchar("match".to_owned())),
        SanitizedCell::Value(ScalarValue::Uuid([0xa1; 16])),
        SanitizedCell::Value(ScalarValue::Date(20_000)),
        SanitizedCell::Value(ScalarValue::Boolean(true)),
    ];
    let mut reference_matches = Vec::new();
    let mut cursor = None;
    loop {
        let page = invocation.scan_authorized(table_id, cursor.as_ref(), 8, &projection)?;
        reference_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
            (cells.as_slice() == target_cells).then_some((row_id, cells))
        }));
        let Some(next) = page.continuation else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(reference_matches.len(), 1);
    assert!(matches!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[
                Some(ScalarValue::Integer(42)),
                Some(ScalarValue::Varchar("match".to_owned())),
            ],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id, cells }
            if id == reference_matches[0].0 && cells == reference_matches[0].1
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[
                Some(ScalarValue::Integer(99)),
                Some(ScalarValue::Varchar("match".to_owned())),
            ],
            &projection
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch,
    );

    let varchar_target = SanitizedCell::Value(ScalarValue::Varchar("match".to_owned()));
    let mut varchar_reference_matches = Vec::new();
    let mut varchar_cursor = None;
    loop {
        let page = invocation.scan_authorized(table_id, varchar_cursor.as_ref(), 8, &projection)?;
        varchar_reference_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
            (cells.get(1) == Some(&varchar_target)).then_some((row_id, cells))
        }));
        let Some(next) = page.continuation else {
            break;
        };
        varchar_cursor = Some(next);
    }
    assert_eq!(varchar_reference_matches.len(), 1);
    assert!(matches!(
        invocation.probe_catalog_unique_index(
            table_id,
            varchar_index_id,
            &[Some(ScalarValue::Varchar("match".to_owned()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id, cells }
            if id == varchar_reference_matches[0].0 && cells == varchar_reference_matches[0].1
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            varchar_index_id,
            &[Some(ScalarValue::Varchar("absent".to_owned()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch,
    );

    let uuid_target = SanitizedCell::Value(ScalarValue::Uuid([0xa1; 16]));
    let mut uuid_reference_matches = Vec::new();
    let mut uuid_cursor = None;
    loop {
        let page = invocation.scan_authorized(table_id, uuid_cursor.as_ref(), 8, &projection)?;
        uuid_reference_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
            (cells.get(2) == Some(&uuid_target)).then_some((row_id, cells))
        }));
        let Some(next) = page.continuation else {
            break;
        };
        uuid_cursor = Some(next);
    }
    assert_eq!(uuid_reference_matches.len(), 1);
    assert!(matches!(
        invocation.probe_catalog_unique_index(
            table_id,
            uuid_index_id,
            &[Some(ScalarValue::Uuid([0xa1; 16]))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id, cells }
            if id == uuid_reference_matches[0].0 && cells == uuid_reference_matches[0].1
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            uuid_index_id,
            &[Some(ScalarValue::Uuid([0xaf; 16]))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch,
    );

    let date_target = SanitizedCell::Value(ScalarValue::Date(20_000));
    let mut date_reference_matches = Vec::new();
    let mut date_cursor = None;
    loop {
        let page = invocation.scan_authorized(table_id, date_cursor.as_ref(), 8, &projection)?;
        date_reference_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
            (cells.get(3) == Some(&date_target)).then_some((row_id, cells))
        }));
        let Some(next) = page.continuation else {
            break;
        };
        date_cursor = Some(next);
    }
    assert_eq!(date_reference_matches.len(), 1);
    assert!(matches!(
        invocation.probe_catalog_unique_index(
            table_id,
            date_index_id,
            &[Some(ScalarValue::Date(20_000))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id, cells }
            if id == date_reference_matches[0].0 && cells == date_reference_matches[0].1
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            date_index_id,
            &[Some(ScalarValue::Date(20_010))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch,
    );

    let boolean_target = SanitizedCell::Value(ScalarValue::Boolean(true));
    let mut boolean_reference_matches = Vec::new();
    let mut boolean_cursor = None;
    loop {
        let page = invocation.scan_authorized(table_id, boolean_cursor.as_ref(), 8, &projection)?;
        boolean_reference_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
            (cells.get(4) == Some(&boolean_target)).then_some((row_id, cells))
        }));
        let Some(next) = page.continuation else {
            break;
        };
        boolean_cursor = Some(next);
    }
    assert_eq!(boolean_reference_matches.len(), 1);
    assert!(matches!(
        invocation.probe_catalog_unique_index(
            table_id,
            boolean_index_id,
            &[Some(ScalarValue::Boolean(true))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id, cells }
            if id == boolean_reference_matches[0].0 && cells == boolean_reference_matches[0].1
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            boolean_index_id,
            &[Some(ScalarValue::Boolean(false))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch,
    );

    let mut mismatched_invocation =
        InvocationTransaction::begin(&database, [0x36; 16], [9; 16], 9_999, 8, 4096)?;
    let mismatch_schema = mismatched_invocation.load_catalog_table_schema(table_id)?;
    let mismatch_projection = AuthorizedProjection {
        columns: &mismatch_schema.columns,
        column_ids: &mismatch_schema.column_ids,
        schema_version: mismatch_schema.schema_version,
        schema_id: mismatch_schema.schema_id,
        ..projection
    };
    let integer_mismatch_result = mismatched_invocation.probe_catalog_unique_index(
        table_id,
        index_id,
        &[
            Some(ScalarValue::Text("42".to_owned())),
            Some(ScalarValue::Varchar("match".to_owned())),
        ],
        &mismatch_projection,
    );
    assert!(
        matches!(
            &integer_mismatch_result,
            Err(TransactionError::Tuple(
                prdb_engine::tuple::TupleError::TypeMismatch
            ))
        ),
        "unexpected INTEGER key mismatch result: {integer_mismatch_result:?}"
    );
    let mut varchar_mismatched_invocation =
        InvocationTransaction::begin(&database, [0x37; 16], [9; 16], 9_999, 8, 4096)?;
    let varchar_mismatch_schema =
        varchar_mismatched_invocation.load_catalog_table_schema(table_id)?;
    let varchar_mismatch_projection = AuthorizedProjection {
        columns: &varchar_mismatch_schema.columns,
        column_ids: &varchar_mismatch_schema.column_ids,
        schema_version: varchar_mismatch_schema.schema_version,
        schema_id: varchar_mismatch_schema.schema_id,
        ..projection
    };
    let varchar_mismatch_result = varchar_mismatched_invocation.probe_catalog_unique_index(
        table_id,
        varchar_index_id,
        &[Some(ScalarValue::Text("match".to_owned()))],
        &varchar_mismatch_projection,
    );
    assert!(
        matches!(
            &varchar_mismatch_result,
            Err(TransactionError::Tuple(
                prdb_engine::tuple::TupleError::TypeMismatch
            ))
        ),
        "VARCHAR unique probes must reject TEXT without coercion: {varchar_mismatch_result:?}"
    );
    Ok(())
}

pub(super) fn catalog_foreign_keys_check_child_and_parent_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x10; 16];
    let source_column_id = [0x66; 16];
    let target_table_id = [0x21; 16];
    let target_column_id = [0x22; 16];
    let target_constraint_id = [0x23; 16];
    let target_index_id = [0x24; 16];
    let foreign_key_id = [0x25; 16];
    {
        let target_table_key = catalog_id_key(5, target_table_id);
        let target_table = catalog_record_payload(
            5,
            &target_table_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: source_table_id.to_vec(),
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
                    payload: target_table_id.to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )?;
        let target_column_key = catalog_id_key(6, target_column_id);
        let target_column = catalog_record_payload(
            6,
            &target_column_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: target_table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U32,
                    payload: 0u32.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: format!("object_{}", target_column_id[0]).into_bytes(),
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
                    payload: vec![0],
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
        let (target_constraint_key, target_constraint) = unique_constraint_catalog_record(
            target_constraint_id,
            target_table_id,
            target_column_id,
            target_index_id,
        )?;
        let (target_index_key, target_index) = index_catalog_record_with_constraint(
            target_index_id,
            target_table_id,
            target_column_id,
            1,
            true,
            Some(target_constraint_id),
        )?;
        let foreign_key_key = catalog_id_key(8, foreign_key_id);
        let foreign_key_columns = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let foreign_key = catalog_record_payload(
            8,
            &foreign_key_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: source_table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U16,
                    payload: 5u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Ids,
                    payload: foreign_key_columns(&[source_column_id]),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Id,
                    payload: target_constraint_id.to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Ids,
                    payload: foreign_key_columns(&[target_column_id]),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 8,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 9,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 10,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )?;
        let target_objects = [
            object_catalog_record_for(target_table_id, 3),
            object_catalog_record_for(target_column_id, 4),
            object_catalog_record_for(target_constraint_id, 6),
            object_catalog_record_for(target_index_id, 7),
            object_catalog_record_for(foreign_key_id, 6),
        ];
        let target_index_edge = dependency_catalog_write(target_constraint_id, target_index_id, 4)?;
        let foreign_key_edge = dependency_catalog_write(foreign_key_id, target_constraint_id, 3)?;
        let write = database.begin_write()?;
        let mut catalog = write.open_table(CATALOG)?;
        for (key, value) in [
            (target_table_key, target_table),
            (target_column_key, target_column),
            (target_constraint_key, target_constraint),
            (target_index_key, target_index),
            (foreign_key_key, foreign_key),
        ]
        .into_iter()
        .chain(target_objects)
        .chain([
            (target_index_edge.key, target_index_edge.payload.unwrap()),
            (foreign_key_edge.key, foreign_key_edge.payload.unwrap()),
        ]) {
            catalog.insert(key.as_slice(), value.as_slice())?;
        }
        drop(catalog);
        write.commit()?;
    }

    let parent_row_id = prdb_engine::overlay::RowId {
        table: target_table_id,
        row: [1; 16],
    };
    let parent_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
    )?;
    let mut parent = InvocationTransaction::begin(&database, [31; 16], [9; 16], 9_999, 8, 512)?;
    let parent_schema = parent.load_catalog_table_schema(target_table_id)?;
    parent.load_catalog_check_constraints(&parent_schema)?;
    parent.load_catalog_indexes(&parent_schema)?;
    parent.stage_put(parent_row_id, &parent_payload)?;
    assert_eq!(parent.commit()?, 1);

    let child_row_id = id(32);
    let missing_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"missing".to_vec()))],
    )?;
    let mut child = InvocationTransaction::begin(&database, [32; 16], [9; 16], 9_999, 8, 512)?;
    let child_schema = child.load_catalog_table_schema(source_table_id)?;
    child.load_catalog_check_constraints(&child_schema)?;
    child.load_catalog_indexes(&child_schema)?;
    child.stage_put(child_row_id, &missing_payload)?;
    let first_foreign_key_error = child
        .commit()
        .expect_err("a child reference without a parent must fail commit");
    assert!(matches!(
        &first_foreign_key_error,
        TransactionError::Commit(CommitError::ForeignKeyViolation)
    ));

    let alternate_child_row_id = id(37);
    let alternate_missing_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"another missing key".to_vec()))],
    )?;
    let mut alternate_child =
        InvocationTransaction::begin(&database, [0x39; 16], [9; 16], 9_999, 8, 512)?;
    let alternate_child_schema = alternate_child.load_catalog_table_schema(source_table_id)?;
    alternate_child.load_catalog_check_constraints(&alternate_child_schema)?;
    alternate_child.load_catalog_indexes(&alternate_child_schema)?;
    alternate_child.stage_put(alternate_child_row_id, &alternate_missing_payload)?;
    let alternate_foreign_key_error = alternate_child
        .commit()
        .expect_err("a different missing child key must fail commit");
    assert_eq!(
        format!("{first_foreign_key_error:?}"),
        format!("{alternate_foreign_key_error:?}"),
        "foreign-key diagnostics must not include rejected values or row identities"
    );
    assert!(matches!(
        alternate_foreign_key_error,
        TransactionError::Commit(CommitError::ForeignKeyViolation)
    ));
    {
        let read = database.begin_read()?;
        assert!(
            read.open_table(ROWS)?
                .get(&key(child_row_id)[..])?
                .is_none()
        );
    }

    let valid_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
    )?;
    let mut child = InvocationTransaction::begin(&database, [33; 16], [9; 16], 9_999, 8, 512)?;
    let child_schema = child.load_catalog_table_schema(source_table_id)?;
    child.load_catalog_check_constraints(&child_schema)?;
    child.load_catalog_indexes(&child_schema)?;
    child.stage_put(child_row_id, &valid_payload)?;
    assert_eq!(child.commit()?, 2);

    let null_child_row_id = id(36);
    let null_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[None],
    )?;
    let mut null_child = InvocationTransaction::begin(&database, [36; 16], [9; 16], 9_999, 8, 512)?;
    let child_schema = null_child.load_catalog_table_schema(source_table_id)?;
    null_child.load_catalog_check_constraints(&child_schema)?;
    null_child.load_catalog_indexes(&child_schema)?;
    null_child.stage_put(null_child_row_id, &null_payload)?;
    assert_eq!(null_child.commit()?, 3);

    let mut delete_parent =
        InvocationTransaction::begin(&database, [34; 16], [9; 16], 9_999, 8, 512)?;
    let parent_schema = delete_parent.load_catalog_table_schema(target_table_id)?;
    delete_parent.load_catalog_check_constraints(&parent_schema)?;
    delete_parent.load_catalog_indexes(&parent_schema)?;
    delete_parent.stage_delete(parent_row_id)?;
    assert!(matches!(
        delete_parent.commit(),
        Err(TransactionError::Commit(CommitError::ForeignKeyViolation))
    ));
    {
        let read = database.begin_read()?;
        assert!(
            read.open_table(ROWS)?
                .get(&key(parent_row_id)[..])?
                .is_some()
        );
    }

    let mut delete_both =
        InvocationTransaction::begin(&database, [35; 16], [9; 16], 9_999, 8, 512)?;
    let child_schema = delete_both.load_catalog_table_schema(source_table_id)?;
    delete_both.load_catalog_check_constraints(&child_schema)?;
    delete_both.load_catalog_indexes(&child_schema)?;
    let parent_schema = delete_both.load_catalog_table_schema(target_table_id)?;
    delete_both.load_catalog_check_constraints(&parent_schema)?;
    delete_both.load_catalog_indexes(&parent_schema)?;
    delete_both.stage_delete(child_row_id)?;
    delete_both.stage_delete(null_child_row_id)?;
    delete_both.stage_delete(parent_row_id)?;
    assert_eq!(delete_both.commit()?, 4);
    let read = database.begin_read()?;
    assert!(
        read.open_table(ROWS)?
            .get(&key(child_row_id)[..])?
            .is_none()
    );
    assert!(
        read.open_table(ROWS)?
            .get(&key(parent_row_id)[..])?
            .is_none()
    );
    assert!(
        read.open_table(ROWS)?
            .get(&key(null_child_row_id)[..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn catalog_schema_validates_rows_without_secondary_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    {
        let write = database.begin_write()?;
        let mut catalog = write.open_table(CATALOG)?;
        for index_id in [[0x55; 16], [1; 16], [2; 16]] {
            let index_key = catalog_id_key(9, index_id);
            catalog.remove(index_key.as_slice())?;
        }
        drop(catalog);
        write.commit()?;
    }

    let row_id = id(50);
    let mut invocation = InvocationTransaction::begin(&database, [50; 16], [9; 16], 9_999, 1, 128)?;
    let schema = invocation.load_catalog_table_schema(row_id.table)?;
    assert!(invocation.load_catalog_indexes(&schema)?.is_empty());
    invocation.stage_put(row_id, b"not a TUP1 tuple")?;
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::InvalidRow))
    ));

    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(row_id)[..])?.is_none());
    let outcome = prdb_engine::outcome::lookup_outcome(&database, [50; 16], [9; 16], 1)?;
    assert_eq!(outcome.unwrap().status, OutcomeStatus::Aborted);
    Ok(())
}

pub(super) fn failed_commit_validation_records_aborted_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let size = prdb_engine::row_record::MAX_ROW_RECORD_BYTES;
    let mut invocation =
        InvocationTransaction::begin(&database, [43; 16], [9; 16], 9_999, 2, size)?;
    invocation.stage_put(id(43), &vec![0; size])?;
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::InvalidWrite))
    ));
    let outcome = prdb_engine::outcome::lookup_outcome(&database, [43; 16], [9; 16], 1)?;
    assert_eq!(outcome.unwrap().status, OutcomeStatus::Aborted);
    Ok(())
}

pub(super) fn fenced_invocation_commit_waits_for_ddl_and_aborts_on_timeout()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let ddl_fences = DdlFenceManager::new();
    let ddl_guard = ddl_fences.acquire_exclusive(&[[0x10; 16]], Duration::ZERO)?;
    let blocked_coordinator =
        DdlFenceCoordinator::new(ddl_fences.clone(), Duration::from_millis(10));
    let mut blocked = InvocationTransaction::begin_with_fences(
        &database,
        blocked_coordinator,
        [145; 16],
        [9; 16],
        9_999,
        2,
        64,
    )?;
    blocked.stage_put(id(1), b"blocked")?;
    assert!(matches!(
        blocked.commit(),
        Err(TransactionError::Commit(CommitError::DdlFenceUnavailable))
    ));
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [145; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let blocked_key = {
        let mut key = [0; 32];
        key[..16].copy_from_slice(&id(1).table);
        key[16..].copy_from_slice(&id(1).row);
        key
    };
    assert!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&blocked_key[..])?
            .is_none()
    );

    drop(ddl_guard);
    let admitted_coordinator = DdlFenceCoordinator::new(ddl_fences, Duration::from_millis(100));
    let mut admitted = InvocationTransaction::begin_with_fences(
        &database,
        admitted_coordinator,
        [146; 16],
        [9; 16],
        9_999,
        2,
        64,
    )?;
    admitted.stage_put(id(2), b"after ddl")?;
    admitted.commit()?;
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn replacing_new_index_membership_discards_the_intermediate_key()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(44);
    let old_key = compose_key([1; 16], &[1, 7], row_id.row);
    let new_key = compose_key([1; 16], &[1, 8], row_id.row);
    let mut invocation = InvocationTransaction::begin(&database, [44; 16], [9; 16], 9_999, 2, 40)?;
    invocation.stage_put(row_id, b"initial")?;
    invocation.stage_index_membership(row_id, old_key.clone(), true)?;
    invocation.stage_put(row_id, b"updated")?;
    invocation.stage_index_membership(row_id, old_key.clone(), false)?;
    invocation.stage_index_membership(row_id, new_key.clone(), true)?;
    invocation.commit()?;
    let read = database.begin_read()?;
    let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
    assert!(indexes.get(old_key.as_slice())?.is_none());
    assert!(indexes.get(new_key.as_slice())?.is_some());
    Ok(())
}

pub(super) fn missing_index_removal_aborts_with_a_durable_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(40);
    let mut invocation = InvocationTransaction::begin(&database, [40; 16], [9; 16], 9_999, 2, 64)?;
    invocation.stage_put(row_id, b"staged")?;
    let missing = compose_key([1; 16], &[1, 7], row_id.row);
    assert!(matches!(
        invocation.stage_index_membership(row_id, missing, false),
        Err(TransactionError::Commit(CommitError::InvalidIndex))
    ));
    let outcome = prdb_engine::outcome::lookup_outcome(&database, [40; 16], [9; 16], 1)?;
    assert_eq!(outcome.unwrap().status, OutcomeStatus::Aborted);
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    Ok(())
}

pub(super) fn deleting_new_row_releases_index_overlay_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let mut invocation = InvocationTransaction::begin(&database, [41; 16], [9; 16], 9_999, 2, 40)?;
    let first = id(41);
    invocation.stage_put(first, b"a")?;
    invocation.stage_index_membership(first, compose_key([1; 16], &[1, 7], first.row), true)?;
    invocation.stage_delete(first)?;
    let second = id(42);
    invocation.stage_put(second, b"b")?;
    invocation.stage_index_membership(second, compose_key([1; 16], &[1, 8], second.row), true)?;
    invocation.commit()?;
    Ok(())
}

pub(super) fn index_overlay_budget_aborts_the_entire_invocation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(8);
    let mut invocation = InvocationTransaction::begin(&database, [8; 16], [9; 16], 9_999, 2, 40)?;
    invocation.stage_put(row_id, b"staged")?;
    let first = compose_key([1; 16], &[1, 7], row_id.row);
    let second = compose_key([2; 16], &[1, 8], row_id.row);
    invocation.stage_index_membership(row_id, first, true)?;
    assert!(matches!(
        invocation.stage_index_membership(row_id, second, true),
        Err(TransactionError::IndexOverlayLimitExceeded)
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(row_id)[..])?.is_none());
    let outcome = read
        .open_table(prdb_engine::schema::OUTCOMES)?
        .get(&[8; 16][..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        InvocationOutcome::decode(&outcome)?.status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn unsupported_database_versions_reject_transactions_and_commits()
-> Result<(), Box<dyn std::error::Error>> {
    for versions in [(1, 1, 1), (2, 2, 1), (2, 1, 2)] {
        let fixture = Fixture::new();
        let database = initialize(&fixture.path())?;
        let mut pending = InvocationTransaction::begin(&database, [32; 16], [9; 16], 9_999, 2, 64)?;
        pending.stage_put(id(35), b"pending")?;
        let mut pending_abort =
            InvocationTransaction::begin(&database, [31; 16], [9; 16], 9_999, 2, 64)?;
        let metadata = EngineMetadata {
            database_id: [0x33; 16],
            catalog_version: versions.0,
            expression_version: versions.1,
            builtin_semantics_version: versions.2,
            commit_sequence: 0,
            lifecycle: Lifecycle::Ready,
        }
        .encode()?;
        {
            let write = database.begin_write()?;
            {
                write
                    .open_table(ENGINE_META)?
                    .insert(&b"format"[..], &metadata[..])?;
            }
            write.commit()?;
        }
        assert!(matches!(
            InvocationTransaction::begin(&database, [33; 16], [9; 16], 9_999, 2, 64),
            Err(TransactionError::UnsupportedFormat)
        ));
        assert!(matches!(
            pending.commit(),
            Err(TransactionError::Commit(CommitError::UnsupportedFormat))
        ));
        assert!(matches!(
            pending_abort.abort(),
            Err(TransactionError::Commit(CommitError::UnsupportedFormat))
        ));
        assert_eq!(
            commit_rows(
                &database,
                [34; 16],
                [9; 16],
                9_999,
                &[RowWrite {
                    key: key(id(35)),
                    expected_version: None,
                    payload: Some(b"no".to_vec())
                }]
            ),
            Err(CommitError::UnsupportedFormat)
        );
        let read = database.begin_read()?;
        assert!(read.open_table(ROWS)?.get(&key(id(35))[..])?.is_none());
        assert!(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[34; 16][..])?
                .is_none()
        );
        assert!(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[32; 16][..])?
                .is_none()
        );
        assert!(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[31; 16][..])?
                .is_none()
        );
        assert_eq!(commit_sequence(&database)?, 0);
    }
    Ok(())
}

pub(super) fn deleting_same_snapshot_row_twice_aborts_invocation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [36; 16], [9; 16], 9_999, 2, 64)?;
    invocation.stage_delete(id(1))?;
    assert_eq!(invocation.read(id(1))?, None);
    assert!(matches!(
        invocation.stage_delete(id(1)),
        Err(TransactionError::NotFound)
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(1))[..])?.is_some());
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [36; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn stale_invocation_aborts_and_insert_then_delete_commits_as_noop()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;

    let mut stale = InvocationTransaction::begin(&database, [3; 16], [9; 16], 9_999, 4, 64)?;
    stale.stage_put(id(1), b"stale")?;
    commit_rows(
        &database,
        [2; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(1)),
            expected_version: Some(1),
            payload: Some(b"winner".to_vec()),
        }],
    )?;
    assert!(matches!(
        stale.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));

    let mut delete = InvocationTransaction::begin(&database, [5; 16], [9; 16], 9_999, 2, 16)?;
    assert_eq!(delete.read(id(1))?, Some(b"winner".to_vec()));
    delete.stage_delete(id(1))?;
    assert_eq!(delete.read(id(1))?, None);
    assert_eq!(delete.commit()?, 3);

    let mut no_op = InvocationTransaction::begin(&database, [4; 16], [9; 16], 9_999, 2, 16)?;
    no_op.stage_put(id(4), b"transient")?;
    assert_eq!(no_op.read(id(4))?, Some(b"transient".to_vec()));
    no_op.stage_delete(id(4))?;
    assert_eq!(no_op.read(id(4))?, None);
    assert_eq!(no_op.commit()?, 4);
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(4))[..])?.is_none());
    assert!(read.open_table(ROWS)?.get(&key(id(1))[..])?.is_none());
    Ok(())
}
