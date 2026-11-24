use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::authorization::{
    AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Effect, Expr,
    Grant, Operation, Policy, SanitizedCell, Subject,
};
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::commit::{
    CatalogWrite, CommitError, RowWrite, commit_catalog_and_audit, commit_rows,
};
use prdb_engine::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use prdb_engine::expression::encode_expression;
use prdb_engine::index_key::{compose_key, derive_index_key};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ENGINE_META, PRINCIPAL_NAMES, ROWS};
use prdb_engine::transaction::{InsertInput, InvocationTransaction, TransactionError};
use prdb_engine::tuple::{
    ColumnSchema, FieldPatch, ScalarType, ScalarValue, decode_tuple, encode_tuple,
};
use redb::{Database, ReadableDatabase};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-txn-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("transaction.redb")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn id(row: u8) -> prdb_engine::overlay::RowId {
    prdb_engine::overlay::RowId {
        table: [0x10; 16],
        row: [row; 16],
    }
}

fn builtin_owner_id() -> [u8; 16] {
    builtin_id(3)
}

fn builtin_id(code: u16) -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = code as u8;
    id
}

fn key(id: prdb_engine::overlay::RowId) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&id.table);
    key[16..].copy_from_slice(&id.row);
    key
}

fn numbered_row_id(table: [u8; 16], number: u64) -> prdb_engine::overlay::RowId {
    prdb_engine::overlay::RowId {
        table,
        row: u128::from(number).to_be_bytes(),
    }
}

fn expression_catalog_payload(
    expression_id: [u8; 16],
    expression: &Expr,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    expression_catalog_payload_with_purpose(expression_id, 1, expression)
}

fn expression_catalog_payload_with_purpose(
    expression_id: [u8; 16],
    purpose: u16,
    expression: &Expr,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    expression_catalog_payload_full(expression_id, purpose, [45; 16], [0; 8], &[], expression)
}

fn expression_catalog_payload_full(
    expression_id: [u8; 16],
    purpose: u16,
    result_type_id: [u8; 16],
    result_parameters: [u8; 8],
    dependencies: &[[u8; 16]],
    expression: &Expr,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut expression_key = 7u16.to_be_bytes().to_vec();
    expression_key.push(1);
    expression_key.extend_from_slice(&expression_id);
    let mut dependency_payload = (dependencies.len() as u32).to_le_bytes().to_vec();
    dependency_payload.extend(dependencies.iter().flatten());
    Ok(encode_envelope(
        &family_schema(7).unwrap(),
        &expression_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: purpose.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Id,
                payload: result_type_id.to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bytes,
                payload: result_parameters.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: encode_expression(expression)?,
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: dependency_payload,
            },
        ],
    )?)
}

fn catalog_id_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

fn catalog_tombstone_write(
    object_id: [u8; 16],
) -> Result<CatalogWrite, Box<dyn std::error::Error>> {
    let key = catalog_id_key(17, object_id);
    Ok(CatalogWrite {
        payload: Some(catalog_record_payload(17, &key, &[])?),
        key,
        expected_value: None,
    })
}

fn dependency_catalog_write(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    kind: u16,
) -> Result<CatalogWrite, Box<dyn std::error::Error>> {
    let mut key = 15u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&dependent_id);
    key.push(1);
    key.extend_from_slice(&referenced_id);
    key.push(1);
    key.extend_from_slice(&kind.to_be_bytes());
    Ok(CatalogWrite {
        payload: Some(catalog_record_payload(15, &key, &[])?),
        key,
        expected_value: None,
    })
}

fn catalog_record_payload(
    family: u16,
    key: &[u8],
    fields: &[CatalogField],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(encode_envelope(
        &family_schema(family).unwrap(),
        key,
        fields,
    )?)
}

fn index_catalog_record(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_id: [u8; 16],
    schema_version: u64,
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    index_catalog_record_with_constraint(index_id, table_id, column_id, schema_version, false, None)
}

fn index_catalog_record_with_constraint(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_id: [u8; 16],
    schema_version: u64,
    unique: bool,
    owning_constraint_id: Option<[u8; 16]>,
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    let key = catalog_id_key(9, index_id);
    let mut columns = 1u32.to_le_bytes().to_vec();
    columns.extend_from_slice(&column_id);
    let mut fields = vec![
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: table_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Ids,
            payload: columns,
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bool,
            payload: vec![u8::from(unique)],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
    ];
    if let Some(owning_constraint_id) = owning_constraint_id {
        fields.push(CatalogField {
            tag: 6,
            wire_type: WireType::Id,
            payload: owning_constraint_id.to_vec(),
        });
    }
    fields.extend([
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
            payload: schema_version.to_le_bytes().to_vec(),
        },
    ]);
    let payload = catalog_record_payload(9, &key, &fields)?;
    Ok((key, payload))
}

fn unique_constraint_catalog_record(
    constraint_id: [u8; 16],
    table_id: [u8; 16],
    column_id: [u8; 16],
    backing_index_id: [u8; 16],
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    let key = catalog_id_key(8, constraint_id);
    let mut source_columns = 1u32.to_le_bytes().to_vec();
    source_columns.extend_from_slice(&column_id);
    let payload = catalog_record_payload(
        8,
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
                payload: 3u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: source_columns,
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
            CatalogField {
                tag: 11,
                wire_type: WireType::Id,
                payload: backing_index_id.to_vec(),
            },
        ],
    )?;
    Ok((key, payload))
}

fn principal_catalog_record(authority_epoch: u64) -> (Vec<u8>, Vec<u8>) {
    principal_catalog_record_for([7; 16], 1, b"user", true, authority_epoch)
}

fn principal_catalog_record_for(
    principal_id: [u8; 16],
    kind: u16,
    _name: &[u8],
    login_enabled: bool,
    authority_epoch: u64,
) -> (Vec<u8>, Vec<u8>) {
    let key = catalog_id_key(10, principal_id);
    let name = principal_test_name(principal_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: kind.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Text,
            payload: name.into_bytes(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Bool,
            payload: vec![u8::from(login_enabled)],
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::U64,
            payload: authority_epoch.to_le_bytes().to_vec(),
        },
    ];
    let schema = family_schema(10).unwrap();
    (
        key.clone(),
        encode_envelope(&schema, &key, &fields).unwrap(),
    )
}

fn principal_object_record_for(principal_id: [u8; 16], principal_kind: u16) -> (Vec<u8>, Vec<u8>) {
    let object_kind: u16 = match principal_kind {
        1 => 8,
        2 => 9,
        _ => panic!("unsupported test principal kind"),
    };
    object_catalog_record_with_name(
        principal_id,
        object_kind,
        &principal_test_name(principal_id),
    )
}

fn object_catalog_record_for(object_id: [u8; 16], object_kind: u16) -> (Vec<u8>, Vec<u8>) {
    object_catalog_record_with_name(object_id, object_kind, &format!("object_{}", object_id[0]))
}

fn principal_test_name(principal_id: [u8; 16]) -> String {
    if principal_id == builtin_owner_id() {
        return "database_admin".to_owned();
    }
    let suffix = principal_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("principal_{suffix}")
}

fn object_catalog_record_with_name(
    object_id: [u8; 16],
    object_kind: u16,
    name: &str,
) -> (Vec<u8>, Vec<u8>) {
    let key = catalog_id_key(2, object_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: object_kind.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: name.as_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: builtin_owner_id().to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
    ];
    let schema = family_schema(2).unwrap();
    (
        key.clone(),
        encode_envelope(&schema, &key, &fields).unwrap(),
    )
}

fn object_catalog_write_for(object_id: [u8; 16], object_kind: u16) -> CatalogWrite {
    let (key, payload) = object_catalog_record_for(object_id, object_kind);
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn boolean_type_catalog_write(
    type_id: [u8; 16],
) -> Result<CatalogWrite, Box<dyn std::error::Error>> {
    let key = catalog_id_key(4, type_id);
    let payload = catalog_record_payload(
        4,
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
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
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

fn membership_catalog_record(
    member_id: [u8; 16],
    role_id: [u8; 16],
    grantor_id: [u8; 16],
    membership_epoch: u64,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 11u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&member_id);
    key.push(1);
    key.extend_from_slice(&role_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: grantor_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U64,
            payload: membership_epoch.to_le_bytes().to_vec(),
        },
    ];
    (
        key.clone(),
        encode_envelope(&family_schema(11).unwrap(), &key, &fields).unwrap(),
    )
}

fn grant_catalog_record(
    grantee_id: [u8; 16],
    object_id: [u8; 16],
    privilege: u16,
    target_scope: u16,
    target_id: [u8; 16],
    grantor_id: [u8; 16],
    authority_epoch: u64,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 12u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&grantee_id);
    key.push(1);
    key.extend_from_slice(&object_id);
    key.push(1);
    key.extend_from_slice(&privilege.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_scope.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: grantor_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U64,
            payload: authority_epoch.to_le_bytes().to_vec(),
        },
    ];
    (
        key.clone(),
        encode_envelope(&family_schema(12).unwrap(), &key, &fields).unwrap(),
    )
}

fn policy_target_catalog_record(
    policy_id: [u8; 16],
    target_kind: u16,
    target_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 14u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&policy_id);
    key.push(1);
    key.extend_from_slice(&target_kind.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_id);
    (
        key.clone(),
        encode_envelope(&family_schema(14).unwrap(), &key, &[]).unwrap(),
    )
}

fn policy_catalog_record(
    policy_id: [u8; 16],
    table_id: [u8; 16],
    kind: u16,
    operation: u16,
    using_expression_id: Option<[u8; 16]>,
    check_expression_id: Option<[u8; 16]>,
) -> (Vec<u8>, Vec<u8>) {
    let key = catalog_id_key(13, policy_id);
    let mut fields = vec![
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: table_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U16,
            payload: kind.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U16,
            payload: operation.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    if let Some(expression_id) = using_expression_id {
        fields.push(CatalogField {
            tag: 5,
            wire_type: WireType::Id,
            payload: expression_id.to_vec(),
        });
    }
    if let Some(expression_id) = check_expression_id {
        fields.push(CatalogField {
            tag: 6,
            wire_type: WireType::Id,
            payload: expression_id.to_vec(),
        });
    }
    fields.sort_by_key(|field| field.tag);
    (
        key.clone(),
        catalog_record_payload(13, &key, &fields).unwrap(),
    )
}

fn check_constraint_catalog_record(
    constraint_id: [u8; 16],
    table_id: [u8; 16],
    expression_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let key = catalog_id_key(8, constraint_id);
    let fields = [
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
            payload: expression_id.to_vec(),
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
    ];
    (
        key.clone(),
        encode_envelope(&family_schema(8).unwrap(), &key, &fields).unwrap(),
    )
}

fn not_null_constraint_catalog_record(
    constraint_id: [u8; 16],
    table_id: [u8; 16],
    column_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let key = catalog_id_key(8, constraint_id);
    let mut source_columns = 1u32.to_le_bytes().to_vec();
    source_columns.extend_from_slice(&column_id);
    let fields = [
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
            wire_type: WireType::Ids,
            payload: source_columns,
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
    ];
    (
        key.clone(),
        encode_envelope(&family_schema(8).unwrap(), &key, &fields).unwrap(),
    )
}

fn initialize(path: &std::path::Path) -> Result<Database, Box<dyn std::error::Error>> {
    let database = Database::create(path)?;
    let mut write = database.begin_write()?;
    write.set_durability(redb::Durability::Immediate)?;
    write.set_two_phase_commit(true);
    schema::create_tables(&write)?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        let table_id = [0x10; 16];
        let column_id = [0x66; 16];
        for index_id in [[0x55; 16], [1; 16], [2; 16]] {
            let key = catalog_id_key(9, index_id);
            let mut columns = 1u32.to_le_bytes().to_vec();
            columns.extend_from_slice(&column_id);
            let fields = [
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::Ids,
                    payload: columns,
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
            ];
            let payload = catalog_record_payload(9, &key, &fields)?;
            catalog.insert(&key[..], &payload[..])?;
        }
        let table_key = catalog_id_key(5, table_id);
        let table_payload = catalog_record_payload(
            5,
            &table_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
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
                    payload: [145; 16].to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )?;
        let column_key = catalog_id_key(6, column_id);
        let column_payload = catalog_record_payload(
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
                    payload: b"object_102".to_vec(),
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
        let (table_object_key, table_object_payload) = object_catalog_record_for(table_id, 3);
        let (column_object_key, column_object_payload) = object_catalog_record_for(column_id, 4);
        for (key, payload) in [
            (table_key, table_payload),
            (column_key, column_payload),
            (table_object_key, table_object_payload),
            (column_object_key, column_object_payload),
        ] {
            catalog.insert(&key[..], &payload[..])?;
        }
        let owner_id = builtin_owner_id();
        let (principal_key, principal_value) =
            principal_catalog_record_for(owner_id, 2, b"database_admin", false, 1);
        let (owner_object_key, owner_object_value) = principal_object_record_for(owner_id, 2);
        catalog.insert(&principal_key[..], &principal_value[..])?;
        catalog.insert(&owner_object_key[..], &owner_object_value[..])?;
        let type_key = catalog_id_key(4, [0x40; 16]);
        let type_payload = catalog_record_payload(
            4,
            &type_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: if std::env::var_os("PRDB_BENCH_INDEX_TEXT").is_some() {
                        6u16.to_le_bytes().to_vec()
                    } else {
                        8u16.to_le_bytes().to_vec()
                    },
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
        catalog.insert(&type_key[..], &type_payload[..])?;
    }
    let metadata = EngineMetadata {
        database_id: [0x33; 16],
        catalog_version: 2,
        expression_version: 1,
        builtin_semantics_version: 1,
        commit_sequence: 0,
        lifecycle: Lifecycle::Ready,
    }
    .encode()?;
    write
        .open_table(ENGINE_META)?
        .insert(&b"format"[..], &metadata[..])?;
    write.commit()?;
    Ok(database)
}

fn install_relation_discovery_catalog(
    database: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_id = [0x91; 16];
    let hidden_schema_id = [0x92; 16];
    let second_visible_schema_id = [0x93; 16];
    let table_id = [0x10; 16];
    let hidden_table_id = [0x20; 16];
    let column_id = [0x66; 16];
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        let mut grants = write.open_table(prdb_engine::schema::GRANTS)?;
        let mut table_ids = 2u32.to_le_bytes().to_vec();
        table_ids.extend_from_slice(&table_id);
        table_ids.extend_from_slice(&hidden_table_id);
        let schema_key = catalog_id_key(3, schema_id);
        let schema_value = catalog_record_payload(
            3,
            &schema_key,
            &[CatalogField {
                tag: 1,
                wire_type: WireType::Ids,
                payload: table_ids,
            }],
        )?;
        let schema_object_key = catalog_id_key(2, schema_id);
        let schema_object_value = catalog_record_payload(
            2,
            &schema_object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: b"app".to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: builtin_owner_id().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )?;
        catalog.insert(&schema_key[..], &schema_value[..])?;
        catalog.insert(&schema_object_key[..], &schema_object_value[..])?;
        for (other_schema_id, name) in [
            (hidden_schema_id, "private"),
            (second_visible_schema_id, "public"),
        ] {
            let other_schema_key = catalog_id_key(3, other_schema_id);
            let other_schema_value = catalog_record_payload(
                3,
                &other_schema_key,
                &[CatalogField {
                    tag: 1,
                    wire_type: WireType::Ids,
                    payload: 0u32.to_le_bytes().to_vec(),
                }],
            )?;
            let (other_object_key, other_object_value) =
                object_catalog_record_with_name(other_schema_id, 1, name);
            catalog.insert(&other_schema_key[..], &other_schema_value[..])?;
            catalog.insert(&other_object_key[..], &other_object_value[..])?;
        }

        for (table_id, name, storage_id) in [
            (table_id, b"metrics".as_slice(), [145; 16]),
            (hidden_table_id, b"secret".as_slice(), [146; 16]),
        ] {
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
                        payload: storage_id.to_vec(),
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::U16,
                        payload: 1u16.to_le_bytes().to_vec(),
                    },
                ],
            )?;
            let object_fields = vec![
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 3u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::Id,
                    payload: schema_id.to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: name.to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: builtin_owner_id().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ];
            let object_key = catalog_id_key(2, table_id);
            let object_value = catalog_record_payload(2, &object_key, &object_fields)?;
            catalog.insert(&table_key[..], &table_value[..])?;
            catalog.insert(&object_key[..], &object_value[..])?;
        }

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
                    payload: b"value".to_vec(),
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
        let column_object_key = catalog_id_key(2, column_id);
        let column_object_value = catalog_record_payload(
            2,
            &column_object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 4u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: b"value".to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: builtin_owner_id().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )?;
        catalog.insert(&column_key[..], &column_value[..])?;
        catalog.insert(&column_object_key[..], &column_object_value[..])?;

        let (user_key, user_value) =
            principal_catalog_record_for([9; 16], 1, b"metadata_user", true, 1);
        let (user_object_key, user_object_value) = principal_object_record_for([9; 16], 1);
        catalog.insert(&user_key[..], &user_value[..])?;
        catalog.insert(&user_object_key[..], &user_object_value[..])?;
        let (restricted_user_key, restricted_user_value) =
            principal_catalog_record_for([10; 16], 1, b"restricted_metadata_user", true, 1);
        let (restricted_object_key, restricted_object_value) =
            principal_object_record_for([10; 16], 1);
        catalog.insert(&restricted_user_key[..], &restricted_user_value[..])?;
        catalog.insert(&restricted_object_key[..], &restricted_object_value[..])?;
        for (object_id, privilege, target_scope, target_id) in [
            (schema_id, 1, 1, schema_id),
            (second_visible_schema_id, 1, 1, second_visible_schema_id),
            (table_id, 3, 1, table_id),
            (table_id, 2, 2, column_id),
            (hidden_table_id, 5, 1, hidden_table_id),
        ] {
            let (grant_key, grant_value) = grant_catalog_record(
                [9; 16],
                object_id,
                privilege,
                target_scope,
                target_id,
                builtin_owner_id(),
                1,
            );
            catalog.insert(&grant_key[..], &grant_value[..])?;
            for (direction, indexed_id) in [(1u8, [9; 16]), (2u8, object_id)] {
                let mut index_key = Vec::with_capacity(76);
                index_key.push(direction);
                index_key.extend_from_slice(&indexed_id);
                index_key.extend_from_slice(&grant_key);
                grants.insert(&index_key[..], &[][..])?;
            }
        }
        let (restricted_grant_key, restricted_grant_value) = grant_catalog_record(
            [10; 16],
            second_visible_schema_id,
            1,
            1,
            second_visible_schema_id,
            builtin_owner_id(),
            1,
        );
        catalog.insert(&restricted_grant_key[..], &restricted_grant_value[..])?;
        for (direction, indexed_id) in [(1u8, [10; 16]), (2u8, second_visible_schema_id)] {
            let mut index_key = Vec::with_capacity(76);
            index_key.push(direction);
            index_key.extend_from_slice(&indexed_id);
            index_key.extend_from_slice(&restricted_grant_key);
            grants.insert(&index_key[..], &[][..])?;
        }
    }
    write.commit()?;
    Ok(())
}

fn install_second_relation_column(database: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        for (column_id, table_id, ordinal, name, nullable) in [
            ([0x67; 16], [0x10; 16], 1u32, b"value2".as_slice(), false),
            (
                [0x68; 16],
                [0x20; 16],
                0u32,
                b"secret_value".as_slice(),
                true,
            ),
        ] {
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
                        payload: vec![u8::from(nullable)],
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
            let object_key = catalog_id_key(2, column_id);
            let object_value = catalog_record_payload(
                2,
                &object_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: 4u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::Id,
                        payload: table_id.to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Text,
                        payload: name.to_vec(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Id,
                        payload: builtin_owner_id().to_vec(),
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::U64,
                        payload: 1u64.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::U16,
                        payload: 1u16.to_le_bytes().to_vec(),
                    },
                ],
            )?;
            catalog.insert(&column_key[..], &column_value[..])?;
            catalog.insert(&object_key[..], &object_value[..])?;
        }
    }
    write.commit()?;
    Ok(())
}

#[test]
fn authorized_relation_listing_uses_schema_usage_and_data_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;

    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Insert, AccessTarget::Table(table_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [180; 16], [9; 16], 9_999, 1, 64)?;
    let page = invocation
        .list_authorized_relations(schema_id, 16, None, &context, &grants, &[])?
        .expect("schema is discoverable");
    assert_eq!(page.relations.len(), 1);
    assert_eq!(page.relations[0].relation_id, table_id);
    assert_eq!(page.relations[0].name, "metrics");
    assert!(page.continuation.is_none());

    let mut malformed_hidden_row_key = [0x20; 32];
    malformed_hidden_row_key[16..].fill(0xfe);
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&malformed_hidden_row_key[..], &b"not-a-row"[..])?;
    write.commit()?;
    let mut populated_hidden_invocation =
        InvocationTransaction::begin(&database, [181; 16], [9; 16], 9_999, 1, 64)?;
    let populated_hidden_page = populated_hidden_invocation
        .list_authorized_relations(schema_id, 16, None, &context, &grants, &[])?
        .expect("schema remains discoverable with hidden row data");
    assert_eq!(
        populated_hidden_page.relations, page.relations,
        "inaccessible relation rows cannot change ordinary metadata discovery"
    );

    let no_usage = AuthorizationContext {
        scope: std::collections::HashSet::from([(
            Operation::Insert,
            AccessTarget::Table(table_id),
        )]),
        ..context
    };
    assert!(
        invocation
            .list_authorized_relations(schema_id, 16, None, &no_usage, &grants, &[])?
            .is_none()
    );
    Ok(())
}

#[test]
fn relation_listing_rejects_grants_not_present_in_the_pinned_catalog()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
        ]),
    };
    let forged_grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [183; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        invocation.list_authorized_relations(schema_id, 16, None, &context, &forged_grants, &[]),
        Err(TransactionError::InvalidContext)
    ));
    Ok(())
}

#[test]
fn schema_listing_includes_the_public_namespace_and_usage_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let app_schema_id = [0x91; 16];
    let public_schema_id = [0x93; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(app_schema_id)),
            (Operation::Usage, AccessTarget::Schema(public_schema_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(app_schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(public_schema_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [185; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.list_authorized_schemas(1, None, &context, &grants, &[])?;
    assert_eq!(first.schemas.len(), 1);
    assert_eq!(first.schemas[0].name, "information_schema");
    let cursor = first.continuation.expect("another visible schema");
    let serialized_cursor = cursor.to_bytes();
    assert!(
        prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&serialized_cursor)
            .is_none()
    );
    assert!(
        prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&serialized_cursor).is_none()
    );
    let cursor = prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&serialized_cursor)
        .expect("cursor wire form is fixed width");
    let mut other_invocation =
        InvocationTransaction::begin(&database, [195; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_invocation.list_authorized_schemas(1, Some(cursor.clone()), &context, &grants, &[],),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let second =
        invocation.list_authorized_schemas(1, Some(cursor.clone()), &context, &grants, &[])?;
    assert_eq!(second.schemas.len(), 1);
    assert_eq!(second.schemas[0].schema_id, app_schema_id);
    assert_eq!(second.schemas[0].name, "app");
    let third = invocation.list_authorized_schemas(
        1,
        second.continuation.clone(),
        &context,
        &grants,
        &[],
    )?;
    assert_eq!(third.schemas.len(), 1);
    assert_eq!(third.schemas[0].schema_id, public_schema_id);
    assert_eq!(third.schemas[0].name, "public");
    assert!(third.continuation.is_none());
    assert!(matches!(
        invocation.list_authorized_schemas(1, Some(cursor), &context, &grants, &[]),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

#[test]
fn schema_cursor_rejects_modified_wire_tokens() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Usage, AccessTarget::Schema([0x93; 16])),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema([0x93; 16]),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [196; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.list_authorized_schemas(1, None, &context, &grants, &[])?;
    let cursor = first.continuation.expect("another visible schema");
    let mut bytes = cursor.to_bytes();
    bytes[1] ^= 1;
    let modified = prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&bytes).unwrap();
    assert!(matches!(
        invocation.list_authorized_schemas(1, Some(modified), &context, &grants, &[]),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&bytes[..15]).is_none());
    Ok(())
}

#[test]
fn authorized_schema_resolution_matches_only_visible_exact_names()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([(
            Operation::Usage,
            AccessTarget::Schema(schema_id),
        )]),
    };
    let grants = [Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(schema_id),
    }];
    let mut invocation = InvocationTransaction::begin(&database, [189; 16], [9; 16], 9_999, 1, 64)?;
    let resolved = invocation
        .resolve_authorized_schema("app", &context, &grants, &[])?
        .expect("USAGE-visible exact name resolves");
    assert_eq!(resolved.schema_id, schema_id);
    assert_eq!(resolved.name, "app");
    assert!(
        invocation
            .resolve_authorized_schema("private", &context, &grants, &[])?
            .is_none()
    );
    assert!(
        invocation
            .resolve_authorized_schema("missing", &context, &grants, &[])?
            .is_none()
    );
    assert!(
        invocation
            .resolve_authorized_schema("App", &context, &grants, &[])?
            .is_none()
    );
    Ok(())
}

#[test]
fn schema_access_loader_uses_pinned_principal_schema_usage_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let app_schema_id = [0x91; 16];
    let public_schema_id = [0x93; 16];
    let mut invocation = InvocationTransaction::begin(&database, [190; 16], [9; 16], 9_999, 1, 64)?;
    let (roles, grants) = invocation.load_catalog_schema_usage_grants([9; 16])?;
    assert!(roles.is_empty());
    assert_eq!(grants.len(), 2);
    assert!(grants.iter().all(|grant| {
        grant.subject == Subject::Principal([9; 16]) && grant.operation == Operation::Usage
    }));
    let mut visible_schemas = grants
        .iter()
        .filter_map(|grant| match grant.target {
            AccessTarget::Schema(schema_id) => Some(schema_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    visible_schemas.sort_unstable();
    assert_eq!(visible_schemas, [app_schema_id, public_schema_id]);
    Ok(())
}

#[test]
fn relation_discovery_grant_loader_filters_to_one_schema() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let mut invocation = InvocationTransaction::begin(&database, [192; 16], [9; 16], 9_999, 1, 64)?;
    let (roles, grants) = invocation.load_catalog_relation_discovery_grants(schema_id)?;
    assert!(roles.is_empty());
    assert_eq!(grants.len(), 4);
    assert!(
        grants
            .iter()
            .all(|grant| grant.subject == Subject::Principal([9; 16]))
    );
    assert!(grants.contains(&Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(schema_id),
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Insert,
        target: AccessTarget::Table([0x10; 16]),
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Select,
        target: AccessTarget::Column {
            table_id: [0x10; 16],
            column_id: [0x66; 16],
        },
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Delete,
        target: AccessTarget::Table([0x20; 16]),
    }));
    Ok(())
}

#[test]
fn catalog_authorized_relation_listing_derives_the_invocation_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let mut invocation = InvocationTransaction::begin(&database, [193; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .list_catalog_authorized_relations(schema_id, 1, None)?
        .expect("schema is visible");
    assert_eq!(first.relations.len(), 1);
    assert_eq!(first.relations[0].relation_id, [0x10; 16]);
    assert_eq!(first.relations[0].name, "metrics");
    let columns = invocation
        .describe_catalog_authorized_columns_page(schema_id, [0x10; 16], 128, None)?
        .expect("table is discoverable through a column SELECT grant");
    assert_eq!(columns.relation_id, [0x10; 16]);
    assert_eq!(columns.columns.len(), 1);
    assert_eq!(columns.columns[0].column_id, [0x66; 16]);
    assert_eq!(columns.columns[0].name, "value");
    assert_eq!(columns.columns[0].ordinal, 0);
    assert_eq!(columns.columns[0].scalar_type, ScalarType::Bytea);
    assert!(columns.columns[0].nullable);
    let continuation = first.continuation.expect("second granted relation follows");
    let second = invocation
        .list_catalog_authorized_relations(schema_id, 1, Some(continuation))?
        .expect("schema remains visible");
    assert_eq!(second.relations.len(), 1);
    assert_eq!(second.relations[0].relation_id, [0x20; 16]);
    assert_eq!(second.relations[0].name, "secret");
    assert!(second.continuation.is_none());
    assert!(
        invocation
            .list_catalog_authorized_relations([0x92; 16], 1, None)?
            .is_none()
    );
    assert!(
        invocation
            .list_catalog_authorized_relations([0x94; 16], 1, None)?
            .is_none()
    );
    Ok(())
}

#[test]
fn catalog_authorized_schema_listing_derives_the_invocation_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [191; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.list_catalog_authorized_schemas(1, None)?;
    assert_eq!(first.schemas.len(), 1);
    assert_eq!(first.schemas[0].name, "information_schema");
    let continuation = first.continuation.expect("public schema follows");
    let second = invocation.list_catalog_authorized_schemas(1, Some(continuation))?;
    assert_eq!(second.schemas.len(), 1);
    assert_eq!(second.schemas[0].schema_id, [0x91; 16]);
    assert_eq!(second.schemas[0].name, "app");
    let third = invocation.list_catalog_authorized_schemas(1, second.continuation)?;
    assert_eq!(third.schemas[0].schema_id, [0x93; 16]);
    assert_eq!(third.schemas[0].name, "public");
    assert!(third.continuation.is_none());
    Ok(())
}

#[test]
fn information_schema_schemata_exposes_public_and_authorized_names()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [199; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.scan_information_schema_schemata(1, None)?;
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].schema_name, "information_schema");
    let continuation = first.continuation.expect("public schema follows");
    let second = invocation.scan_information_schema_schemata(1, Some(continuation))?;
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].schema_name, "app");
    let third = invocation.scan_information_schema_schemata(1, second.continuation)?;
    assert_eq!(third.rows[0].schema_name, "public");
    assert!(third.continuation.is_none());
    Ok(())
}

#[test]
fn information_schema_builtin_relations_have_public_self_descriptions()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [212; 16], [9; 16], 9_999, 1, 64)?;

    let schemata = invocation.scan_information_schema_schemata(1, None)?;
    assert_eq!(schemata.rows.len(), 1);
    assert_eq!(schemata.rows[0].schema_name, "information_schema");

    let tables = invocation
        .scan_information_schema_tables_in_schema("information_schema", 1, None)?
        .expect("the built-in namespace is public");
    assert_eq!(tables.rows.len(), 1);
    assert_eq!(tables.rows[0].table_schema, "information_schema");
    assert_eq!(tables.rows[0].table_name, "columns");
    assert_eq!(tables.rows[0].table_type, "VIEW");
    let tables_continuation = tables
        .continuation
        .clone()
        .expect("built-in relation follows");

    let columns = invocation
        .scan_information_schema_columns_for_relation("information_schema", "columns", 1, None)?
        .expect("the built-in relation shape is public");
    assert_eq!(columns.rows.len(), 1);
    assert_eq!(columns.rows[0].table_schema, "information_schema");
    assert_eq!(columns.rows[0].table_name, "columns");
    assert_eq!(columns.rows[0].column_name, "table_schema");
    assert_eq!(columns.rows[0].ordinal_position, 1);
    assert_eq!(columns.rows[0].data_type, "TEXT");
    assert_eq!(columns.rows[0].is_nullable, "NO");
    let columns_continuation = columns
        .continuation
        .clone()
        .expect("built-in column follows");

    let mut other_table_invocation =
        InvocationTransaction::begin(&database, [213; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_table_invocation.scan_information_schema_tables_in_schema(
            "information_schema",
            1,
            Some(tables_continuation),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let mut other_column_invocation =
        InvocationTransaction::begin(&database, [214; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_column_invocation.scan_information_schema_columns_for_relation(
            "information_schema",
            "columns",
            1,
            Some(columns_continuation),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let description = invocation
        .resolve_catalog_authorized_relation("information_schema", "columns")?
        .expect("the public definition resolves");
    assert_eq!(description.name, "columns");
    assert_eq!(description.columns.len(), 9);
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        [
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "data_type",
            "is_nullable",
            "character_maximum_length",
            "numeric_precision",
            "numeric_scale",
        ]
    );
    assert!(
        description.columns[6..]
            .iter()
            .all(|column| column.nullable)
    );
    assert!(
        description.columns[..6]
            .iter()
            .all(|column| !column.nullable)
    );
    assert_eq!(description.columns[3].scalar_type, ScalarType::Integer);
    assert_eq!(description.columns[6].scalar_type, ScalarType::Integer);
    let schemata_description = invocation
        .resolve_catalog_authorized_relation("information_schema", "schemata")?
        .expect("the public schema definition resolves");
    assert_eq!(schemata_description.columns[0].name, "schema_name");
    let tables_description = invocation
        .resolve_catalog_authorized_relation("information_schema", "tables")?
        .expect("the public table definition resolves");
    assert_eq!(
        tables_description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["table_schema", "table_name", "table_type"]
    );

    let all_tables = invocation.scan_information_schema_tables(1, None)?;
    assert_eq!(all_tables.rows[0].table_schema, "information_schema");
    let all_columns = invocation.scan_information_schema_columns(1, None)?;
    assert_eq!(all_columns.rows[0].table_schema, "information_schema");
    Ok(())
}

#[test]
fn information_schema_tables_projects_authorized_schema_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [201; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .scan_information_schema_tables_in_schema("app", 1, None)?
        .expect("app is visible");
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_schema, "app");
    assert_eq!(first.rows[0].table_name, "metrics");
    assert_eq!(first.rows[0].table_type, "BASE TABLE");
    let second = invocation
        .scan_information_schema_tables_in_schema("app", 1, first.continuation)?
        .expect("app is visible");
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].table_name, "secret");
    assert!(second.continuation.is_none());

    let empty = invocation
        .scan_information_schema_tables_in_schema("public", 1, None)?
        .expect("public is visible");
    assert!(empty.rows.is_empty());
    assert!(empty.continuation.is_none());
    assert!(
        invocation
            .scan_information_schema_tables_in_schema("private", 1, None)?
            .is_none()
    );
    assert!(
        invocation
            .scan_information_schema_tables_in_schema("missing", 1, None)?
            .is_none()
    );

    let mut cursor_probe =
        InvocationTransaction::begin(&database, [202; 16], [9; 16], 9_999, 1, 64)?;
    let projected = cursor_probe
        .scan_information_schema_tables_in_schema("app", 1, None)?
        .expect("app is visible");
    let cursor = projected.continuation.expect("second relation follows");
    assert!(matches!(
        cursor_probe.list_catalog_authorized_relations([0x91; 16], 1, Some(cursor)),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

#[test]
fn information_schema_tables_scan_walks_visible_schema_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [205; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.scan_information_schema_tables(1, None)?;
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_schema, "information_schema");
    assert_eq!(first.rows[0].table_name, "columns");
    assert_eq!(first.rows[0].table_type, "VIEW");
    let cursor = first
        .continuation
        .expect("second built-in relation follows");
    let cursor_bytes = cursor.to_bytes();
    assert!(
        prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(&cursor_bytes)
            .is_some()
    );
    assert!(
        prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&cursor_bytes).is_none()
    );
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&cursor_bytes).is_none());
    let second = invocation.scan_information_schema_tables(
        1,
        Some(
            prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(
                &cursor_bytes,
            )
            .expect("table scan token round trips"),
        ),
    )?;
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].table_schema, "information_schema");
    assert_eq!(second.rows[0].table_name, "schemata");
    assert_eq!(second.rows[0].table_type, "VIEW");
    let next_builtin_relation = second
        .continuation
        .expect("third built-in relation follows");
    let third = invocation.scan_information_schema_tables(1, Some(next_builtin_relation))?;
    assert_eq!(third.rows[0].table_schema, "information_schema");
    assert_eq!(third.rows[0].table_name, "tables");
    assert_eq!(third.rows[0].table_type, "VIEW");
    let next_app = third.continuation.expect("visible app schema follows");
    let fourth = invocation.scan_information_schema_tables(1, Some(next_app))?;
    assert_eq!(fourth.rows[0].table_schema, "app");
    assert_eq!(fourth.rows[0].table_name, "metrics");
    assert_eq!(fourth.rows[0].table_type, "BASE TABLE");
    let next_secret = fourth.continuation.expect("second app relation follows");
    let fifth = invocation.scan_information_schema_tables(1, Some(next_secret))?;
    assert_eq!(fifth.rows[0].table_schema, "app");
    assert_eq!(fifth.rows[0].table_name, "secret");
    assert_eq!(fifth.rows[0].table_type, "BASE TABLE");
    let next_schema = fifth.continuation.expect("visible public schema follows");
    let sixth = invocation.scan_information_schema_tables(1, Some(next_schema))?;
    assert!(sixth.rows.is_empty());
    assert!(sixth.continuation.is_none());

    let mut cross_invocation =
        InvocationTransaction::begin(&database, [206; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        cross_invocation.scan_information_schema_tables(
            1,
            Some(
                prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(
                    &cursor_bytes,
                )
                .expect("table scan token round trips"),
            ),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut replay_invocation =
        InvocationTransaction::begin(&database, [207; 16], [9; 16], 9_999, 1, 64)?;
    let first = replay_invocation.scan_information_schema_tables(1, None)?;
    let first_cursor = first.continuation.expect("second relation follows");
    let second = replay_invocation.scan_information_schema_tables(1, Some(first_cursor.clone()))?;
    assert!(second.continuation.is_some());
    assert!(matches!(
        replay_invocation.scan_information_schema_tables(1, Some(first_cursor)),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut substitution_probe =
        InvocationTransaction::begin(&database, [212; 16], [9; 16], 9_999, 1, 64)?;
    let first = substitution_probe.scan_information_schema_tables(1, None)?;
    let mut substituted_cursor_bytes = first
        .continuation
        .expect("another table follows")
        .to_bytes();
    substituted_cursor_bytes[8] ^= 1;
    assert!(matches!(
        substitution_probe.scan_information_schema_tables(
            1,
            Some(
                prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(
                    &substituted_cursor_bytes,
                )
                .expect("same-kind substituted cursor retains its wire shape"),
            ),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

#[test]
fn information_schema_tables_cursor_retains_renamed_relation_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut retained = InvocationTransaction::begin(&database, [211; 16], [9; 16], 9_999, 1, 64)?;
    let first = retained.scan_information_schema_tables(1, None)?;
    assert_eq!(first.rows[0].table_name, "columns");
    let continuation = first.continuation.expect("more visible relations follow");

    let table_id = [0x10; 16];
    let object_key = catalog_id_key(2, table_id);
    let current_object = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(object_key.as_slice())?
        .ok_or("metrics table object missing")?
        .value()
        .to_vec();
    let mut object = prdb_engine::catalog::decode_registered_envelope(&current_object)?;
    let name = object
        .fields
        .iter_mut()
        .find(|field| field.tag == 3)
        .expect("table object has a name");
    name.payload = b"renamed_metrics".to_vec();
    let object_version = object
        .fields
        .iter_mut()
        .find(|field| field.tag == 5)
        .expect("table object has a version");
    let version = u64::from_le_bytes(object_version.payload.as_slice().try_into()?);
    object_version.payload = version
        .checked_add(1)
        .ok_or("table object version overflow")?
        .to_le_bytes()
        .to_vec();
    let renamed_object = encode_envelope(
        &family_schema(2).expect("table object family is registered"),
        &object.logical_key,
        &object.fields,
    )?;
    let table_key = catalog_id_key(5, table_id);
    let current_table = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(table_key.as_slice())?
        .ok_or("metrics table definition missing")?
        .value()
        .to_vec();
    let mut table = prdb_engine::catalog::decode_registered_envelope(&current_table)?;
    let epoch = table
        .fields
        .iter_mut()
        .find(|field| field.tag == 3)
        .expect("table definition has a write epoch");
    let current_epoch = u64::from_le_bytes(epoch.payload.as_slice().try_into()?);
    epoch.payload = current_epoch
        .checked_add(1)
        .ok_or("table write epoch overflow")?
        .to_le_bytes()
        .to_vec();
    let renamed_table = encode_envelope(
        &family_schema(5).expect("table definition family is registered"),
        &table.logical_key,
        &table.fields,
    )?;
    // This fixture intentionally omits parts of the production catalog, so
    // publish the two catalog records directly. The invocation snapshots are
    // the behavior under test; DDL validation is covered elsewhere.
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        catalog.insert(table_key.as_slice(), renamed_table.as_slice())?;
        catalog.insert(object_key.as_slice(), renamed_object.as_slice())?;
    }
    write.commit()?;

    let mut retained_names = first
        .rows
        .into_iter()
        .map(|row| (row.table_schema, row.table_name))
        .collect::<Vec<_>>();
    let mut cursor = Some(continuation);
    while let Some(next) = cursor {
        let page = retained.scan_information_schema_tables(1, Some(next))?;
        retained_names.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        cursor = page.continuation;
    }
    assert!(retained_names.contains(&("app".to_owned(), "metrics".to_owned())));
    assert!(!retained_names.contains(&("app".to_owned(), "renamed_metrics".to_owned())));

    let mut current = InvocationTransaction::begin(&database, [215; 16], [9; 16], 9_999, 1, 64)?;
    let mut current_names = Vec::new();
    let mut page = current.scan_information_schema_tables(1, None)?;
    loop {
        current_names.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        match page.continuation {
            Some(next) => page = current.scan_information_schema_tables(1, Some(next))?,
            None => break,
        }
    }
    assert!(!current_names.contains(&("app".to_owned(), "metrics".to_owned())));
    assert!(current_names.contains(&("app".to_owned(), "renamed_metrics".to_owned())));
    Ok(())
}

#[test]
fn information_schema_tables_cursor_retains_grant_snapshot_across_revocation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;

    let mut retained = InvocationTransaction::begin(&database, [0xe1; 16], [9; 16], 9_999, 1, 64)?;
    let first = retained.scan_information_schema_tables(1, None)?;
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_name, "columns");
    let continuation = first
        .continuation
        .expect("public relation rows precede visible application tables");

    let grantor_id = builtin_owner_id();
    let revoked_grants = [
        (
            [0x91; 16],
            grant_catalog_record([9; 16], [0x91; 16], 1, 1, [0x91; 16], grantor_id, 1).0,
        ),
        (
            [0x10; 16],
            grant_catalog_record([9; 16], [0x10; 16], 3, 1, [0x10; 16], grantor_id, 1).0,
        ),
        (
            [0x10; 16],
            grant_catalog_record([9; 16], [0x10; 16], 2, 2, [0x66; 16], grantor_id, 1).0,
        ),
    ];
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        let mut grants = write.open_table(prdb_engine::schema::GRANTS)?;
        for (object_id, grant_key) in &revoked_grants {
            catalog.remove(grant_key.as_slice())?;
            for direction in [1u8, 2u8] {
                let indexed_id = if direction == 1 { [9; 16] } else { *object_id };
                let mut index_key = Vec::with_capacity(1 + 16 + grant_key.len());
                index_key.push(direction);
                index_key.extend_from_slice(&indexed_id);
                index_key.extend_from_slice(grant_key);
                grants.remove(index_key.as_slice())?;
            }
        }
    }
    write.commit()?;

    let mut retained_names = first
        .rows
        .into_iter()
        .map(|row| (row.table_schema, row.table_name))
        .collect::<Vec<_>>();
    let mut cursor = Some(continuation.clone());
    while let Some(next) = cursor {
        let page = retained.scan_information_schema_tables(1, Some(next))?;
        retained_names.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        cursor = page.continuation;
    }
    assert!(retained_names.contains(&("app".to_owned(), "metrics".to_owned())));

    let mut current = InvocationTransaction::begin(&database, [0xe2; 16], [9; 16], 9_999, 1, 64)?;
    let mut current_names = Vec::new();
    let mut page = current.scan_information_schema_tables(1, None)?;
    loop {
        current_names.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        match page.continuation {
            Some(next) => page = current.scan_information_schema_tables(1, Some(next))?,
            None => break,
        }
    }
    assert!(!current_names.contains(&("app".to_owned(), "metrics".to_owned())));
    assert!(
        current_names
            .iter()
            .all(|(schema, _)| schema == "information_schema")
    );

    let mut other = InvocationTransaction::begin(&database, [0xe3; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other.scan_information_schema_tables(1, Some(continuation)),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

#[test]
fn information_schema_columns_projects_declared_shape_in_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [203; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .scan_information_schema_columns_for_relation("app", "metrics", 1, None)?
        .expect("metrics is visible");
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_schema, "app");
    assert_eq!(first.rows[0].table_name, "metrics");
    assert_eq!(first.rows[0].column_name, "value");
    assert_eq!(first.rows[0].ordinal_position, 1);
    assert_eq!(first.rows[0].data_type, "BYTEA");
    assert_eq!(first.rows[0].is_nullable, "YES");
    assert_eq!(first.rows[0].character_maximum_length, None);
    assert_eq!(first.rows[0].numeric_precision, None);
    assert_eq!(first.rows[0].numeric_scale, None);
    let second = invocation
        .scan_information_schema_columns_for_relation("app", "metrics", 1, first.continuation)?
        .expect("metrics is visible");
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].column_name, "value2");
    assert_eq!(second.rows[0].ordinal_position, 2);
    assert_eq!(second.rows[0].is_nullable, "NO");
    assert!(second.continuation.is_none());
    assert!(
        invocation
            .scan_information_schema_columns_for_relation("private", "metrics", 1, None)?
            .is_none()
    );
    assert!(
        invocation
            .scan_information_schema_columns_for_relation("app", "missing", 1, None)?
            .is_none()
    );

    let mut cursor_probe =
        InvocationTransaction::begin(&database, [204; 16], [9; 16], 9_999, 1, 64)?;
    let projected = cursor_probe
        .scan_information_schema_columns_for_relation("app", "metrics", 1, None)?
        .expect("metrics is visible");
    let cursor = projected.continuation.expect("second column follows");
    let (role_ids, grants) = cursor_probe.load_catalog_relation_discovery_grants([0x91; 16])?;
    let scope = grants
        .iter()
        .map(|grant| (grant.operation, grant.target))
        .collect();
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids,
        scope,
    };
    assert!(matches!(
        cursor_probe.describe_authorized_columns_page(
            [0x10; 16],
            1,
            Some(cursor),
            &context,
            &grants,
            &[],
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

#[test]
fn information_schema_columns_scan_walks_visible_relation_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [209; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.scan_information_schema_columns(1, None)?;
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_schema, "information_schema");
    assert_eq!(first.rows[0].table_name, "columns");
    assert_eq!(first.rows[0].column_name, "table_schema");
    assert_eq!(first.rows[0].ordinal_position, 1);
    let cursor = first.continuation.clone().expect("second column follows");
    let cursor_bytes = cursor.to_bytes();
    assert!(
        prdb_engine::metadata_discovery::InformationSchemaColumnsCursor::from_bytes(&cursor_bytes)
            .is_some()
    );
    assert!(prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(
        prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(&cursor_bytes)
            .is_none()
    );
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(
        prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&cursor_bytes).is_none()
    );
    let expected_columns = [
        ("information_schema", "columns", "table_schema", 1),
        ("information_schema", "columns", "table_name", 2),
        ("information_schema", "columns", "column_name", 3),
        ("information_schema", "columns", "ordinal_position", 4),
        ("information_schema", "columns", "data_type", 5),
        ("information_schema", "columns", "is_nullable", 6),
        (
            "information_schema",
            "columns",
            "character_maximum_length",
            7,
        ),
        ("information_schema", "columns", "numeric_precision", 8),
        ("information_schema", "columns", "numeric_scale", 9),
        ("information_schema", "schemata", "schema_name", 1),
        ("information_schema", "tables", "table_schema", 1),
        ("information_schema", "tables", "table_name", 2),
        ("information_schema", "tables", "table_type", 3),
        ("app", "metrics", "value", 1),
        ("app", "metrics", "value2", 2),
        ("app", "secret", "secret_value", 1),
    ];
    let mut page = first;
    for (index, (table_schema, table_name, column_name, ordinal_position)) in
        expected_columns.iter().enumerate()
    {
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].table_schema, *table_schema);
        assert_eq!(page.rows[0].table_name, *table_name);
        assert_eq!(page.rows[0].column_name, *column_name);
        assert_eq!(page.rows[0].ordinal_position, *ordinal_position);
        if index + 1 < expected_columns.len() {
            let next = page.continuation.expect("next visible column follows");
            page = invocation.scan_information_schema_columns(1, Some(next))?;
        }
    }
    let next_schema = page.continuation.expect("visible public schema follows");
    let final_page = invocation.scan_information_schema_columns(1, Some(next_schema))?;
    assert!(final_page.rows.is_empty());
    assert!(final_page.continuation.is_none());

    let mut cross_invocation =
        InvocationTransaction::begin(&database, [210; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        cross_invocation.scan_information_schema_columns(
            1,
            Some(
                prdb_engine::metadata_discovery::InformationSchemaColumnsCursor::from_bytes(
                    &cursor_bytes,
                )
                .expect("columns scan token round trips"),
            ),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut replay_invocation =
        InvocationTransaction::begin(&database, [211; 16], [9; 16], 9_999, 1, 64)?;
    let first = replay_invocation.scan_information_schema_columns(1, None)?;
    let first_cursor = first.continuation.expect("second column follows");
    let second =
        replay_invocation.scan_information_schema_columns(1, Some(first_cursor.clone()))?;
    assert!(second.continuation.is_some());
    assert!(matches!(
        replay_invocation.scan_information_schema_columns(1, Some(first_cursor)),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut substitution_probe =
        InvocationTransaction::begin(&database, [212; 16], [9; 16], 9_999, 1, 64)?;
    let first = substitution_probe.scan_information_schema_columns(1, None)?;
    let mut substituted_cursor_bytes = first
        .continuation
        .expect("another column follows")
        .to_bytes();
    substituted_cursor_bytes[8] ^= 1;
    assert!(matches!(
        substitution_probe.scan_information_schema_columns(
            1,
            Some(
                prdb_engine::metadata_discovery::InformationSchemaColumnsCursor::from_bytes(
                    &substituted_cursor_bytes,
                )
                .expect("same-kind substituted cursor retains its wire shape"),
            ),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

#[test]
fn information_schema_scans_respect_each_principals_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;

    let mut broader = InvocationTransaction::begin(&database, [218; 16], [9; 16], 9_999, 1, 64)?;
    let mut restricted =
        InvocationTransaction::begin(&database, [219; 16], [10; 16], 9_999, 1, 64)?;

    let broader_schemata = broader
        .scan_information_schema_schemata(128, None)?
        .rows
        .into_iter()
        .map(|row| row.schema_name)
        .collect::<Vec<_>>();
    let restricted_schemata = restricted
        .scan_information_schema_schemata(128, None)?
        .rows
        .into_iter()
        .map(|row| row.schema_name)
        .collect::<Vec<_>>();
    assert_eq!(broader_schemata, ["information_schema", "app", "public"]);
    assert_eq!(restricted_schemata, ["information_schema", "public"]);

    let mut broader_tables = Vec::new();
    let mut broader_table_cursor = None;
    loop {
        let page = broader.scan_information_schema_tables(128, broader_table_cursor)?;
        broader_tables.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        broader_table_cursor = page.continuation;
        if broader_table_cursor.is_none() {
            break;
        }
    }
    let mut restricted_tables = Vec::new();
    let mut restricted_table_cursor = None;
    loop {
        let page = restricted.scan_information_schema_tables(128, restricted_table_cursor)?;
        restricted_tables.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        restricted_table_cursor = page.continuation;
        if restricted_table_cursor.is_none() {
            break;
        }
    }
    assert!(broader_tables.contains(&("app".to_owned(), "metrics".to_owned())));
    assert!(broader_tables.contains(&("app".to_owned(), "secret".to_owned())));
    assert_eq!(restricted_tables.len(), 3);
    assert!(
        restricted_tables
            .iter()
            .all(|(schema, _)| schema == "information_schema")
    );

    let mut broader_columns = Vec::new();
    let mut broader_column_cursor = None;
    loop {
        let page = broader.scan_information_schema_columns(128, broader_column_cursor)?;
        broader_columns.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name, row.column_name)),
        );
        broader_column_cursor = page.continuation;
        if broader_column_cursor.is_none() {
            break;
        }
    }
    let mut restricted_columns = Vec::new();
    let mut restricted_column_cursor = None;
    loop {
        let page = restricted.scan_information_schema_columns(128, restricted_column_cursor)?;
        restricted_columns.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name, row.column_name)),
        );
        restricted_column_cursor = page.continuation;
        if restricted_column_cursor.is_none() {
            break;
        }
    }
    assert!(broader_columns.iter().any(|(schema, table, column)| {
        schema == "app" && table == "metrics" && column == "value2"
    }));
    assert!(broader_columns.iter().any(|(schema, table, column)| {
        schema == "app" && table == "secret" && column == "secret_value"
    }));
    assert_eq!(restricted_columns.len(), 13);
    assert!(
        restricted_columns
            .iter()
            .all(|(schema, _, _)| schema == "information_schema")
    );
    Ok(())
}

#[test]
fn executor_scan_reads_filtered_information_schema_schemata()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [220; 16], [9; 16], 9_999, 1, 64)?;
    let mut connection = invocation.open_executor_metadata_connection();
    let (relation, description) = connection
        .resolve_relation("information_schema", "schemata")?
        .expect("public virtual relation resolves");
    assert_eq!(description.columns.len(), 1);
    assert_eq!(description.columns[0].name, "schema_name");

    let mut cursor = connection.open_relation_scan(&relation, &[])?;
    let mut names = Vec::new();
    let mut page_number = 0;
    let mut virtual_row_handle = None;
    loop {
        page_number += 1;
        let consumed_cursor = cursor.clone();
        let page = connection
            .next_relation_scan_batch(&cursor, 1, 1024)
            .map_err(|error| format!("batch {page_number} failed: {error:?}"))?;
        for (handle, cells) in page.rows {
            virtual_row_handle.get_or_insert(handle);
            match cells.as_slice() {
                [SanitizedCell::Value(ScalarValue::Text(name))] => names.push(name.clone()),
                _ => return Err("virtual schema row did not have one text cell".into()),
            }
        }
        assert!(matches!(
            connection.next_relation_scan_batch(&consumed_cursor, 1, 1024),
            Err(TransactionError::InvalidMetadataRequest)
        ));
        match page.continuation {
            Some(continuation) => cursor = continuation,
            None => break,
        }
    }
    assert_eq!(names, ["information_schema", "app", "public"]);

    let (tables_relation, _) = connection
        .resolve_relation("information_schema", "tables")?
        .expect("public tables relation resolves");
    let mut cursor = connection.open_relation_scan(&tables_relation, &[])?;
    let mut tables = Vec::new();
    let mut tables_row_handle = None;
    loop {
        let page = connection.next_relation_scan_batch(&cursor, 1, 1024)?;
        for (handle, cells) in page.rows {
            tables_row_handle.get_or_insert(handle);
            tables.push(cells);
        }
        match page.continuation {
            Some(continuation) => cursor = continuation,
            None => break,
        }
    }
    assert_eq!(tables.len(), 5);
    assert!(matches!(
        tables[0].as_slice(),
        [
            SanitizedCell::Value(ScalarValue::Text(schema)),
            SanitizedCell::Value(ScalarValue::Text(table)),
            SanitizedCell::Value(ScalarValue::Text(kind)),
        ] if schema == "information_schema" && table == "columns" && kind == "VIEW"
    ));
    assert!(matches!(
        tables[3].as_slice(),
        [
            SanitizedCell::Value(ScalarValue::Text(schema)),
            SanitizedCell::Value(ScalarValue::Text(table)),
            SanitizedCell::Value(ScalarValue::Text(kind)),
        ] if schema == "app" && table == "metrics" && kind == "BASE TABLE"
    ));

    let (columns_relation, _) = connection
        .resolve_relation("information_schema", "columns")?
        .expect("public columns relation resolves");
    let mut cursor = connection.open_relation_scan(&columns_relation, &[])?;
    let mut columns = Vec::new();
    let mut columns_row_handle = None;
    loop {
        let page = connection.next_relation_scan_batch(&cursor, 1, 1024)?;
        for (handle, cells) in page.rows {
            columns_row_handle.get_or_insert(handle);
            columns.push(cells);
        }
        match page.continuation {
            Some(continuation) => cursor = continuation,
            None => break,
        }
    }
    assert_eq!(columns.len(), 16);
    assert!(matches!(
        columns[0].as_slice(),
        [
            SanitizedCell::Value(ScalarValue::Text(schema)),
            SanitizedCell::Value(ScalarValue::Text(table)),
            SanitizedCell::Value(ScalarValue::Text(column)),
            SanitizedCell::Value(ScalarValue::Integer(1)),
            SanitizedCell::Value(ScalarValue::Text(data_type)),
            SanitizedCell::Value(ScalarValue::Text(nullable)),
            SanitizedCell::Null,
            SanitizedCell::Null,
            SanitizedCell::Null,
        ] if schema == "information_schema" && table == "columns" && column == "table_schema"
            && data_type == "TEXT" && nullable == "NO"
    ));
    assert!(matches!(
        columns[14].as_slice(),
        [
            SanitizedCell::Value(ScalarValue::Text(schema)),
            SanitizedCell::Value(ScalarValue::Text(table)),
            SanitizedCell::Value(ScalarValue::Text(column)),
            SanitizedCell::Value(ScalarValue::Integer(2)),
            ..
        ] if schema == "app" && table == "metrics" && column == "value2"
    ));
    for handle in [
        virtual_row_handle
            .as_ref()
            .expect("virtual schema row handle was issued"),
        tables_row_handle
            .as_ref()
            .expect("virtual table row handle was issued"),
        columns_row_handle
            .as_ref()
            .expect("virtual column row handle was issued"),
    ] {
        assert!(matches!(
            connection.stage_update(
                handle,
                &[FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Text("forbidden".to_owned())),
                }],
            ),
            Err(TransactionError::InvalidMetadataRequest)
        ));
        assert!(matches!(
            connection.stage_delete(handle),
            Err(TransactionError::InvalidMetadataRequest)
        ));
    }
    drop(connection);

    for (index, relation_name) in ["schemata", "tables", "columns"].into_iter().enumerate() {
        let mut mutation_invocation = InvocationTransaction::begin(
            &database,
            [221 + index as u8; 16],
            [9; 16],
            9_999,
            1,
            64,
        )?;
        let mut mutation_connection = mutation_invocation.open_executor_metadata_connection();
        let (virtual_relation, description) = mutation_connection
            .resolve_relation("information_schema", relation_name)?
            .expect("public virtual relation resolves for read-only rejection");
        let insert_values = vec![InsertInput::Value(None); description.columns.len()];
        assert!(matches!(
            mutation_connection.stage_insert(&virtual_relation, &insert_values),
            Err(TransactionError::InvalidContext)
        ));
    }
    Ok(())
}

#[test]
fn column_grants_can_discover_a_relation_and_pages_are_snapshot_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: [0x10; 16],
                    column_id: [0x66; 16],
                },
            ),
            (Operation::Delete, AccessTarget::Table([0x20; 16])),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: [0x10; 16],
                column_id: [0x66; 16],
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Delete,
            target: AccessTarget::Table([0x20; 16]),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [181; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .list_authorized_relations(schema_id, 1, None, &context, &grants, &[])?
        .expect("schema is discoverable");
    assert_eq!(first.relations.len(), 1);
    assert_eq!(first.relations[0].name, "metrics");
    let cursor = first.continuation.expect("second granted relation");
    let cursor_bytes = cursor.to_bytes();
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&cursor_bytes).is_none());
    let cursor = prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&cursor_bytes)
        .expect("relation cursor wire form is fixed width");
    let mut other_invocation =
        InvocationTransaction::begin(&database, [182; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_invocation.list_authorized_relations(
            schema_id,
            1,
            Some(cursor.clone()),
            &context,
            &grants,
            &[],
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let second = invocation
        .list_authorized_relations(schema_id, 1, Some(cursor.clone()), &context, &grants, &[])?
        .expect("schema remains discoverable");
    assert_eq!(second.relations.len(), 1);
    assert_eq!(second.relations[0].name, "secret");
    assert!(second.continuation.is_none());
    assert!(matches!(
        invocation.list_authorized_relations(schema_id, 1, Some(cursor), &context, &grants, &[]),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

#[test]
fn authorized_column_description_returns_only_declared_shape()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Insert, AccessTarget::Table(table_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [184; 16], [9; 16], 9_999, 1, 64)?;
    let description = invocation
        .describe_authorized_columns(table_id, &context, &grants, &[])?
        .expect("table write grant permits shape discovery");
    assert_eq!(description.relation_id, table_id);
    assert_eq!(description.columns.len(), 1);
    assert_eq!(
        description.columns[0],
        prdb_engine::metadata_discovery::VisibleColumn {
            column_id: [0x66; 16],
            name: "value".to_owned(),
            ordinal: 0,
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }
    );

    let no_data_scope = AuthorizationContext {
        scope: std::collections::HashSet::from([(
            Operation::Usage,
            AccessTarget::Schema(schema_id),
        )]),
        ..context
    };
    assert!(
        invocation
            .describe_authorized_columns(table_id, &no_data_scope, &grants, &[])?
            .is_none()
    );
    Ok(())
}

#[test]
fn authorized_relation_resolution_requires_visibility_and_exact_name()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut malformed_hidden_row_key = [0x20; 32];
    malformed_hidden_row_key[16..].fill(0xfe);
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&malformed_hidden_row_key[..], &b"not-a-row"[..])?;
    write.commit()?;
    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Insert, AccessTarget::Table(table_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [188; 16], [9; 16], 9_999, 1, 64)?;
    let resolved = invocation
        .resolve_authorized_relation(schema_id, "metrics", &context, &grants, &[])?
        .expect("visible exact name resolves");
    assert_eq!(resolved.relation_id, table_id);
    assert_eq!(resolved.name, "metrics");
    assert_eq!(resolved.columns.len(), 1);
    assert_eq!(resolved.columns[0].name, "value");
    assert!(
        invocation
            .resolve_authorized_relation(schema_id, "secret", &context, &grants, &[])?
            .is_none()
    );
    assert!(
        invocation
            .resolve_authorized_relation(schema_id, "missing", &context, &grants, &[])?
            .is_none()
    );
    assert!(
        invocation
            .resolve_authorized_relation(schema_id, "Metrics", &context, &grants, &[])?
            .is_none()
    );
    Ok(())
}

#[test]
fn catalog_authorized_qualified_relation_resolution_uses_pinned_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [194; 16], [9; 16], 9_999, 1, 64)?;
    let resolved = invocation
        .resolve_catalog_authorized_relation("app", "metrics")?
        .expect("qualified visible relation resolves");
    assert_eq!(resolved.relation_id, [0x10; 16]);
    assert_eq!(resolved.name, "metrics");
    assert_eq!(resolved.columns.len(), 1);
    assert_eq!(resolved.columns[0].name, "value");
    assert!(
        invocation
            .resolve_catalog_authorized_relation("private", "secret")?
            .is_none()
    );
    assert!(
        invocation
            .resolve_catalog_authorized_relation("missing", "secret")?
            .is_none()
    );
    assert!(
        invocation
            .resolve_catalog_authorized_relation("app", "missing")?
            .is_none()
    );
    assert!(
        invocation
            .resolve_catalog_authorized_relation("APP", "metrics")?
            .is_none()
    );
    Ok(())
}

#[test]
fn executor_relation_handles_are_connection_scoped_and_revalidated()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [215; 16], [9; 16], 9_999, 1, 64)?;
    let handle_bytes;
    {
        let mut connection = invocation.open_executor_metadata_connection();
        let (handle, description) = connection
            .resolve_relation("app", "metrics")?
            .expect("qualified visible relation resolves");
        assert_eq!(description.relation_id, [0x10; 16]);
        assert_eq!(description.columns[0].name, "value");
        handle_bytes = handle.to_bytes();
        let decoded =
            prdb_engine::metadata_discovery::ResolvedRelationHandle::from_bytes(&handle_bytes)
                .expect("opaque handle has its fixed wire form");
        assert!(
            prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&handle_bytes).is_none()
        );
        assert!(
            prdb_engine::metadata_discovery::ResolvedRelationHandle::from_bytes(
                &handle_bytes[..15]
            )
            .is_none()
        );
        assert_eq!(
            connection.revalidate_relation_handle(&decoded)?,
            description
        );
        connection.close_relation_handle(&decoded)?;
        assert!(matches!(
            connection.revalidate_relation_handle(&decoded),
            Err(TransactionError::InvalidMetadataRequest)
        ));
    }

    let decoded =
        prdb_engine::metadata_discovery::ResolvedRelationHandle::from_bytes(&handle_bytes)
            .expect("opaque handle has its fixed wire form");
    let mut new_connection = invocation.open_executor_metadata_connection();
    assert!(matches!(
        new_connection.revalidate_relation_handle(&decoded),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    drop(new_connection);

    let mut other_invocation =
        InvocationTransaction::begin(&database, [216; 16], [9; 16], 9_999, 1, 64)?;
    let mut other_connection = other_invocation.open_executor_metadata_connection();
    assert!(matches!(
        other_connection.revalidate_relation_handle(&decoded),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut bounded_connection = invocation.open_executor_metadata_connection();
    for _ in 0..prdb_engine::metadata_discovery::MAX_RELATION_HANDLES {
        assert!(
            bounded_connection
                .resolve_relation("app", "metrics")?
                .is_some()
        );
    }
    assert!(matches!(
        bounded_connection.resolve_relation("app", "metrics"),
        Err(TransactionError::RelationHandleLimitExceeded)
    ));
    Ok(())
}

#[test]
fn executor_resolve_relation_ipc_dispatch_returns_only_authorized_handle()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        ExecutorRequestError, OpenScanRequest, ResolveRelationRequest, ResolveRelationResponse,
        StageInsertRequest, dispatch_executor_request, dispatch_executor_resolve_relation,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, ServiceRole};
    use prdb_engine::transaction::InsertInput;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let app_schema_id = [0x91; 16];
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        let mut grants = write.open_table(prdb_engine::schema::GRANTS)?;
        let (grant_key, grant_value) = grant_catalog_record(
            [10; 16],
            app_schema_id,
            1,
            1,
            app_schema_id,
            builtin_owner_id(),
            1,
        );
        catalog.insert(&grant_key[..], &grant_value[..])?;
        for (direction, indexed_id) in [(1u8, [10; 16]), (2u8, app_schema_id)] {
            let mut index_key = Vec::with_capacity(76);
            index_key.push(direction);
            index_key.extend_from_slice(&indexed_id);
            index_key.extend_from_slice(&grant_key);
            grants.insert(&index_key[..], &[][..])?;
        }
    }
    write.commit()?;
    let mut invocation = InvocationTransaction::begin(&database, [217; 16], [9; 16], 9_999, 1, 64)?;
    let mut connection = invocation.open_executor_metadata_connection();

    let encode_request = |schema: &str, relation: &str| {
        ResolveRelationRequest {
            schema_name: schema.to_owned(),
            relation_name: relation.to_owned(),
        }
        .encode()
        .expect("valid relation names encode")
    };
    let request = IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 41,
        payload: encode_request("app", "metrics"),
    };
    assert_eq!(
        request.payload,
        [
            0, 1, 0, 3, 0, 7, b'a', b'p', b'p', b'm', b'e', b't', b'r', b'i', b'c', b's'
        ]
    );
    let response =
        dispatch_executor_resolve_relation(ServiceRole::Executor, &mut connection, &request)?;
    assert_eq!(response.operation, request.operation);
    assert_eq!(response.request_id, request.request_id);
    let ResolveRelationResponse::Resolved {
        handle,
        description,
    } = ResolveRelationResponse::decode(&response.payload)?
    else {
        panic!("visible relation must resolve")
    };
    assert_eq!(description.relation_id, [0x10; 16]);
    assert_eq!(description.name, "metrics");
    assert_eq!(description.columns[0].name, "value");
    assert_eq!(
        connection.revalidate_relation_handle(&handle)?.relation_id,
        [0x10; 16]
    );
    let mut forged_handle_bytes = handle.to_bytes();
    forged_handle_bytes[1] ^= 1;
    let forged_handle =
        prdb_engine::metadata_discovery::ResolvedRelationHandle::from_bytes(&forged_handle_bytes)
            .expect("forged token retains the valid wire shape");
    let forged_open_scan = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 42,
        payload: OpenScanRequest {
            relation: forged_handle,
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &forged_open_scan),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));

    connection.close_relation_handle(&handle)?;
    let closed_handle_scan = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 43,
        payload: OpenScanRequest {
            relation: handle.clone(),
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &closed_handle_scan),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));
    let closed_handle_insert = IpcFrame {
        operation: Operation::StageInsert as u16,
        request_id: 45,
        payload: StageInsertRequest {
            relation: handle.clone(),
            values: vec![InsertInput::Value(Some(ScalarValue::BigInt(1)))],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut connection,
            &closed_handle_insert
        ),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));
    let reopened = dispatch_executor_resolve_relation(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::ResolveRelation as u16,
            request_id: 44,
            payload: encode_request("app", "metrics"),
        },
    )?;
    let ResolveRelationResponse::Resolved {
        handle: reopened_handle,
        ..
    } = ResolveRelationResponse::decode(&reopened.payload)?
    else {
        panic!("visible relation must resolve after handle close")
    };
    assert_ne!(
        reopened_handle, handle,
        "reopening issues a fresh capability"
    );
    let mut stale_after_reopen = closed_handle_scan;
    stale_after_reopen.request_id = 45;
    assert!(matches!(
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &stale_after_reopen),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));

    let hidden_request = IpcFrame {
        payload: encode_request("private", "metrics"),
        ..request.clone()
    };
    let hidden_response = dispatch_executor_resolve_relation(
        ServiceRole::Executor,
        &mut connection,
        &hidden_request,
    )?;
    assert_eq!(
        ResolveRelationResponse::decode(&hidden_response.payload)?,
        ResolveRelationResponse::Unavailable
    );
    let hidden_table_request = IpcFrame {
        payload: encode_request("app", "secret"),
        ..request.clone()
    };
    let absent_table_request = IpcFrame {
        payload: encode_request("app", "missing"),
        ..request.clone()
    };
    let mut restricted_invocation =
        InvocationTransaction::begin(&database, [218; 16], [10; 16], 9_999, 1, 64)?;
    let mut restricted_connection = restricted_invocation.open_executor_metadata_connection();
    let hidden_table_response = dispatch_executor_resolve_relation(
        ServiceRole::Executor,
        &mut restricted_connection,
        &hidden_table_request,
    )?;
    let absent_table_response = dispatch_executor_resolve_relation(
        ServiceRole::Executor,
        &mut restricted_connection,
        &absent_table_request,
    )?;
    assert_eq!(
        ResolveRelationResponse::decode(&hidden_table_response.payload)?,
        ResolveRelationResponse::Unavailable
    );
    assert_eq!(
        hidden_table_response.payload, absent_table_response.payload,
        "a hidden relation and an absent name must have identical executor IPC diagnostics"
    );

    assert!(matches!(
        dispatch_executor_resolve_relation(ServiceRole::Planner, &mut connection, &request),
        Err(ExecutorRequestError::Dispatch(_))
    ));
    let malformed = IpcFrame {
        payload: vec![0, 1, 0],
        ..request
    };
    assert!(matches!(
        dispatch_executor_resolve_relation(ServiceRole::Executor, &mut connection, &malformed),
        Err(ExecutorRequestError::InvalidPayload)
    ));
    drop(connection);
    let mut replay_connection = invocation.open_executor_metadata_connection();
    let cross_connection_replay = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 46,
        payload: OpenScanRequest {
            relation: reopened_handle.clone(),
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut replay_connection,
            &cross_connection_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));
    let mut other_principal_invocation =
        InvocationTransaction::begin(&database, [0x9f; 16], [10; 16], 9_999, 1, 64)?;
    let mut other_principal_connection =
        other_principal_invocation.open_executor_metadata_connection();
    let cross_principal_replay = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 47,
        payload: OpenScanRequest {
            relation: reopened_handle,
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut other_principal_connection,
            &cross_principal_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));
    Ok(())
}

#[test]
fn executor_relation_socket_loop_ends_session_without_finalizing()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        CloseCursorRequest, ExecutorConnectionError, OpenScanRequest, OpenScanResponse,
        ResolveRelationRequest, ResolveRelationResponse, serve_executor,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let invocation_id = [218; 16];
    let principal_id = [9; 16];
    let mut invocation =
        InvocationTransaction::begin(&database, invocation_id, principal_id, 9_999, 1, 64)?;
    let (mut client, mut server) = UnixStream::pair()?;
    let client_task = std::thread::spawn(move || {
        let resolve_request = IpcFrame {
            operation: Operation::ResolveRelation as u16,
            request_id: 51,
            payload: ResolveRelationRequest {
                schema_name: "app".to_owned(),
                relation_name: "metrics".to_owned(),
            }
            .encode()
            .unwrap(),
        };
        write_frame(&mut client, &resolve_request).unwrap();
        let resolve_response = read_frame(&mut client).unwrap().unwrap();
        let ResolveRelationResponse::Resolved { handle, .. } =
            ResolveRelationResponse::decode(&resolve_response.payload).unwrap()
        else {
            panic!("visible relation must resolve over the stream")
        };

        let open_request = IpcFrame {
            operation: Operation::OpenScan as u16,
            request_id: 52,
            payload: OpenScanRequest {
                relation: handle,
                column_ids: Vec::new(),
            }
            .encode()
            .unwrap(),
        };
        write_frame(&mut client, &open_request).unwrap();
        let open_response = read_frame(&mut client).unwrap().unwrap();
        let cursor = OpenScanResponse::decode(&open_response.payload)
            .unwrap()
            .cursor;

        let close_request = IpcFrame {
            operation: Operation::CloseCursor as u16,
            request_id: 53,
            payload: CloseCursorRequest { cursor }.encode(),
        };
        write_frame(&mut client, &close_request).unwrap();
        let close_response = read_frame(&mut client).unwrap().unwrap();
        assert_eq!(close_response.payload, [0, 1, 0]);
        client.shutdown(Shutdown::Write).unwrap();
    });
    serve_executor(&mut server, &mut invocation)?;
    client_task
        .join()
        .expect("client stream sequence completes");
    assert!(lookup_outcome(&database, invocation_id, principal_id, 1)?.is_none());
    invocation.abort()?;
    let outcome = lookup_outcome(&database, invocation_id, principal_id, 1)?
        .expect("the supervisor's abort persists an outcome");
    assert_eq!(outcome.status, OutcomeStatus::Aborted);

    let duplicate_invocation_id = [219; 16];
    let mut duplicate_invocation = InvocationTransaction::begin(
        &database,
        duplicate_invocation_id,
        principal_id,
        9_999,
        1,
        64,
    )?;
    let (mut duplicate_client, mut duplicate_server) = UnixStream::pair()?;
    let duplicate_request = IpcFrame {
        request_id: 52,
        operation: Operation::ResolveRelation as u16,
        payload: ResolveRelationRequest {
            schema_name: "app".to_owned(),
            relation_name: "metrics".to_owned(),
        }
        .encode()?,
    };
    write_frame(&mut duplicate_client, &duplicate_request)?;
    write_frame(&mut duplicate_client, &duplicate_request)?;
    duplicate_client.shutdown(Shutdown::Write)?;
    assert!(matches!(
        serve_executor(&mut duplicate_server, &mut duplicate_invocation),
        Err(ExecutorConnectionError::DuplicateRequestId)
    ));
    assert!(read_frame(&mut duplicate_client)?.is_some());
    let duplicate_outcome = lookup_outcome(&database, duplicate_invocation_id, principal_id, 1)?
        .expect("duplicate request persists an aborted outcome");
    assert_eq!(duplicate_outcome.status, OutcomeStatus::Aborted);
    Ok(())
}

#[test]
fn information_schema_virtual_scans_round_trip_through_executor_ipc()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        NextBatchRequest, NextBatchResponse, OpenScanRequest, OpenScanResponse,
        ResolveRelationRequest, ResolveRelationResponse, serve_executor,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;

    let mut invocation = InvocationTransaction::begin(&database, [222; 16], [9; 16], 9_999, 1, 64)?;
    let (mut client, mut server) = UnixStream::pair()?;
    let client_task = std::thread::spawn(move || {
        let mut request_id = 1u64;
        macro_rules! exchange {
            ($operation:expr, $payload:expr) => {{
                let request = IpcFrame {
                    operation: $operation as u16,
                    request_id,
                    payload: $payload,
                };
                request_id += 1;
                write_frame(&mut client, &request).unwrap();
                read_frame(&mut client).unwrap().unwrap()
            }};
        }

        let mut collected = Vec::new();
        for relation_name in ["schemata", "tables", "columns"] {
            let response = exchange!(
                Operation::ResolveRelation,
                ResolveRelationRequest {
                    schema_name: "information_schema".to_owned(),
                    relation_name: relation_name.to_owned(),
                }
                .encode()
                .unwrap()
            );
            let ResolveRelationResponse::Resolved {
                handle,
                description,
            } = ResolveRelationResponse::decode(&response.payload).unwrap()
            else {
                panic!("public information_schema relation resolves")
            };
            assert_eq!(description.name, relation_name);

            let response = exchange!(
                Operation::OpenScan,
                OpenScanRequest {
                    relation: handle,
                    column_ids: Vec::new(),
                }
                .encode()
                .unwrap()
            );
            let mut cursor = OpenScanResponse::decode(&response.payload).unwrap().cursor;
            let mut relation_rows = Vec::new();
            loop {
                let response = exchange!(
                    Operation::NextBatch,
                    NextBatchRequest {
                        cursor: cursor.clone(),
                        max_scanned: 1,
                        max_bytes: 4096,
                    }
                    .encode()
                    .unwrap()
                );
                let page = NextBatchResponse::decode(&response.payload).unwrap();
                relation_rows.extend(page.rows.into_iter().map(|row| row.cells));
                match page.continuation {
                    Some(next) => cursor = next,
                    None => break,
                }
            }
            collected.push((relation_name, relation_rows));
        }
        client.shutdown(Shutdown::Write).unwrap();
        collected
    });

    serve_executor(&mut server, &mut invocation)?;
    let collected = client_task
        .join()
        .expect("executor IPC scan sequence completes");

    let schemata = &collected[0].1;
    assert_eq!(
        schemata,
        &[
            vec![SanitizedCell::Value(ScalarValue::Text(
                "information_schema".to_owned(),
            ))],
            vec![SanitizedCell::Value(ScalarValue::Text("app".to_owned()))],
            vec![SanitizedCell::Value(ScalarValue::Text("public".to_owned()))],
        ]
    );
    let tables = &collected[1].1;
    assert_eq!(tables.len(), 5);
    assert_eq!(
        tables[0],
        [
            SanitizedCell::Value(ScalarValue::Text("information_schema".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("columns".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("VIEW".to_owned())),
        ]
    );
    assert_eq!(
        tables[3],
        [
            SanitizedCell::Value(ScalarValue::Text("app".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("metrics".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("BASE TABLE".to_owned())),
        ]
    );

    let columns = &collected[2].1;
    assert_eq!(columns.len(), 16);
    assert_eq!(
        columns[0],
        [
            SanitizedCell::Value(ScalarValue::Text("information_schema".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("columns".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("table_schema".to_owned())),
            SanitizedCell::Value(ScalarValue::Integer(1)),
            SanitizedCell::Value(ScalarValue::Text("TEXT".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("NO".to_owned())),
            SanitizedCell::Null,
            SanitizedCell::Null,
            SanitizedCell::Null,
        ]
    );
    assert_eq!(
        columns[14][..4],
        [
            SanitizedCell::Value(ScalarValue::Text("app".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("metrics".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("value2".to_owned())),
            SanitizedCell::Value(ScalarValue::Integer(2)),
        ]
    );
    invocation.abort()?;
    Ok(())
}

#[test]
fn catalog_authorized_scan_builds_its_projection_from_pinned_records()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        CloseCursorRequest, ExecutorRequestError, NextBatchRequest, NextBatchResponse,
        OpenScanRequest, OpenScanResponse, StageDeleteRequest, StageInsertRequest,
        StageInsertResponse, StageUpdateRequest, dispatch_executor_request,
    };
    use prdb_engine::ipc::{IpcFrame, Operation as IpcOperation, ServiceRole};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;

    let principal_id = [9; 16];
    let restricted_principal_id = [10; 16];
    let table_id = [0x10; 16];
    let boolean_type_id = [46; 16];
    let policy_id = [221; 16];
    let expression_id = [222; 16];
    let policy_key = catalog_id_key(13, policy_id);
    let cell_policy_id = [226; 16];
    let cell_policy_key = catalog_id_key(13, cell_policy_id);
    let expression_key = catalog_id_key(7, expression_id);
    let (target_key, target_payload) = policy_target_catalog_record(policy_id, 1, principal_id);
    let (cell_principal_target_key, cell_principal_target_payload) =
        policy_target_catalog_record(cell_policy_id, 1, principal_id);
    let (cell_column_target_key, cell_column_target_payload) =
        policy_target_catalog_record(cell_policy_id, 3, [0x66; 16]);
    let (type_object_key, type_object_payload) = object_catalog_record_for(boolean_type_id, 2);
    let type_write = boolean_type_catalog_write(boolean_type_id)?;
    let (table_select_key, table_select_payload) = grant_catalog_record(
        principal_id,
        table_id,
        2,
        1,
        table_id,
        builtin_owner_id(),
        1,
    );
    let (table_delete_key, table_delete_payload) = grant_catalog_record(
        principal_id,
        table_id,
        5,
        1,
        table_id,
        builtin_owner_id(),
        1,
    );
    let delete_policy_id = [228; 16];
    let delete_policy_key = catalog_id_key(13, delete_policy_id);
    let (delete_policy_target_key, delete_policy_target_payload) =
        policy_target_catalog_record(delete_policy_id, 1, principal_id);
    let update_policy_id = [230; 16];
    let update_policy_key = catalog_id_key(13, update_policy_id);
    let (update_policy_target_key, update_policy_target_payload) =
        policy_target_catalog_record(update_policy_id, 1, principal_id);
    let cell_update_policy_id = [231; 16];
    let cell_update_policy_key = catalog_id_key(13, cell_update_policy_id);
    let (cell_update_principal_target_key, cell_update_principal_target_payload) =
        policy_target_catalog_record(cell_update_policy_id, 1, principal_id);
    let (cell_update_column_target_key, cell_update_column_target_payload) =
        policy_target_catalog_record(cell_update_policy_id, 3, [0x66; 16]);
    let update_check_expression_id = [232; 16];
    let update_check_expression_key = catalog_id_key(7, update_check_expression_id);
    let insert_policy_id = [233; 16];
    let (insert_policy_key, insert_policy_payload) = policy_catalog_record(
        insert_policy_id,
        table_id,
        1,
        2,
        None,
        Some(update_check_expression_id),
    );
    let (insert_policy_target_key, insert_policy_target_payload) =
        policy_target_catalog_record(insert_policy_id, 1, principal_id);
    let cell_insert_policy_id = [234; 16];
    let (cell_insert_policy_key, cell_insert_policy_payload) = policy_catalog_record(
        cell_insert_policy_id,
        table_id,
        2,
        2,
        None,
        Some(update_check_expression_id),
    );
    let (cell_insert_principal_target_key, cell_insert_principal_target_payload) =
        policy_target_catalog_record(cell_insert_policy_id, 1, principal_id);
    let (cell_insert_column_target_key, cell_insert_column_target_payload) =
        policy_target_catalog_record(cell_insert_policy_id, 3, [0x66; 16]);
    let (restricted_schema_grant_key, restricted_schema_grant_payload) = grant_catalog_record(
        restricted_principal_id,
        [0x91; 16],
        1,
        1,
        [0x91; 16],
        builtin_owner_id(),
        1,
    );
    let (restricted_table_grant_key, restricted_table_grant_payload) = grant_catalog_record(
        restricted_principal_id,
        table_id,
        2,
        1,
        table_id,
        builtin_owner_id(),
        1,
    );
    let (restricted_column_grant_key, restricted_column_grant_payload) = grant_catalog_record(
        restricted_principal_id,
        table_id,
        2,
        2,
        [0x66; 16],
        builtin_owner_id(),
        1,
    );
    let restricted_row_policy_id = [235; 16];
    let (restricted_row_policy_key, restricted_row_policy_payload) = policy_catalog_record(
        restricted_row_policy_id,
        table_id,
        1,
        1,
        Some(expression_id),
        None,
    );
    let (restricted_row_target_key, restricted_row_target_payload) =
        policy_target_catalog_record(restricted_row_policy_id, 1, restricted_principal_id);
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        let mut grants = write.open_table(prdb_engine::schema::GRANTS)?;
        for (key, payload) in [
            (
                type_write.key.clone(),
                type_write.payload.clone().expect("type definition payload"),
            ),
            (type_object_key, type_object_payload),
            (
                expression_key.clone(),
                expression_catalog_payload_full(
                    expression_id,
                    3,
                    boolean_type_id,
                    [0; 8],
                    &[],
                    &Expr::Literal(Some(ScalarValue::Boolean(true))),
                )?,
            ),
            (
                update_check_expression_key.clone(),
                expression_catalog_payload_full(
                    update_check_expression_id,
                    4,
                    boolean_type_id,
                    [0; 8],
                    &[],
                    &Expr::Literal(Some(ScalarValue::Boolean(true))),
                )?,
            ),
            (
                policy_key.clone(),
                catalog_record_payload(
                    13,
                    &policy_key,
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
                            payload: expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (
                cell_policy_key.clone(),
                catalog_record_payload(
                    13,
                    &cell_policy_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U16,
                            payload: 2u16.to_le_bytes().to_vec(),
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
                            payload: expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (
                delete_policy_key.clone(),
                catalog_record_payload(
                    13,
                    &delete_policy_key,
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
                            payload: 4u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::Id,
                            payload: expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (
                update_policy_key.clone(),
                catalog_record_payload(
                    13,
                    &update_policy_key,
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
                            payload: 3u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::Id,
                            payload: expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 6,
                            wire_type: WireType::Id,
                            payload: update_check_expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (
                cell_update_policy_key.clone(),
                catalog_record_payload(
                    13,
                    &cell_update_policy_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U16,
                            payload: 2u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U16,
                            payload: 3u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::Id,
                            payload: expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 6,
                            wire_type: WireType::Id,
                            payload: update_check_expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (target_key, target_payload),
            (insert_policy_key, insert_policy_payload),
            (cell_insert_policy_key, cell_insert_policy_payload),
            (cell_principal_target_key, cell_principal_target_payload),
            (cell_column_target_key, cell_column_target_payload),
            (delete_policy_target_key, delete_policy_target_payload),
            (insert_policy_target_key, insert_policy_target_payload),
            (
                cell_insert_principal_target_key,
                cell_insert_principal_target_payload,
            ),
            (
                cell_insert_column_target_key,
                cell_insert_column_target_payload,
            ),
            (restricted_schema_grant_key, restricted_schema_grant_payload),
            (restricted_table_grant_key, restricted_table_grant_payload),
            (restricted_column_grant_key, restricted_column_grant_payload),
            (restricted_row_policy_key, restricted_row_policy_payload),
            (restricted_row_target_key, restricted_row_target_payload),
            (update_policy_target_key, update_policy_target_payload),
            (
                cell_update_principal_target_key,
                cell_update_principal_target_payload,
            ),
            (
                cell_update_column_target_key,
                cell_update_column_target_payload,
            ),
            (table_select_key, table_select_payload),
            (table_delete_key, table_delete_payload),
            grant_catalog_record(
                principal_id,
                table_id,
                3,
                1,
                table_id,
                builtin_owner_id(),
                1,
            ),
            grant_catalog_record(
                principal_id,
                table_id,
                3,
                2,
                [0x66; 16],
                builtin_owner_id(),
                1,
            ),
            grant_catalog_record(
                principal_id,
                table_id,
                4,
                1,
                table_id,
                builtin_owner_id(),
                1,
            ),
            grant_catalog_record(
                principal_id,
                table_id,
                4,
                2,
                [0x66; 16],
                builtin_owner_id(),
                1,
            ),
        ] {
            catalog.insert(&key[..], &payload[..])?;
            if key.starts_with(&12u16.to_be_bytes()) {
                let grantee: [u8; 16] = key[3..19].try_into()?;
                let object: [u8; 16] = key[20..36].try_into()?;
                for (direction, indexed_id) in [(1u8, grantee), (2u8, object)] {
                    let mut index_key = Vec::with_capacity(76);
                    index_key.push(direction);
                    index_key.extend_from_slice(&indexed_id);
                    index_key.extend_from_slice(&key);
                    grants.insert(&index_key[..], &[][..])?;
                }
            }
        }
    }
    write.commit()?;

    let mut seed =
        InvocationTransaction::begin(&database, [224; 16], principal_id, 9_999, 2, 16_384)?;
    let mut seed_connection = seed.open_executor_metadata_connection();
    let (seed_relation, _) = seed_connection
        .resolve_relation("app", "metrics")?
        .expect("insert grants keep the relation discoverable");
    let seed_first = seed_connection.stage_insert(
        &seed_relation,
        &[InsertInput::Value(Some(ScalarValue::Bytea(vec![1, 2, 3])))],
    )?;
    let seed_first_id = seed_connection.revalidate_row_handle(&seed_first)?;
    let seed_second = seed_connection.stage_insert(
        &seed_relation,
        &[InsertInput::Value(Some(ScalarValue::Bytea(vec![4, 5, 6])))],
    )?;
    let seed_second_id = seed_connection.revalidate_row_handle(&seed_second)?;
    drop(seed_connection);
    seed.commit()?;
    let (row_id, first_value, second_row_id, second_value) =
        if seed_first_id.row < seed_second_id.row {
            (seed_first_id, vec![1, 2, 3], seed_second_id, vec![4, 5, 6])
        } else {
            (seed_second_id, vec![4, 5, 6], seed_first_id, vec![1, 2, 3])
        };

    let mut invocation =
        InvocationTransaction::begin(&database, [225; 16], principal_id, 9_999, 4, 16_384)?;
    let point_read = invocation
        .read_catalog_authorized_row(table_id, row_id.row)?
        .expect("catalog grants authorize the seeded point read");
    assert_eq!(
        point_read,
        vec![SanitizedCell::Value(ScalarValue::Bytea(
            first_value.clone()
        ))]
    );
    assert!(
        invocation
            .read_catalog_authorized_row(table_id, [0x99; 16])?
            .is_none()
    );
    for (invocation_id, requested_table_id, requested_row_id) in [
        ([226; 16], [0; 16], [0xd3; 16]),
        ([227; 16], table_id, [0; 16]),
    ] {
        let mut invalid =
            InvocationTransaction::begin(&database, invocation_id, principal_id, 9_999, 1, 1024)?;
        assert!(matches!(
            invalid.read_catalog_authorized_row(requested_table_id, requested_row_id),
            Err(TransactionError::InvalidContext)
        ));
    }
    let page = invocation.scan_catalog_authorized_rows(table_id, None, 3)?;
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.rows[0].0, row_id);
    assert_eq!(page.rows[1].0, second_row_id);
    assert!(matches!(page.rows[0].1[0], SanitizedCell::Value(_)));
    assert!(page.continuation.is_none());

    let mut connection = invocation.open_executor_metadata_connection();
    let (relation_handle, _) = connection
        .resolve_relation("app", "metrics")?
        .expect("catalog grants authorize relation resolution");
    let open_request = IpcFrame {
        operation: IpcOperation::OpenScan as u16,
        request_id: 61,
        payload: OpenScanRequest {
            relation: relation_handle.clone(),
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    let open_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &open_request)?;
    let scan_cursor = OpenScanResponse::decode(&open_response.payload)?.cursor;
    assert_eq!(scan_cursor.to_bytes()[0], 7);
    assert!(
        prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&scan_cursor.to_bytes())
            .is_none()
    );

    let first_request = IpcFrame {
        operation: IpcOperation::NextBatch as u16,
        request_id: 62,
        payload: NextBatchRequest {
            cursor: scan_cursor.clone(),
            max_scanned: 2,
            max_bytes: 48,
        }
        .encode()?,
    };
    let first_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &first_request)?;
    let first_page = NextBatchResponse::decode(&first_response.payload)?;
    assert_eq!(first_page.rows.len(), 1);
    assert_eq!(
        first_page.rows[0].cells,
        [SanitizedCell::Value(ScalarValue::Bytea(
            first_value.clone()
        ))]
    );
    assert_eq!(first_page.rows[0].handle.to_bytes()[0], 8);
    assert_eq!(
        connection.revalidate_row_handle(&first_page.rows[0].handle)?,
        row_id
    );
    let mut replay_invocation =
        InvocationTransaction::begin(&database, [0x9f; 16], restricted_principal_id, 9_999, 1, 64)?;
    let mut replay_connection = replay_invocation.open_executor_metadata_connection();
    let mut replay_target_row_key = Vec::with_capacity(32);
    replay_target_row_key.extend_from_slice(&table_id);
    replay_target_row_key.extend_from_slice(&row_id.row);
    let replay_target_before = database
        .begin_read()?
        .open_table(ROWS)?
        .get(replay_target_row_key.as_slice())?
        .expect("seeded row is present before cross-principal IPC")
        .value()
        .to_vec();
    let cross_principal_row_replay = IpcFrame {
        operation: IpcOperation::StageDelete as u16,
        request_id: 73,
        payload: StageDeleteRequest {
            handle: first_page.rows[0].handle.clone(),
        }
        .encode(),
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut replay_connection,
            &cross_principal_row_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            TransactionError::InvalidMetadataRequest
        ))
    ));
    let cross_principal_row_update_replay = IpcFrame {
        operation: IpcOperation::StageUpdate as u16,
        request_id: 74,
        payload: StageUpdateRequest {
            handle: first_page.rows[0].handle.clone(),
            patches: vec![FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(
                    b"forged cross-principal update".to_vec(),
                )),
            }],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut replay_connection,
            &cross_principal_row_update_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            TransactionError::InvalidMetadataRequest
        ))
    ));
    drop(replay_connection);
    replay_invocation.abort()?;

    let mut same_principal_replay_invocation =
        InvocationTransaction::begin(&database, [0xa1; 16], principal_id, 9_999, 1, 64)?;
    let mut same_principal_replay_connection =
        same_principal_replay_invocation.open_executor_metadata_connection();
    let same_principal_row_replay = IpcFrame {
        operation: IpcOperation::StageDelete as u16,
        request_id: 75,
        payload: StageDeleteRequest {
            handle: first_page.rows[0].handle.clone(),
        }
        .encode(),
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut same_principal_replay_connection,
            &same_principal_row_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            TransactionError::InvalidMetadataRequest
        ))
    ));
    let same_principal_row_update_replay = IpcFrame {
        operation: IpcOperation::StageUpdate as u16,
        request_id: 76,
        payload: StageUpdateRequest {
            handle: first_page.rows[0].handle.clone(),
            patches: vec![FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(
                    b"forged cross-invocation update".to_vec(),
                )),
            }],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut same_principal_replay_connection,
            &same_principal_row_update_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            TransactionError::InvalidMetadataRequest
        ))
    ));
    drop(same_principal_replay_connection);
    same_principal_replay_invocation.abort()?;
    assert_eq!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(replay_target_row_key.as_slice())?
            .expect("forged IPC requests leave the seeded row present")
            .value(),
        replay_target_before,
        "cross-principal and cross-invocation DELETE and UPDATE requests must not change the stored row"
    );
    let next_cursor = first_page
        .continuation
        .expect("full page returns a fresh continuation");
    assert!(matches!(
        connection.next_relation_scan_page(&scan_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let second_request = IpcFrame {
        request_id: 63,
        payload: NextBatchRequest {
            cursor: next_cursor.clone(),
            max_scanned: 1,
            max_bytes: 256,
        }
        .encode()?,
        ..first_request.clone()
    };
    let second_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &second_request)?;
    let second_page = NextBatchResponse::decode(&second_response.payload)?;
    assert_eq!(second_page.rows.len(), 1);
    assert_eq!(
        second_page.rows[0].cells,
        [SanitizedCell::Value(ScalarValue::Bytea(second_value))]
    );
    let final_cursor = second_page
        .continuation
        .expect("full final page requires an EOF confirmation");
    let final_request = IpcFrame {
        request_id: 64,
        payload: NextBatchRequest {
            cursor: final_cursor.clone(),
            max_scanned: 1,
            max_bytes: 256,
        }
        .encode()?,
        ..first_request.clone()
    };
    let final_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &final_request)?;
    let final_page = NextBatchResponse::decode(&final_response.payload)?;
    assert!(final_page.rows.is_empty());
    assert!(final_page.continuation.is_none());
    assert!(matches!(
        connection.next_relation_scan_page(&final_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let second_open_request = IpcFrame {
        request_id: 65,
        ..open_request
    };
    let second_open_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &second_open_request)?;
    let close_cursor = OpenScanResponse::decode(&second_open_response.payload)?.cursor;
    let alias_page_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::NextBatch as u16,
            request_id: 70,
            payload: NextBatchRequest {
                cursor: close_cursor.clone(),
                max_scanned: 2,
                max_bytes: 256,
            }
            .encode()?,
        },
    )?;
    let alias_page = NextBatchResponse::decode(&alias_page_response.payload)?;
    assert_eq!(alias_page.rows.len(), 2);
    let update_alias = alias_page
        .rows
        .iter()
        .find(|row| connection.revalidate_row_handle(&row.handle).ok() == Some(row_id))
        .expect("second scan issues an alias handle for the updated row")
        .handle
        .clone();
    let delete_alias = alias_page
        .rows
        .iter()
        .find(|row| connection.revalidate_row_handle(&row.handle).ok() == Some(second_row_id))
        .expect("second scan issues an alias handle for the deleted row")
        .handle
        .clone();
    assert_ne!(update_alias, first_page.rows[0].handle);
    assert_ne!(delete_alias, second_page.rows[0].handle);
    let alias_cursor = alias_page
        .continuation
        .expect("full alias page requires an EOF confirmation");
    let close_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::CloseCursor as u16,
            request_id: 66,
            payload: CloseCursorRequest {
                cursor: alias_cursor.clone(),
            }
            .encode(),
        },
    )?;
    assert_eq!(close_response.payload, [0, 1, 0]);
    assert!(matches!(
        connection.next_relation_scan_page(&alias_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let insert_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::StageInsert as u16,
            request_id: 67,
            payload: StageInsertRequest {
                relation: relation_handle.clone(),
                values: vec![InsertInput::Value(Some(ScalarValue::Bytea(vec![4, 5])))],
            }
            .encode()?,
        },
    )?;
    let inserted_handle = StageInsertResponse::decode(&insert_response.payload)?.handle;
    let inserted_row_id = connection.revalidate_row_handle(&inserted_handle)?;
    assert_eq!(inserted_row_id.table, table_id);
    let updated_row_handle = first_page.rows[0].handle.clone();
    let update_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::StageUpdate as u16,
            request_id: 68,
            payload: StageUpdateRequest {
                handle: updated_row_handle.clone(),
                patches: vec![FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Bytea(vec![9, 9])),
                }],
            }
            .encode()?,
        },
    )?;
    assert_eq!(update_response.payload, [0, 1, 0]);
    assert!(matches!(
        connection.revalidate_row_handle(&updated_row_handle),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(
        matches!(
            connection.revalidate_row_handle(&update_alias),
            Err(TransactionError::InvalidMetadataRequest)
        ),
        "UPDATE invalidates every handle previously issued for the row"
    );
    let deleted_row_handle = second_page.rows[0].handle.clone();
    let delete_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::StageDelete as u16,
            request_id: 69,
            payload: StageDeleteRequest {
                handle: deleted_row_handle.clone(),
            }
            .encode(),
        },
    )?;
    assert_eq!(delete_response.payload, [0, 1, 0]);
    assert!(matches!(
        connection.revalidate_row_handle(&deleted_row_handle),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(
        matches!(
            connection.revalidate_row_handle(&delete_alias),
            Err(TransactionError::InvalidMetadataRequest)
        ),
        "DELETE invalidates every handle previously issued for the row"
    );
    let relation_close_cursor = connection.open_relation_scan(&relation_handle, &[[0x66; 16]])?;
    connection.close_relation_handle(&relation_handle)?;
    assert!(matches!(
        connection.revalidate_relation_handle(&relation_handle),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(matches!(
        connection.revalidate_row_handle(&inserted_handle),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(matches!(
        connection.next_relation_scan_page(&relation_close_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    drop(connection);
    let mut replacement = invocation.open_executor_metadata_connection();
    assert!(matches!(
        replacement.next_relation_scan_page(&next_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    drop(replacement);
    invocation.commit()?;
    let mut verify =
        InvocationTransaction::begin(&database, [236; 16], principal_id, 9_999, 1, 128)?;
    let persisted_schema = verify.load_catalog_table_schema(table_id)?;
    let persisted = verify
        .read(row_id)?
        .expect("updated row remains visible after commit");
    assert_eq!(
        decode_tuple(
            &persisted_schema.columns,
            persisted_schema.schema_version,
            &persisted
        )?
        .values,
        [Some(ScalarValue::Bytea(vec![9, 9]))]
    );
    assert!(verify.read(second_row_id)?.is_none());
    let inserted = verify
        .read(inserted_row_id)?
        .expect("inserted row remains visible after commit");
    assert_eq!(
        decode_tuple(
            &persisted_schema.columns,
            persisted_schema.schema_version,
            &inserted
        )?
        .values,
        [Some(ScalarValue::Bytea(vec![4, 5]))]
    );
    verify.abort()?;

    let mut restricted = InvocationTransaction::begin(
        &database,
        [238; 16],
        restricted_principal_id,
        9_999,
        1,
        1024,
    )?;
    let restricted_schema = restricted.load_catalog_table_schema(table_id)?;
    let (_, restricted_grants) =
        restricted.load_catalog_grants(restricted_principal_id, &restricted_schema)?;
    let restricted_policies =
        restricted.load_catalog_policies(restricted_principal_id, &restricted_schema)?;
    assert_eq!(restricted_grants.len(), 3);
    assert_eq!(restricted_policies.len(), 1);
    assert_eq!(
        restricted.read_catalog_authorized_row(table_id, row_id.row)?,
        Some(vec![SanitizedCell::Redacted])
    );
    let mut restricted_connection = restricted.open_executor_metadata_connection();
    let (restricted_relation, _) = restricted_connection
        .resolve_relation("app", "metrics")?
        .expect("the table grant should make the relation available");
    let restricted_open = dispatch_executor_request(
        ServiceRole::Executor,
        &mut restricted_connection,
        &IpcFrame {
            operation: IpcOperation::OpenScan as u16,
            request_id: 71,
            payload: OpenScanRequest {
                relation: restricted_relation,
                column_ids: vec![[0x66; 16]],
            }
            .encode()?,
        },
    )?;
    let restricted_cursor = OpenScanResponse::decode(&restricted_open.payload)?.cursor;
    let restricted_batch = dispatch_executor_request(
        ServiceRole::Executor,
        &mut restricted_connection,
        &IpcFrame {
            operation: IpcOperation::NextBatch as u16,
            request_id: 72,
            payload: NextBatchRequest {
                cursor: restricted_cursor,
                max_scanned: 8,
                max_bytes: 1024,
            }
            .encode()?,
        },
    )?;
    let restricted_rows = NextBatchResponse::decode(&restricted_batch.payload)?.rows;
    assert_eq!(restricted_rows.len(), 2);
    assert!(
        restricted_rows
            .iter()
            .all(|row| { row.cells == [SanitizedCell::Redacted] })
    );
    drop(restricted_connection);
    restricted.abort()?;

    let oversized_invocation_id = [237; 16];
    let mut oversized = InvocationTransaction::begin(
        &database,
        oversized_invocation_id,
        principal_id,
        9_999,
        1,
        128,
    )?;
    let mut oversized_connection = oversized.open_executor_metadata_connection();
    let (oversized_relation, _) = oversized_connection
        .resolve_relation("app", "metrics")?
        .expect("visible relation resolves");
    let open_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut oversized_connection,
        &IpcFrame {
            operation: IpcOperation::OpenScan as u16,
            request_id: 71,
            payload: OpenScanRequest {
                relation: oversized_relation,
                column_ids: vec![[0x66; 16]],
            }
            .encode()?,
        },
    )?;
    let oversized_cursor = OpenScanResponse::decode(&open_response.payload)?.cursor;
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut oversized_connection,
            &IpcFrame {
                operation: IpcOperation::NextBatch as u16,
                request_id: 72,
                payload: NextBatchRequest {
                    cursor: oversized_cursor,
                    max_scanned: 1,
                    max_bytes: 46,
                }
                .encode()?,
            },
        ),
        Err(
            prdb_engine::executor_ipc::ExecutorRequestError::Transaction(
                TransactionError::MetadataResultTooLarge
            )
        )
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, oversized_invocation_id, principal_id, 1,)?
            .expect("oversized batch aborts durably")
            .status,
        OutcomeStatus::Aborted
    );
    let guarded_value = vec![9, 9];
    let policy_expression = Expr::Equal(Box::new(Expr::Field(0)), Box::new(Expr::Field(0)));
    let policy_id = [0xf4; 16];
    let using_expression_id = [0xf5; 16];
    let check_expression_id = [0xf6; 16];
    let external_policy_id = [0xf8; 16];
    let external_using_expression_id = [0xf9; 16];
    let external_check_expression_id = [0xfa; 16];
    let select_policy_id = [0xf1; 16];
    let select_expression_id = [0xf2; 16];
    let select_expression = Expr::Literal(Some(ScalarValue::Boolean(true)));
    let (select_policy_key, select_policy_payload) = policy_catalog_record(
        select_policy_id,
        table_id,
        1,
        1,
        Some(select_expression_id),
        None,
    );
    let (select_policy_target_key, select_policy_target_payload) =
        policy_target_catalog_record(select_policy_id, 1, principal_id);
    let select_expression_key = catalog_id_key(7, select_expression_id);
    let select_expression_payload = expression_catalog_payload_full(
        select_expression_id,
        3,
        [46; 16],
        [0; 8],
        &[],
        &select_expression,
    )?;
    let (policy_key, policy_payload) = policy_catalog_record(
        policy_id,
        table_id,
        1,
        3,
        Some(using_expression_id),
        Some(check_expression_id),
    );
    let (policy_target_key, policy_target_payload) =
        policy_target_catalog_record(policy_id, 1, principal_id);
    let allow_expression = Expr::Literal(Some(ScalarValue::Boolean(true)));
    let using_expression_key = catalog_id_key(7, using_expression_id);
    let check_expression_key = catalog_id_key(7, check_expression_id);
    let using_expression_payload = expression_catalog_payload_full(
        using_expression_id,
        3,
        [46; 16],
        [0; 8],
        &[],
        &allow_expression,
    )?;
    let check_expression_payload = expression_catalog_payload_full(
        check_expression_id,
        4,
        [46; 16],
        [0; 8],
        &[],
        &allow_expression,
    )?;
    let (external_policy_key, external_policy_payload) = policy_catalog_record(
        external_policy_id,
        table_id,
        1,
        3,
        Some(external_using_expression_id),
        Some(external_check_expression_id),
    );
    let (external_policy_target_key, external_policy_target_payload) =
        policy_target_catalog_record(external_policy_id, 1, restricted_principal_id);
    let external_using_expression_key = catalog_id_key(7, external_using_expression_id);
    let external_check_expression_key = catalog_id_key(7, external_check_expression_id);
    let external_using_expression_payload = expression_catalog_payload_full(
        external_using_expression_id,
        3,
        [46; 16],
        [0; 8],
        &[[0x66; 16]],
        &policy_expression,
    )?;
    let external_check_expression_payload = expression_catalog_payload_full(
        external_check_expression_id,
        4,
        [46; 16],
        [0; 8],
        &[[0x66; 16]],
        &policy_expression,
    )?;
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        catalog.insert(
            select_policy_key.as_slice(),
            select_policy_payload.as_slice(),
        )?;
        catalog.insert(
            select_policy_target_key.as_slice(),
            select_policy_target_payload.as_slice(),
        )?;
        catalog.insert(
            select_expression_key.as_slice(),
            select_expression_payload.as_slice(),
        )?;
        catalog.insert(policy_key.as_slice(), policy_payload.as_slice())?;
        catalog.insert(
            policy_target_key.as_slice(),
            policy_target_payload.as_slice(),
        )?;
        catalog.insert(
            using_expression_key.as_slice(),
            using_expression_payload.as_slice(),
        )?;
        catalog.insert(
            check_expression_key.as_slice(),
            check_expression_payload.as_slice(),
        )?;
        catalog.insert(
            external_policy_key.as_slice(),
            external_policy_payload.as_slice(),
        )?;
        catalog.insert(
            external_policy_target_key.as_slice(),
            external_policy_target_payload.as_slice(),
        )?;
        catalog.insert(
            external_using_expression_key.as_slice(),
            external_using_expression_payload.as_slice(),
        )?;
        catalog.insert(
            external_check_expression_key.as_slice(),
            external_check_expression_payload.as_slice(),
        )?;
        let mut policies = write.open_table(schema::POLICIES)?;
        for (operation, indexed_policy_id) in [
            (1u16, select_policy_id),
            (1u16, restricted_row_policy_id),
            (3u16, policy_id),
            (3u16, external_policy_id),
        ] {
            let mut index_key = Vec::with_capacity(34);
            index_key.extend_from_slice(&table_id);
            index_key.extend_from_slice(&operation.to_be_bytes());
            index_key.extend_from_slice(&indexed_policy_id);
            policies.insert(index_key.as_slice(), &[][..])?;
        }
    }
    write.commit()?;
    let mut guarded_writer =
        InvocationTransaction::begin(&database, [0xf7; 16], principal_id, 9_999, 2, 1024)?;
    let guarded_schema = guarded_writer.load_catalog_table_schema(table_id)?;
    let guarded_policies = guarded_writer
        .load_catalog_policies(principal_id, &guarded_schema)
        .map_err(|error| format!("policy-driving expression failed to load: {error:?}"))?;
    let update_policies = guarded_policies
        .iter()
        .filter(|policy| policy.operation == Operation::Update)
        .collect::<Vec<_>>();
    assert_eq!(update_policies.len(), 1);
    assert_eq!(update_policies[0].using, Some(allow_expression.clone()));
    assert_eq!(update_policies[0].with_check, Some(allow_expression));
    let mut guarded_connection = guarded_writer.open_executor_metadata_connection();
    let (guarded_relation, _) = guarded_connection
        .resolve_relation("app", "metrics")?
        .expect("the SELECT grant keeps the relation available");
    let guarded_scan = guarded_connection.open_relation_scan(&guarded_relation, &[[0x66; 16]])?;
    let guarded_page = guarded_connection.next_relation_scan_page(&guarded_scan, 8)?;
    let guarded_handle = &guarded_page
        .rows
        .first()
        .ok_or("policy-driving test row was not visible")?
        .0;
    assert!(matches!(
        guarded_connection.stage_update(
            guarded_handle,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(vec![0xaa])),
            }],
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));
    let guarded_row = RowRecord::decode(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&key(row_id)[..])?
            .ok_or("policy-driven row was unexpectedly removed")?
            .value(),
    )?;
    assert_eq!(
        decode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: false,
            }],
            1,
            &guarded_row.payload
        )?
        .values,
        [Some(ScalarValue::Bytea(guarded_value))],
        "a write must not mutate a field used by another principal's policy"
    );
    drop(guarded_connection);
    guarded_writer.abort()?;

    Ok(())
}

#[test]
fn authorized_column_description_pages_bind_the_snapshot_and_order()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Insert, AccessTarget::Table(table_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [186; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .describe_authorized_columns_page(table_id, 1, None, &context, &grants, &[])?
        .expect("table write grant permits shape discovery");
    assert_eq!(first.columns.len(), 1);
    assert_eq!(first.columns[0].name, "value");
    assert_eq!(first.columns[0].ordinal, 0);
    let cursor = first.continuation.expect("second column");
    let cursor_bytes = cursor.to_bytes();
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(
        prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&cursor_bytes).is_none()
    );
    let cursor = prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&cursor_bytes)
        .expect("column cursor wire form is fixed width");

    let mut other_invocation =
        InvocationTransaction::begin(&database, [187; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_invocation.describe_authorized_columns_page(
            table_id,
            1,
            Some(cursor.clone()),
            &context,
            &grants,
            &[],
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let second = invocation
        .describe_authorized_columns_page(
            table_id,
            1,
            Some(cursor.clone()),
            &context,
            &grants,
            &[],
        )?
        .expect("same snapshot continues");
    assert_eq!(second.columns.len(), 1);
    assert_eq!(second.columns[0].column_id, [0x67; 16]);
    assert_eq!(second.columns[0].name, "value2");
    assert_eq!(second.columns[0].ordinal, 1);
    assert!(second.continuation.is_none());
    assert!(matches!(
        invocation.describe_authorized_columns_page(
            table_id,
            1,
            Some(cursor),
            &context,
            &grants,
            &[],
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

fn stage_initial_row(database: &Database) -> Result<(), Box<dyn std::error::Error>> {
    commit_rows(
        database,
        [1; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(1)),
            expected_version: None,
            payload: Some(b"snapshot".to_vec()),
        }],
    )?;
    Ok(())
}

#[test]
fn invocation_reads_are_projected_after_snapshot_selection()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Uuid,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
    ];
    let column_ids = [[31; 16], [32; 16], [33; 16]];
    let row_id = id(33);
    let payload = encode_tuple(
        &columns,
        1,
        &[
            Some(ScalarValue::Text("Alice".to_owned())),
            Some(ScalarValue::Uuid([9; 16])),
            Some(ScalarValue::Text(
                "long protected value that must not escape".to_owned(),
            )),
        ],
    )?;
    commit_rows(
        &database,
        [30; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(payload),
        }],
    )?;

    let table_target = AccessTarget::Table(row_id.table);
    let contexts = [[9; 16], [7; 16]].map(|principal_id| AuthorizationContext {
        principal_id,
        role_ids: std::collections::HashSet::from([[40; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, table_target),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: row_id.table,
                    column_id: column_ids[0],
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: row_id.table,
                    column_id: column_ids[1],
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: row_id.table,
                    column_id: column_ids[2],
                },
            ),
        ]),
    });
    let mut grants = column_ids
        .map(|column_id| Grant {
            subject: Subject::Role([40; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: row_id.table,
                column_id,
            },
        })
        .to_vec();
    grants.push(Grant {
        subject: Subject::Role([40; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(row_id.table),
    });
    let policies = [
        Policy {
            subjects: vec![Subject::Role([40; 16])],
            operation: Operation::Select,
            target: table_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([40; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: row_id.table,
                column_id: column_ids[0],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([40; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: row_id.table,
                column_id: column_ids[2],
            },
            effect: Effect::Allow,
            using: Some(Expr::Equal(
                Box::new(Expr::Field(1)),
                Box::new(Expr::CurrentPrincipalId),
            )),
            with_check: None,
        },
    ];

    for (index, context) in contexts.iter().enumerate() {
        let mut invocation = InvocationTransaction::begin(
            &database,
            [34 + index as u8; 16],
            context.principal_id,
            9_999,
            2,
            128,
        )?;
        let output = invocation
            .read_authorized(
                row_id,
                &AuthorizedProjection {
                    columns: &columns,
                    column_ids: &column_ids,
                    schema_version: 1,
                    schema_id: row_id.table,
                    context,
                    grants: &grants,
                    policies: &policies,
                    checks: &[],
                    policy_driving_columns: &[],
                    dependencies: &[],
                },
            )?
            .unwrap();
        assert_eq!(
            output[0],
            SanitizedCell::Value(ScalarValue::Text("Alice".into()))
        );
        if index == 0 {
            assert_eq!(
                output[2],
                SanitizedCell::Value(ScalarValue::Text(
                    "long protected value that must not escape".to_owned()
                ))
            );
        } else {
            assert_eq!(output[2], SanitizedCell::Redacted);
        }
        assert_eq!(output[1], SanitizedCell::Redacted);
        invocation.abort()?;
    }
    Ok(())
}

#[test]
fn authorization_evaluation_error_aborts_and_persists_terminal_status()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(44);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[45; 16]];
    let payload = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Text("protected".to_owned()))],
    )?;
    commit_rows(
        &database,
        [46; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(payload),
        }],
    )?;
    let target = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[47; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, AccessTarget::Table(row_id.table)),
            (Operation::Select, target),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Role([47; 16]),
            operation: Operation::Select,
            target,
        },
        Grant {
            subject: Subject::Role([47; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(row_id.table),
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role([47; 16])],
            operation: Operation::Select,
            target: AccessTarget::Table(row_id.table),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([47; 16])],
            operation: Operation::Select,
            target,
            effect: Effect::Allow,
            using: Some(Expr::Field(99)),
            with_check: None,
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [48; 16], [9; 16], 9_999, 2, 64)?;
    assert!(matches!(
        invocation.read_authorized(
            row_id,
            &AuthorizedProjection {
                columns: &columns,
                column_ids: &column_ids,
                schema_version: 1,
                schema_id: row_id.table,
                context: &context,
                grants: &grants,
                policies: &policies,
                checks: &[],
                policy_driving_columns: &[],
                dependencies: &[],
            }
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    let read = database.begin_read()?;
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[48; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn authorization_context_cannot_change_the_admitted_principal()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[91; 16]];
    let context = AuthorizationContext {
        principal_id: [8; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::new(),
    };
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: [90; 16],
        context: &context,
        grants: &[],
        policies: &[],
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut invocation = InvocationTransaction::begin(&database, [92; 16], [9; 16], 9_999, 2, 64)?;
    assert!(matches!(
        invocation.read_authorized(id(93), &projection),
        Err(TransactionError::InvalidContext)
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    let read = database.begin_read()?;
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[92; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn latest_catalog_authority_change_blocks_a_stale_invocation_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let (principal_key, principal_v1) = principal_catalog_record(1);
    let (object_key, object_value) = principal_object_record_for([7; 16], 1);
    let write = database.begin_write()?;
    write
        .open_table(CATALOG)?
        .insert(&principal_key[..], &principal_v1[..])?;
    write
        .open_table(CATALOG)?
        .insert(&object_key[..], &object_value[..])?;
    write
        .open_table(PRINCIPAL_NAMES)?
        .insert(principal_test_name([7; 16]).as_bytes(), &[7; 16][..])?;
    write.commit()?;

    let mut invocation = InvocationTransaction::begin(&database, [18; 16], [9; 16], 9_999, 2, 64)?;
    let principal = invocation.read_catalog_record(&principal_key)?.unwrap();
    assert_eq!(principal.family_code, 10);
    invocation.stage_put(id(18), b"write authorized at old epoch")?;

    let (_, principal_v2) = principal_catalog_record(2);
    let authority_change = AuditEvent {
        event_id: [20; 16],
        actor_id: Some([9; 16]),
        correlation_id: [21; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![[7; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [19; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: principal_key,
            expected_value: Some(principal_v1),
            payload: Some(principal_v2),
        }],
        &[authority_change],
    )?;

    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert_eq!(commit_sequence(&database)?, 1);
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(18))[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[18; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn racing_catalog_authority_change_linearizes_before_or_after_pending_write()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let principal_id = [0xA1; 16];
    let (principal_key, principal_v1) =
        principal_catalog_record_for(principal_id, 1, b"race_user", true, 1);
    let (object_key, object_value) = principal_object_record_for(principal_id, 1);
    let seed = database.begin_write()?;
    seed.open_table(CATALOG)?
        .insert(principal_key.as_slice(), principal_v1.as_slice())?;
    seed.open_table(CATALOG)?
        .insert(object_key.as_slice(), object_value.as_slice())?;
    seed.open_table(PRINCIPAL_NAMES)?.insert(
        principal_test_name(principal_id).as_bytes(),
        &principal_id[..],
    )?;
    seed.commit()?;

    let (principal_key, principal_v2) =
        principal_catalog_record_for(principal_id, 1, b"race_user", true, 2);
    let row_id = id(0xA2);
    let invocation_id = [0xA3; 16];
    let authority_invocation_id = [0xA4; 16];
    let barrier = Arc::new(Barrier::new(3));
    let (write_result, authority_result) = std::thread::scope(|scope| {
        let write_barrier = barrier.clone();
        let write_database = &database;
        let write = scope.spawn(move || {
            let mut invocation = InvocationTransaction::begin(
                write_database,
                invocation_id,
                principal_id,
                9_999,
                2,
                64,
            )?;
            assert!(
                invocation
                    .load_catalog_role_closure(principal_id)?
                    .is_empty()
            );
            invocation.stage_put(row_id, b"authorized before race")?;
            write_barrier.wait();
            invocation.commit()
        });

        let authority_barrier = barrier.clone();
        let authority_database = &database;
        let authority_key = principal_key.clone();
        let authority_expected = principal_v1.clone();
        let authority_payload = principal_v2.clone();
        let authority = scope.spawn(move || {
            authority_barrier.wait();
            let event = AuditEvent {
                event_id: authority_invocation_id,
                actor_id: Some([9; 16]),
                correlation_id: [0xA5; 16],
                occurred_at_us: 1_700_000_000_000_000,
                action: AuditAction::AuthorityChange,
                result: AuditResult::Succeeded,
                target_ids: vec![principal_id],
            };
            commit_catalog_and_audit(
                authority_database,
                authority_invocation_id,
                [9; 16],
                9_999,
                &[CatalogWrite {
                    key: authority_key,
                    expected_value: Some(authority_expected),
                    payload: Some(authority_payload),
                }],
                &[event],
            )
        });

        barrier.wait();
        (write.join().unwrap(), authority.join().unwrap())
    });

    let authority_sequence = authority_result?;
    let write_sequence = match write_result {
        Ok(sequence) => Some(sequence),
        Err(TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    let read = database.begin_read()?;
    let row_exists = read.open_table(ROWS)?.get(&key(row_id)[..])?.is_some();
    let write_outcome = InvocationOutcome::decode(
        read.open_table(prdb_engine::schema::OUTCOMES)?
            .get(&invocation_id[..])?
            .ok_or("invocation outcome missing")?
            .value(),
    )?;
    assert_eq!(row_exists, write_sequence.is_some());
    assert_eq!(
        write_outcome.status,
        if write_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(commit_sequence(&database)?, authority_sequence);
    assert!(
        write_sequence.is_none() || write_sequence.unwrap() < authority_sequence,
        "a pending write may commit only before the concurrent authority change"
    );
    Ok(())
}

#[test]
fn catalog_absence_guard_blocks_a_phantom_authority_record()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let mut invocation = InvocationTransaction::begin(&database, [22; 16], [9; 16], 9_999, 2, 64)?;
    let (principal_key, principal_v1) = principal_catalog_record(1);
    assert!(invocation.read_catalog_record(&principal_key)?.is_none());
    invocation.stage_put(id(22), b"write based on absent authority record")?;

    let authority_change = AuditEvent {
        event_id: [23; 16],
        actor_id: Some([9; 16]),
        correlation_id: [24; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![[7; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [25; 16],
        [9; 16],
        9_999,
        &[
            CatalogWrite {
                key: principal_key,
                expected_value: None,
                payload: Some(principal_v1),
            },
            {
                let (key, payload) = principal_object_record_for([7; 16], 1);
                CatalogWrite {
                    key,
                    expected_value: None,
                    payload: Some(payload),
                }
            },
        ],
        &[authority_change],
    )?;

    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert_eq!(commit_sequence(&database)?, 1);
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(22))[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[22; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn catalog_expression_reads_are_decoded_and_guarded_at_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let expression_id = [100; 16];
    let expression_key = {
        let mut key = 7u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&expression_id);
        key
    };
    let original =
        expression_catalog_payload(expression_id, &Expr::Literal(Some(ScalarValue::Integer(1))))?;
    let (object_key, object_payload) = object_catalog_record_for(expression_id, 5);
    let ddl_audit = |event_id, target_id| AuditEvent {
        event_id: [event_id; 16],
        actor_id: Some([9; 16]),
        correlation_id: [event_id.wrapping_add(1); 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[target_id; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [101; 16],
        [9; 16],
        9_999,
        &[
            CatalogWrite {
                key: expression_key.clone(),
                expected_value: None,
                payload: Some(original.clone()),
            },
            CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            },
        ],
        &[ddl_audit(102, 100)],
    )?;

    let mut invocation = InvocationTransaction::begin(&database, [103; 16], [9; 16], 9_999, 1, 64)?;
    let expression = invocation.read_catalog_expression(expression_id)?.unwrap();
    assert_eq!(
        expression.purpose,
        prdb_engine::expression::ExpressionPurpose::Default
    );
    assert_eq!(
        expression.expression,
        Expr::Literal(Some(ScalarValue::Integer(1)))
    );
    invocation.stage_put(id(104), b"write based on pinned expression")?;

    let replacement =
        expression_catalog_payload(expression_id, &Expr::Literal(Some(ScalarValue::Integer(2))))?;
    commit_catalog_and_audit(
        &database,
        [105; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: expression_key,
            expected_value: Some(original),
            payload: Some(replacement),
        }],
        &[ddl_audit(106, 100)],
    )?;
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(104))[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[103; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

fn commit_sequence(database: &Database) -> Result<u64, Box<dyn std::error::Error>> {
    let transaction = database.begin_read()?;
    let bytes = transaction
        .open_table(ENGINE_META)?
        .get(&b"format"[..])?
        .unwrap()
        .value()
        .to_vec();
    Ok(EngineMetadata::decode(&bytes)?.commit_sequence)
}

#[test]
fn invocation_sees_own_writes_and_commits_against_its_pinned_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;

    let mut invocation = InvocationTransaction::begin(&database, [3; 16], [9; 16], 9_999, 4, 64)?;
    assert_eq!(invocation.read(id(1))?, Some(b"snapshot".to_vec()));
    invocation.stage_put(id(1), b"private")?;
    assert_eq!(invocation.read(id(1))?, Some(b"private".to_vec()));
    invocation.stage_put(id(2), b"inserted")?;
    assert_eq!(invocation.read(id(2))?, Some(b"inserted".to_vec()));

    // A different transaction can commit a disjoint row while this snapshot remains pinned.
    commit_rows(
        &database,
        [2; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(3)),
            expected_version: None,
            payload: Some(b"other".to_vec()),
        }],
    )?;
    assert_eq!(invocation.read(id(1))?, Some(b"private".to_vec()));
    assert_eq!(invocation.commit()?, 3);

    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    let updated = RowRecord::decode(rows.get(&key(id(1))[..])?.unwrap().value())?;
    let inserted = RowRecord::decode(rows.get(&key(id(2))[..])?.unwrap().value())?;
    let disjoint = RowRecord::decode(rows.get(&key(id(3))[..])?.unwrap().value())?;
    assert_eq!(
        (updated.row_version, updated.payload.as_slice()),
        (3, &b"private"[..])
    );
    assert_eq!(
        (inserted.row_version, inserted.payload.as_slice()),
        (3, &b"inserted"[..])
    );
    assert_eq!(
        (disjoint.row_version, disjoint.payload.as_slice()),
        (2, &b"other"[..])
    );
    Ok(())
}

#[test]
fn invocation_snapshot_hides_private_writes_and_keeps_repeated_reads_stable()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;

    let mut reader = InvocationTransaction::begin(&database, [20; 16], [9; 16], 9_999, 4, 64)?;
    let mut writer = InvocationTransaction::begin(&database, [21; 16], [9; 16], 9_999, 4, 64)?;
    assert_eq!(reader.read(id(1))?, Some(b"snapshot".to_vec()));
    writer.stage_put(id(1), b"writer-private")?;
    writer.stage_put(id(2), b"insert-private")?;
    assert_eq!(writer.read(id(1))?, Some(b"writer-private".to_vec()));
    assert_eq!(writer.read(id(2))?, Some(b"insert-private".to_vec()));

    // Another invocation sees neither the writer's private update nor insert.
    assert_eq!(reader.read(id(1))?, Some(b"snapshot".to_vec()));
    assert_eq!(reader.read(id(2))?, None);

    assert_eq!(writer.commit()?, 2);

    // The already admitted reader retains the old snapshot after publication.
    assert_eq!(reader.read(id(1))?, Some(b"snapshot".to_vec()));
    assert_eq!(reader.read(id(2))?, None);
    reader.abort()?;

    // A new invocation admitted after commit observes both published writes.
    let mut observer = InvocationTransaction::begin(&database, [22; 16], [9; 16], 9_999, 4, 64)?;
    assert_eq!(observer.read(id(1))?, Some(b"writer-private".to_vec()));
    assert_eq!(observer.read(id(2))?, Some(b"insert-private".to_vec()));
    observer.abort()?;

    let read = database.begin_read()?;
    let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
    for (invocation_id, expected_status) in [
        ([20; 16], OutcomeStatus::Aborted),
        ([21; 16], OutcomeStatus::Committed),
        ([22; 16], OutcomeStatus::Aborted),
    ] {
        assert_eq!(
            InvocationOutcome::decode(outcomes.get(&invocation_id[..])?.unwrap().value())?.status,
            expected_status
        );
    }
    Ok(())
}

#[test]
fn retained_invocation_snapshot_survives_a_durable_write_burst()
-> Result<(), Box<dyn std::error::Error>> {
    const VALUE_BYTES: usize = 32 * 1024;
    const WRITE_COUNT: u8 = 64;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;
    let mut reader =
        InvocationTransaction::begin(&database, [0x30; 16], [9; 16], 9_999, 4, 32 * 1024)?;
    let original_value = reader.read(id(1))?.ok_or("initial row missing")?;

    let mut latest_value = Vec::new();
    for version in 0..WRITE_COUNT {
        latest_value = vec![version; VALUE_BYTES];
        let mut writer = InvocationTransaction::begin(
            &database,
            [0x40u8.wrapping_add(version); 16],
            [9; 16],
            9_999,
            4,
            32 * 1024,
        )?;
        writer.stage_put(id(1), &latest_value)?;
        writer.commit()?;
        assert_eq!(
            reader.read(id(1))?,
            Some(original_value.clone()),
            "the pinned invocation must retain its original row after write {version}"
        );
    }
    reader.abort()?;

    let mut observer =
        InvocationTransaction::begin(&database, [0x90; 16], [9; 16], 9_999, 4, 32 * 1024)?;
    assert_eq!(observer.read(id(1))?, Some(latest_value));
    observer.abort()?;
    Ok(())
}

#[test]
fn retained_invocation_snapshot_survives_concurrent_writer_pressure()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITER_COUNT: usize = 4;
    const WRITES_PER_WRITER: u8 = 64;
    const VALUE_BYTES: usize = 32 * 1024;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_ids = (1..=WRITER_COUNT as u8).map(id).collect::<Vec<_>>();
    let original_values = (0..WRITER_COUNT)
        .map(|writer| vec![0x10 + writer as u8; VALUE_BYTES])
        .collect::<Vec<_>>();
    let mut seed =
        InvocationTransaction::begin(&database, [0x20; 16], [9; 16], 9_999, 8, 512 * 1024)?;
    for (row_id, value) in row_ids.iter().copied().zip(&original_values) {
        seed.stage_put(row_id, value)?;
    }
    seed.commit()?;

    let mut reader =
        InvocationTransaction::begin(&database, [0x30; 16], [9; 16], 9_999, 8, 512 * 1024)?;
    for (row_id, value) in row_ids.iter().copied().zip(&original_values) {
        assert_eq!(reader.read(row_id)?, Some(value.clone()));
    }

    let start_round = Arc::new(Barrier::new(WRITER_COUNT + 1));
    let finish_round = Arc::new(Barrier::new(WRITER_COUNT + 1));
    let database_ref = &database;
    let (reader_succeeded, writers_succeeded) = std::thread::scope(|scope| {
        let mut writers = Vec::with_capacity(WRITER_COUNT);
        for (writer_index, row_id) in row_ids.iter().copied().enumerate() {
            let start_round = Arc::clone(&start_round);
            let finish_round = Arc::clone(&finish_round);
            let database = database_ref;
            writers.push(scope.spawn(move || {
                let mut succeeded = true;
                for version in 0..WRITES_PER_WRITER {
                    start_round.wait();
                    if succeeded {
                        let result = (|| {
                            let value = vec![
                                0x40u8.wrapping_add(
                                    (writer_index as u8)
                                        .wrapping_mul(WRITES_PER_WRITER)
                                        .wrapping_add(version),
                                );
                                VALUE_BYTES
                            ];
                            let mut invocation_id = [0; 16];
                            invocation_id[0] = 0x80 + writer_index as u8;
                            invocation_id[1] = version;
                            let mut writer = InvocationTransaction::begin(
                                database,
                                invocation_id,
                                [9; 16],
                                9_999,
                                4,
                                VALUE_BYTES * 2,
                            )?;
                            writer.stage_put(row_id, &value)?;
                            writer.commit()?;
                            Ok::<(), Box<dyn std::error::Error>>(())
                        })();
                        succeeded = result.is_ok();
                    }
                    finish_round.wait();
                }
                succeeded
            }));
        }
        let mut reader_succeeded = true;
        for _ in 0..WRITES_PER_WRITER {
            start_round.wait();
            for (row_id, value) in row_ids.iter().copied().zip(&original_values) {
                match reader.read(row_id) {
                    Ok(actual) if actual == Some(value.clone()) => {}
                    _ => reader_succeeded = false,
                }
            }
            finish_round.wait();
        }
        let writers_succeeded = writers
            .into_iter()
            .all(|writer| writer.join().unwrap_or(false));
        (reader_succeeded, writers_succeeded)
    });
    assert!(writers_succeeded, "a pressure writer failed");
    assert!(
        reader_succeeded,
        "the retained invocation snapshot changed during writer rounds"
    );

    for (row_id, value) in row_ids.iter().copied().zip(&original_values) {
        assert_eq!(
            reader.read(row_id)?,
            Some(value.clone()),
            "the retained invocation snapshot changed under concurrent commits"
        );
    }
    reader.abort()?;

    let mut observer =
        InvocationTransaction::begin(&database, [0xd0; 16], [9; 16], 9_999, 8, 512 * 1024)?;
    for (writer_index, row_id) in row_ids.iter().copied().enumerate() {
        let expected_tag = 0x40u8.wrapping_add(
            (writer_index as u8)
                .wrapping_mul(WRITES_PER_WRITER)
                .wrapping_add(WRITES_PER_WRITER - 1),
        );
        let expected = vec![expected_tag; VALUE_BYTES];
        assert_eq!(observer.read(row_id)?, Some(expected));
    }
    observer.abort()?;
    Ok(())
}

#[test]
fn managed_cancellation_releases_snapshot_admission_during_write_pressure()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITE_COUNT: u8 = 32;
    const VALUE_BYTES: usize = 32 * 1024;

    let fixture = Fixture::new();
    let data_directory = fixture.0.join("managed-cancellation");
    let mut bootstrap =
        prdb_engine::bootstrap::initialize(&data_directory, "cancel_admin", i64::MAX)?;
    let admin_user_id = bootstrap.admin_user_id();
    let mut secret = bootstrap.take_secret().ok_or("bootstrap secret missing")?;
    use zeroize::Zeroize;
    secret.zeroize();
    let instance = bootstrap.acknowledge()?;
    let database = instance.database();
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let retention_deadline_us = now + 60_000_000;
    let storage = prdb_engine::storage_manager::StorageManager::with_config(
        database,
        prdb_engine::storage_manager::StorageManagerConfig {
            max_active_invocations: 3,
            max_active_analytical_invocations: 1,
            ..Default::default()
        },
    )?;
    stage_initial_row(database)?;
    let mut retained =
        storage.begin_invocation([0x30; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    let original_value = retained.read(id(1))?;
    let mut cancelled =
        storage.begin_invocation([0x31; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    cancelled.stage_put(id(2), b"cancelled private write")?;
    let mut staged_writer =
        storage.begin_invocation([0x32; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    staged_writer.stage_put(id(1), b"staged writer value")?;
    assert!(matches!(
        storage.begin_invocation([0x33; 16], admin_user_id, retention_deadline_us, 4, 64),
        Err(TransactionError::AdmissionLimitExceeded)
    ));

    let mut executor = cancelled.open_executor_metadata_connection();
    assert!(!executor.check_cancellation()?);
    let stop_monitoring = Arc::new(AtomicBool::new(false));
    let pages_during_cancellation = std::thread::scope(|scope| {
        let stop_monitoring_thread = Arc::clone(&stop_monitoring);
        let storage = &storage;
        let (monitoring_ready_tx, monitoring_ready_rx) = std::sync::mpsc::channel();
        let monitor = scope.spawn(move || {
            let mut pages = 0usize;
            let first_page = storage
                .list_runtime_invocations(
                    admin_user_id,
                    now + 1,
                    prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
                    None,
                )
                .expect("first admin runtime page during cancellation");
            assert!(
                first_page
                    .invocations
                    .iter()
                    .any(|invocation| invocation.invocation_id == [0x31; 16])
            );
            pages += 1;
            monitoring_ready_tx
                .send(())
                .expect("signal first completed runtime page");
            while !stop_monitoring_thread.load(Ordering::Acquire) {
                let page = storage
                    .list_runtime_invocations(
                        admin_user_id,
                        now + 1,
                        prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
                        None,
                    )
                    .expect("admin runtime page concurrent with cancellation");
                assert!(
                    page.invocations
                        .iter()
                        .any(|invocation| invocation.invocation_id == [0x31; 16])
                );
                pages += 1;
                std::thread::yield_now();
            }
            pages
        });
        monitoring_ready_rx
            .recv()
            .expect("runtime page finished before cancellation");
        let cancellation = storage.cancel_owned_invocation(admin_user_id, [0x31; 16], now + 1);
        stop_monitoring.store(true, Ordering::Release);
        let pages = monitor
            .join()
            .expect("runtime monitoring thread must finish");
        (cancellation, pages)
    });
    assert_eq!(
        pages_during_cancellation.0?,
        prdb_engine::storage_manager::OwnerCancellationResult::Accepted
    );
    assert!(
        pages_during_cancellation.1 > 1,
        "monitoring must continue after cancellation begins"
    );
    assert!(executor.check_cancellation()?);
    drop(executor);

    // The cancellation poll durably aborts and drops its snapshot permit. The
    // other reader and a staged writer still occupy two of the three slots.
    let mut replacement =
        storage.begin_invocation([0x33; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    replacement.abort()?;
    assert_eq!(
        storage
            .lookup_invocation_outcome([0x31; 16], admin_user_id, now + 2)?
            .ok_or("cancelled outcome missing")?
            .status,
        OutcomeStatus::Aborted
    );
    staged_writer.commit()?;

    let completed = AtomicBool::new(false);
    let start = Arc::new(Barrier::new(2));
    let first_commit = Arc::new(Barrier::new(2));
    let samples = std::thread::scope(|scope| {
        let start_writer = Arc::clone(&start);
        let first_commit_writer = Arc::clone(&first_commit);
        let storage = &storage;
        let completed = &completed;
        let writer = scope.spawn(move || {
            start_writer.wait();
            for sequence in 0..WRITE_COUNT {
                let value = vec![0x40u8.wrapping_add(sequence); VALUE_BYTES];
                let mut invocation_id = [0; 16];
                invocation_id[0] = 0x70;
                invocation_id[1] = sequence;
                let mut transaction = storage
                    .begin_invocation(
                        invocation_id,
                        admin_user_id,
                        retention_deadline_us,
                        4,
                        VALUE_BYTES * 2,
                    )
                    .expect("admit pressure writer");
                transaction
                    .stage_put(id(1), &value)
                    .expect("stage pressure write");
                if sequence == 0 {
                    first_commit_writer.wait();
                }
                transaction.commit().expect("commit pressure write");
            }
            completed.store(true, Ordering::Release);
        });
        start.wait();
        assert_eq!(
            retained.read(id(1))?,
            original_value.clone(),
            "the retained snapshot changed before the first pressure commit"
        );
        let mut samples = 1;
        first_commit.wait();
        let first_monitoring_page = storage.list_runtime_invocations(
            admin_user_id,
            now + 3,
            prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
            None,
        )?;
        assert!(
            first_monitoring_page
                .invocations
                .iter()
                .any(|invocation| invocation.invocation_id == [0x30; 16])
        );
        let mut monitoring_pages = 1;
        while !completed.load(Ordering::Acquire) && !writer.is_finished() {
            assert_eq!(
                retained.read(id(1))?,
                original_value.clone(),
                "cancellation or concurrent commits changed the retained snapshot"
            );
            let page = storage.list_runtime_invocations(
                admin_user_id,
                now + i64::from(samples + 3),
                prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
                None,
            )?;
            assert!(
                page.invocations
                    .iter()
                    .any(|invocation| invocation.invocation_id == [0x30; 16])
            );
            monitoring_pages += 1;
            samples += 1;
            std::thread::yield_now();
        }
        writer.join().expect("pressure writer thread");
        Ok::<_, Box<dyn std::error::Error>>((samples, monitoring_pages))
    })?;
    assert!(samples.1 > 0, "monitoring did no work during the writes");
    assert!(
        samples.0 > 0,
        "the retained reader did no work during the writes"
    );

    assert_eq!(retained.read(id(1))?, original_value);
    retained.abort()?;
    let mut observer =
        storage.begin_invocation([0x34; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    assert_eq!(
        observer.read(id(1))?,
        Some(vec![0x40u8.wrapping_add(WRITE_COUNT - 1); VALUE_BYTES])
    );
    assert_eq!(observer.read(id(2))?, None);
    observer.abort()?;
    drop(storage);
    drop(instance);
    Ok(())
}

#[test]
fn authorized_row_scan_pages_over_the_pinned_snapshot_and_private_overlay()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_id = [0x22; 16];
    let column_ids = [column_id];
    let schema_id = [0x23; 16];
    let table_id = id(0).table;
    let first = encode_tuple(&columns, 1, &[Some(ScalarValue::Text("first".to_owned()))])?;
    let third = encode_tuple(&columns, 1, &[Some(ScalarValue::Text("third".to_owned()))])?;
    let mut seed = InvocationTransaction::begin(&database, [0x31; 16], [9; 16], 9_999, 8, 512)?;
    seed.stage_put(id(1), &first)?;
    seed.stage_put(id(3), &third)?;
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        },
    ];
    let allow = Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_allow = Policy {
        target: AccessTarget::Column {
            table_id,
            column_id,
        },
        ..allow.clone()
    };
    let policies = [allow, cell_allow];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };

    let mut invocation =
        InvocationTransaction::begin(&database, [0x32; 16], [9; 16], 9_999, 8, 512)?;
    invocation.stage_delete(id(1))?;
    invocation.stage_put(
        id(2),
        &encode_tuple(&columns, 1, &[Some(ScalarValue::Text("second".to_owned()))])?,
    )?;
    invocation.stage_put(
        id(3),
        &encode_tuple(
            &columns,
            1,
            &[Some(ScalarValue::Text("changed".to_owned()))],
        )?,
    )?;
    commit_rows(
        &database,
        [0x33; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(4)),
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text("concurrent".to_owned()))],
            )?),
        }],
    )?;

    let first_page = invocation.scan_authorized(table_id, None, 2, &projection)?;
    assert!(first_page.continuation.is_some());
    assert_eq!(first_page.rows.len(), 1);
    assert_eq!(first_page.rows[0].0, id(2));
    assert_eq!(
        first_page.rows[0].1,
        vec![SanitizedCell::Value(ScalarValue::Text("second".to_owned()))]
    );

    let second_page =
        invocation.scan_authorized(table_id, first_page.continuation.as_ref(), 2, &projection)?;
    assert!(second_page.continuation.is_none());
    assert_eq!(second_page.rows.len(), 1);
    assert_eq!(second_page.rows[0].0, id(3));
    assert_eq!(
        second_page.rows[0].1,
        vec![SanitizedCell::Value(ScalarValue::Text(
            "changed".to_owned()
        ))]
    );
    invocation.abort()?;

    let mut active_cursor =
        InvocationTransaction::begin(&database, [0x35; 16], [9; 16], 9_999, 8, 512)?;
    let page = active_cursor.scan_authorized(table_id, None, 1, &projection)?;
    assert!(page.continuation.is_some());
    assert!(matches!(
        active_cursor.stage_put(id(5), &first),
        Err(TransactionError::ScanStillActive)
    ));

    let mut unclosed_cursor =
        InvocationTransaction::begin(&database, [0x36; 16], [9; 16], 9_999, 8, 512)?;
    let page = unclosed_cursor.scan_authorized(table_id, None, 1, &projection)?;
    assert!(page.continuation.is_some());
    assert!(matches!(
        unclosed_cursor.commit(),
        Err(TransactionError::ScanStillActive)
    ));

    let mut changed_authority =
        InvocationTransaction::begin(&database, [0x37; 16], [9; 16], 9_999, 8, 512)?;
    let page = changed_authority.scan_authorized(table_id, None, 1, &projection)?;
    let no_grants: [Grant; 0] = [];
    let changed_projection = AuthorizedProjection {
        grants: &no_grants,
        ..projection
    };
    assert!(matches!(
        changed_authority.scan_authorized(
            table_id,
            page.continuation.as_ref(),
            1,
            &changed_projection
        ),
        Err(TransactionError::InvalidScanRequest)
    ));

    Ok(())
}

#[test]
fn managed_scan_work_budget_is_cumulative_and_aborts_private_writes()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{StorageManager, StorageManagerConfig};
    use zeroize::Zeroize;

    let fixture = Fixture::new();
    let data_directory = fixture.0.join("data");
    let mut bootstrap =
        prdb_engine::bootstrap::initialize(&data_directory, "scan_admin", i64::MAX)?;
    let admin_user_id = bootstrap.admin_user_id();
    let mut secret = bootstrap.take_secret().ok_or("bootstrap secret missing")?;
    secret.zeroize();
    let instance = bootstrap.acknowledge()?;
    let database = instance.database();
    let storage = StorageManager::with_config(
        database,
        StorageManagerConfig {
            max_invocation_scan_work: 2,
            ..StorageManagerConfig::default()
        },
    )?;
    let metadata_storage = StorageManager::with_config(
        database,
        StorageManagerConfig {
            max_invocation_scan_work: 1,
            ..StorageManagerConfig::default()
        },
    )?;

    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_id = [0x22; 16];
    let column_ids = [column_id];
    let schema_id = [0x23; 16];
    let table_id = id(0).table;
    let mut seed = InvocationTransaction::begin(database, [0x31; 16], [9; 16], i64::MAX, 4, 512)?;
    for row_id in 1..=3 {
        let payload = encode_tuple(
            &columns,
            1,
            &[Some(ScalarValue::Text(format!("row-{row_id}")))],
        )?;
        seed.stage_put(id(row_id), &payload)?;
    }
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        },
    ];
    let table_policy = Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_policy = Policy {
        target: AccessTarget::Column {
            table_id,
            column_id,
        },
        ..table_policy.clone()
    };
    let policies = [table_policy, cell_policy];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };

    let invocation_id = [0x38; 16];
    let mut invocation = storage.begin_invocation(invocation_id, [9; 16], i64::MAX, 4, 512)?;
    let staged_id = prdb_engine::overlay::RowId {
        table: [0x11; 16],
        row: [0x12; 16],
    };
    invocation.stage_put(staged_id, b"discard after the scan budget is exceeded")?;
    let first_page = invocation.scan_authorized(table_id, None, 1, &projection)?;
    assert!(first_page.continuation.is_some());
    let second_page =
        invocation.scan_authorized(table_id, first_page.continuation.as_ref(), 1, &projection)?;
    assert!(second_page.continuation.is_some());
    assert!(matches!(
        invocation.scan_authorized(table_id, second_page.continuation.as_ref(), 1, &projection,),
        Err(TransactionError::ScanBudgetExceeded)
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));

    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(staged_id)[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&invocation_id[..])?
                .ok_or("scan budget failure did not persist ABORTED")?
                .value(),
        )?
        .status,
        OutcomeStatus::Aborted
    );

    let metadata_invocation_id = [0x39; 16];
    let metadata_row = prdb_engine::overlay::RowId {
        table: [0x41; 16],
        row: [0x42; 16],
    };
    let mut metadata_invocation = metadata_storage.begin_invocation(
        metadata_invocation_id,
        admin_user_id,
        i64::MAX,
        4,
        512,
    )?;
    metadata_invocation.stage_put(metadata_row, b"discard after catalog scan budget")?;
    assert!(matches!(
        metadata_invocation.scan_information_schema_schemata(1, None),
        Err(TransactionError::ScanBudgetExceeded)
    ));
    assert!(matches!(
        metadata_invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    let read = database.begin_read()?;
    assert!(
        read.open_table(ROWS)?
            .get(&key(metadata_row)[..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&metadata_invocation_id[..])?
                .ok_or("catalog scan budget failure did not persist ABORTED")?
                .value(),
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn authorized_scan_estimate_counts_sanitized_cells_and_private_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
    ];
    let column_ids = [[0x22; 16], [0x24; 16]];
    let schema_id = [0x23; 16];
    let table_id = id(0).table;
    let row = |visible: Option<&str>, hidden: &str| {
        encode_tuple(
            &columns,
            1,
            &[
                visible.map(|value| ScalarValue::Text(value.to_owned())),
                Some(ScalarValue::Text(hidden.to_owned())),
            ],
        )
        .unwrap()
    };
    let mut seed = InvocationTransaction::begin(&database, [0x31; 16], [9; 16], 9_999, 8, 512)?;
    seed.stage_put(id(1), &row(Some("first"), "secret-one"))?;
    seed.stage_put(id(2), &row(None, "secret-two"))?;
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: column_ids[0],
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[0],
            },
        },
    ];
    let table_allow = Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let readable_cell_allow = Policy {
        target: AccessTarget::Column {
            table_id,
            column_id: column_ids[0],
        },
        ..table_allow.clone()
    };
    let policies = [table_allow, readable_cell_allow];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };

    let predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
            prdb_engine::optimizer_binding::BoundColumn {
                column_id: column_ids[0],
                ordinal: 0,
                scalar_type: ScalarType::Text,
                nullable: true,
            },
        )),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
            "first".to_owned(),
        )),
    };

    let mut observer = InvocationTransaction::begin(&database, [0x32; 16], [9; 16], 9_999, 8, 512)?;
    let mut writer = InvocationTransaction::begin(&database, [0x33; 16], [9; 16], 9_999, 8, 512)?;
    assert_eq!(
        observer.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(2)
    );
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(table_id, &projection, &predicate, 4,)?,
        Some((2, 1))
    );
    let hidden_predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
            prdb_engine::optimizer_binding::BoundColumn {
                column_id: column_ids[1],
                ordinal: 1,
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        )),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
            "secret-one".to_owned(),
        )),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &hidden_predicate,
            128,
        )?,
        None
    );
    let mut boundary_probe =
        InvocationTransaction::begin(&database, [0x3a; 16], [9; 16], 9_999, 256, 32_768)?;
    boundary_probe.stage_put(id(3), &row(Some("third"), "secret-three"))?;
    boundary_probe.stage_put(id(4), &row(Some("fourth"), "secret-four"))?;
    assert_eq!(
        boundary_probe.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(4)
    );
    boundary_probe.stage_put(id(5), &row(Some("fifth"), "secret-five"))?;
    assert_eq!(
        boundary_probe.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        None
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            4,
        )?,
        None
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            128,
        )?,
        Some((5, 1)),
        "predicate estimates should cover exact populations larger than four rows"
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            129,
        )?,
        Some((5, 1)),
        "predicate estimate limit must cover more than 128 rows when population is smaller"
    );
    for row_number in 6..=129 {
        boundary_probe.stage_put(
            id(row_number),
            &row(Some("other"), &format!("secret-{row_number}")),
        )?;
    }
    assert_eq!(
        boundary_probe.estimate_authorized_population_at_most(
            table_id,
            &projection,
            prdb_engine::statistics::MAX_ESTIMATE_CANDIDATES as u64,
        )?,
        Some(129)
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            128,
        )?,
        None,
        "populations above the caller's bound must remain unknown"
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            prdb_engine::statistics::MAX_ESTIMATE_CANDIDATES as u64,
        )?,
        Some((129, 1)),
        "bounded predicate estimates should cover exact authorized populations above 128 rows"
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            prdb_engine::statistics::MAX_ESTIMATE_CANDIDATES as u64 + 1,
        )?,
        None,
        "predicate estimate limit must remain explicitly bounded"
    );
    boundary_probe.abort()?;
    assert_eq!(
        observer.estimate_authorized_scan(table_id, &projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Exact(
            prdb_engine::statistics::AuthorizedScanSummary {
                visible_rows: 2,
                columns: vec![
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 1,
                        nulls: 1,
                        redacted: 0,
                    },
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 0,
                        nulls: 0,
                        redacted: 2,
                    },
                ],
            }
        )
    );

    writer.stage_put(id(3), &row(Some("third"), "secret-three"))?;
    assert_eq!(
        writer.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(3)
    );
    assert_eq!(
        writer.estimate_authorized_predicate_at_most(table_id, &projection, &predicate, 4,)?,
        Some((3, 1))
    );
    let private_summary = writer.estimate_authorized_scan(table_id, &projection)?;
    assert_eq!(
        private_summary,
        prdb_engine::statistics::AuthorizedScanEstimate::Exact(
            prdb_engine::statistics::AuthorizedScanSummary {
                visible_rows: 3,
                columns: vec![
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 2,
                        nulls: 1,
                        redacted: 0,
                    },
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 0,
                        nulls: 0,
                        redacted: 3,
                    },
                ],
            }
        )
    );
    writer.stage_put(id(1), &row(Some("first"), "replacement-hidden-secret"))?;
    assert_eq!(
        writer.estimate_authorized_predicate_at_most(table_id, &projection, &predicate, 4)?,
        Some((3, 1)),
        "changing only a redacted cell must not change predicate estimate counts"
    );
    assert_eq!(
        writer.estimate_authorized_scan(table_id, &projection)?,
        private_summary,
        "changing only a redacted cell must not change released column populations"
    );
    writer.stage_put(id(1), &row(Some("changed"), "changed-secret"))?;
    assert_eq!(
        writer.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(3)
    );
    assert_eq!(
        writer.estimate_authorized_scan(table_id, &projection)?,
        private_summary
    );
    let changed_predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
            prdb_engine::optimizer_binding::BoundColumn {
                column_id: column_ids[0],
                ordinal: 0,
                scalar_type: ScalarType::Text,
                nullable: true,
            },
        )),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
            "changed".to_owned(),
        )),
    };
    assert_eq!(
        writer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &changed_predicate,
            4,
        )?,
        Some((3, 1))
    );
    writer.stage_delete(id(2))?;
    assert_eq!(
        writer.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(2)
    );
    assert_eq!(
        writer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &changed_predicate,
            4,
        )?,
        Some((2, 1))
    );
    assert_eq!(
        writer.estimate_authorized_scan(table_id, &projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Exact(
            prdb_engine::statistics::AuthorizedScanSummary {
                visible_rows: 2,
                columns: vec![
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 2,
                        nulls: 0,
                        redacted: 0,
                    },
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 0,
                        nulls: 0,
                        redacted: 2,
                    },
                ],
            }
        )
    );
    assert_eq!(
        observer.estimate_authorized_scan(table_id, &projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Exact(
            prdb_engine::statistics::AuthorizedScanSummary {
                visible_rows: 2,
                columns: vec![
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 1,
                        nulls: 1,
                        redacted: 0,
                    },
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 0,
                        nulls: 0,
                        redacted: 2,
                    },
                ],
            }
        )
    );

    let row_filter = Policy {
        using: Some(Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Text("first".to_owned())))),
        )),
        ..policies[0].clone()
    };
    let filtered_policies = [row_filter, policies[1].clone()];
    let filtered_projection = AuthorizedProjection {
        policies: &filtered_policies,
        ..projection
    };
    let mut hidden_value_a =
        InvocationTransaction::begin(&database, [0x36; 16], [9; 16], 9_999, 32, 2_048)?;
    let mut hidden_value_b =
        InvocationTransaction::begin(&database, [0x37; 16], [9; 16], 9_999, 32, 2_048)?;
    hidden_value_a.stage_put(id(3), &row(Some("not-visible"), "private-a"))?;
    hidden_value_b.stage_put(id(3), &row(Some("not-visible"), "private-b"))?;
    let hidden_estimate_a =
        hidden_value_a.estimate_authorized_scan(table_id, &filtered_projection)?;
    let hidden_estimate_b =
        hidden_value_b.estimate_authorized_scan(table_id, &filtered_projection)?;
    assert_eq!(
        hidden_estimate_a,
        prdb_engine::statistics::AuthorizedScanEstimate::Unknown
    );
    assert_eq!(hidden_estimate_a, hidden_estimate_b);
    assert_eq!(
        hidden_value_a.estimate_authorized_predicate_at_most(
            table_id,
            &filtered_projection,
            &predicate,
            128,
        )?,
        None,
        "value-dependent row policy must suppress predicate estimates"
    );
    assert_eq!(
        hidden_value_b.estimate_authorized_predicate_at_most(
            table_id,
            &filtered_projection,
            &predicate,
            128,
        )?,
        None
    );
    assert_eq!(
        hidden_value_a.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(3)
    );
    assert_eq!(
        hidden_value_b.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(3)
    );

    let mut fewer_candidates =
        InvocationTransaction::begin(&database, [0x34; 16], [9; 16], 9_999, 32, 2_048)?;
    let mut more_candidates =
        InvocationTransaction::begin(&database, [0x35; 16], [9; 16], 9_999, 32, 2_048)?;
    for row_id in 3..=10 {
        more_candidates.stage_put(
            id(row_id),
            &row(Some("not-visible"), &format!("hidden-{row_id}")),
        )?;
    }
    assert_eq!(
        fewer_candidates.estimate_authorized_scan(table_id, &filtered_projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Unknown
    );
    assert_eq!(
        more_candidates.estimate_authorized_scan(table_id, &filtered_projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Unknown
    );
    assert_eq!(
        fewer_candidates.estimate_authorized_predicate_at_most(
            table_id,
            &filtered_projection,
            &predicate,
            4,
        )?,
        None
    );
    assert_eq!(
        more_candidates.estimate_authorized_predicate_at_most(
            table_id,
            &filtered_projection,
            &predicate,
            128,
        )?,
        None,
        "adding invisible candidates must not change the released estimate"
    );
    assert_eq!(
        more_candidates.estimate_authorized_population_at_most(
            table_id,
            &filtered_projection,
            4
        )?,
        None
    );
    let fewer_authorized_rows =
        fewer_candidates.scan_authorized(table_id, None, 32, &filtered_projection)?;
    let more_authorized_rows =
        more_candidates.scan_authorized(table_id, None, 32, &filtered_projection)?;
    assert!(fewer_authorized_rows.continuation.is_none());
    assert!(more_authorized_rows.continuation.is_none());
    assert_eq!(fewer_authorized_rows.rows, more_authorized_rows.rows);
    assert_eq!(
        fewer_authorized_rows.rows,
        vec![(
            id(1),
            vec![
                SanitizedCell::Value(ScalarValue::Text("first".to_owned())),
                SanitizedCell::Redacted,
            ],
        )]
    );

    let deny_all_policy = Policy {
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(false)))),
        ..policies[0].clone()
    };
    let deny_all_policies = [deny_all_policy, policies[1].clone()];
    let deny_all_projection = AuthorizedProjection {
        policies: &deny_all_policies,
        ..projection
    };
    let expected_empty_estimate = prdb_engine::statistics::AuthorizedScanEstimate::Exact(
        prdb_engine::statistics::AuthorizedScanSummary {
            visible_rows: 0,
            columns: vec![prdb_engine::statistics::ColumnPopulation::default(); 2],
        },
    );
    assert_eq!(
        fewer_candidates.estimate_authorized_scan(table_id, &deny_all_projection)?,
        expected_empty_estimate
    );
    assert_eq!(
        more_candidates.estimate_authorized_scan(table_id, &deny_all_projection)?,
        expected_empty_estimate
    );
    let fewer_denied_rows =
        fewer_candidates.scan_authorized(table_id, None, 32, &deny_all_projection)?;
    let more_denied_rows =
        more_candidates.scan_authorized(table_id, None, 32, &deny_all_projection)?;
    assert!(fewer_denied_rows.rows.is_empty());
    assert!(more_denied_rows.rows.is_empty());

    let deny_restrict_policy = Policy {
        effect: Effect::Restrict,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(false)))),
        ..policies[0].clone()
    };
    let allow_and_deny_policies = [
        policies[0].clone(),
        deny_restrict_policy.clone(),
        policies[1].clone(),
    ];
    let allow_and_deny_projection = AuthorizedProjection {
        policies: &allow_and_deny_policies,
        ..projection
    };
    assert_eq!(
        fewer_candidates.estimate_authorized_scan(table_id, &allow_and_deny_projection)?,
        expected_empty_estimate
    );
    assert!(
        fewer_candidates
            .scan_authorized(table_id, None, 32, &allow_and_deny_projection)?
            .rows
            .is_empty()
    );
    let deny_and_dynamic_policies = [
        deny_restrict_policy,
        filtered_policies[0].clone(),
        policies[1].clone(),
    ];
    let deny_and_dynamic_projection = AuthorizedProjection {
        policies: &deny_and_dynamic_policies,
        ..projection
    };
    assert_eq!(
        more_candidates.estimate_authorized_scan(table_id, &deny_and_dynamic_projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Unknown
    );
    assert!(fewer_candidates.check_deadline().is_ok());
    assert!(more_candidates.check_deadline().is_ok());
    Ok(())
}

#[test]
fn authorized_predicate_estimate_compares_readable_decimal_columns()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::optimizer_binding::{BoundColumn, BoundPredicate};
    use prdb_engine::optimizer_request::Comparison;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let decimal_type = ScalarType::Decimal {
        precision: 8,
        scale: 2,
    };
    let columns = [
        ColumnSchema {
            scalar_type: decimal_type,
            nullable: true,
        },
        ColumnSchema {
            scalar_type: decimal_type,
            nullable: true,
        },
    ];
    let column_ids = [[0x52; 16], [0x53; 16]];
    let table_id = id(0).table;
    let encode_row = |left: Option<i128>, right: Option<i128>| {
        encode_tuple(
            &columns,
            1,
            &[
                left.map(ScalarValue::Decimal),
                right.map(ScalarValue::Decimal),
            ],
        )
        .unwrap()
    };
    let mut seed = InvocationTransaction::begin(&database, [0x51; 16], [9; 16], 9_999, 8, 512)?;
    seed.stage_put(id(1), &encode_row(Some(110), Some(110)))?;
    seed.stage_put(id(2), &encode_row(Some(220), Some(330)))?;
    seed.stage_put(id(3), &encode_row(None, None))?;
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema([0x54; 16])),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: column_ids[0],
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: column_ids[1],
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema([0x54; 16]),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[0],
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[1],
            },
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal([9; 16])],
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal([9; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[0],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal([9; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[1],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
    ];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: [0x54; 16],
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let predicate = |operator| BoundPredicate::Compare {
        operator,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[0],
            ordinal: 0,
            scalar_type: decimal_type,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[1],
            ordinal: 1,
            scalar_type: decimal_type,
            nullable: true,
        })),
    };
    let mut observer = InvocationTransaction::begin(&database, [0x55; 16], [9; 16], 9_999, 8, 512)?;
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate(Comparison::Equal),
            3,
        )?,
        Some((3, 1))
    );
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate(Comparison::Less),
            3,
        )?,
        Some((3, 1))
    );
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate(Comparison::NotEqual),
            3,
        )?,
        Some((3, 1))
    );
    Ok(())
}

#[test]
fn authorized_predicate_estimate_compares_readable_scalar_columns()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::optimizer_binding::{BoundColumn, BoundPredicate};
    use prdb_engine::optimizer_request::Comparison;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let cases = [
        (
            ScalarType::Boolean,
            ScalarValue::Boolean(false),
            ScalarValue::Boolean(false),
            ScalarValue::Boolean(true),
        ),
        (
            ScalarType::SmallInt,
            ScalarValue::SmallInt(1),
            ScalarValue::SmallInt(1),
            ScalarValue::SmallInt(2),
        ),
        (
            ScalarType::Integer,
            ScalarValue::Integer(10),
            ScalarValue::Integer(10),
            ScalarValue::Integer(11),
        ),
        (
            ScalarType::BigInt,
            ScalarValue::BigInt(20),
            ScalarValue::BigInt(20),
            ScalarValue::BigInt(21),
        ),
        (
            ScalarType::Decimal {
                precision: 8,
                scale: 2,
            },
            ScalarValue::Decimal(1_100),
            ScalarValue::Decimal(1_100),
            ScalarValue::Decimal(1_200),
        ),
        (
            ScalarType::Text,
            ScalarValue::Text("same".to_owned()),
            ScalarValue::Text("left".to_owned()),
            ScalarValue::Text("right".to_owned()),
        ),
        (
            ScalarType::Varchar { max_chars: 32 },
            ScalarValue::Varchar("same".to_owned()),
            ScalarValue::Varchar("left".to_owned()),
            ScalarValue::Varchar("right".to_owned()),
        ),
        (
            ScalarType::Bytea,
            ScalarValue::Bytea(vec![1, 2]),
            ScalarValue::Bytea(vec![1, 2]),
            ScalarValue::Bytea(vec![1, 3]),
        ),
        (
            ScalarType::Uuid,
            ScalarValue::Uuid([1; 16]),
            ScalarValue::Uuid([1; 16]),
            ScalarValue::Uuid([2; 16]),
        ),
        (
            ScalarType::Date,
            ScalarValue::Date(10),
            ScalarValue::Date(10),
            ScalarValue::Date(11),
        ),
        (
            ScalarType::TimestampTz,
            ScalarValue::TimestampTz(100),
            ScalarValue::TimestampTz(100),
            ScalarValue::TimestampTz(101),
        ),
    ];
    let columns: Vec<_> = cases
        .iter()
        .flat_map(|(scalar_type, _, _, _)| {
            [
                ColumnSchema {
                    scalar_type: *scalar_type,
                    nullable: true,
                },
                ColumnSchema {
                    scalar_type: *scalar_type,
                    nullable: true,
                },
            ]
        })
        .collect();
    let column_ids: Vec<_> = (1..=columns.len())
        .map(|ordinal| [ordinal as u8; 16])
        .collect();
    let table_id = id(0).table;
    let equal_values: Vec<_> = cases
        .iter()
        .flat_map(|(_, equal, _, _)| [Some(equal.clone()), Some(equal.clone())])
        .collect();
    let unequal_values: Vec<_> = cases
        .iter()
        .flat_map(|(_, _, left, right)| [Some(left.clone()), Some(right.clone())])
        .collect();
    let null_values = vec![None; columns.len()];
    let mut seed = InvocationTransaction::begin(&database, [0x61; 16], [9; 16], 9_999, 8, 65_536)?;
    for (row_id, values) in [(1, equal_values), (2, unequal_values), (3, null_values)] {
        seed.stage_put(id(row_id), &encode_tuple(&columns, 1, &values)?)?;
    }
    seed.commit()?;

    let schema_id = [0x62; 16];
    let mut scope = std::collections::HashSet::from([
        (Operation::Usage, AccessTarget::Schema(schema_id)),
        (Operation::Select, AccessTarget::Table(table_id)),
    ]);
    let mut grants = vec![Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(schema_id),
    }];
    let mut policies = vec![Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    }];
    for column_id in &column_ids {
        let target = AccessTarget::Column {
            table_id,
            column_id: *column_id,
        };
        scope.insert((Operation::Select, target));
        grants.push(Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target,
        });
        policies.push(Policy {
            subjects: vec![Subject::Principal([9; 16])],
            operation: Operation::Select,
            target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        });
    }
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope,
    };
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut observer =
        InvocationTransaction::begin(&database, [0x63; 16], [9; 16], 9_999, 8, 65_536)?;
    for (case_index, (scalar_type, _, _, _)) in cases.iter().enumerate() {
        let left_ordinal = case_index * 2;
        let right_ordinal = left_ordinal + 1;
        let predicate = |operator| BoundPredicate::Compare {
            operator,
            left: Box::new(BoundPredicate::Column(BoundColumn {
                column_id: column_ids[left_ordinal],
                ordinal: left_ordinal as u32,
                scalar_type: *scalar_type,
                nullable: true,
            })),
            right: Box::new(BoundPredicate::Column(BoundColumn {
                column_id: column_ids[right_ordinal],
                ordinal: right_ordinal as u32,
                scalar_type: *scalar_type,
                nullable: true,
            })),
        };
        assert_eq!(
            observer.estimate_authorized_predicate_at_most(
                table_id,
                &projection,
                &predicate(Comparison::Equal),
                3,
            )?,
            Some((3, 1)),
            "equality estimate for {scalar_type:?}"
        );
        assert_eq!(
            observer.estimate_authorized_predicate_at_most(
                table_id,
                &projection,
                &predicate(Comparison::NotEqual),
                3,
            )?,
            Some((3, 1)),
            "inequality estimate for {scalar_type:?}"
        );
        assert_eq!(
            observer.estimate_authorized_predicate_at_most(
                table_id,
                &projection,
                &predicate(Comparison::Less),
                3,
            )?,
            Some((3, 1)),
            "ordered estimate for {scalar_type:?}"
        );
    }
    let date_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::Date)
        .expect("fixture has a DATE comparison pair");
    let date_ordinal = date_case * 2 + 1;
    let date_literal_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[date_ordinal],
            ordinal: date_ordinal as u32,
            scalar_type: ScalarType::Date,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Date(11)),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &date_literal_predicate,
            3,
        )?,
        Some((3, 1)),
        "DATE literals should compare against authorized DATE cells"
    );
    let uuid_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::Uuid)
        .expect("fixture has a UUID comparison pair");
    let uuid_ordinal = uuid_case * 2 + 1;
    let uuid_literal_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[uuid_ordinal],
            ordinal: uuid_ordinal as u32,
            scalar_type: ScalarType::Uuid,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Uuid([2; 16])),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &uuid_literal_predicate,
            3,
        )?,
        Some((3, 1)),
        "UUID literals should compare against authorized UUID cells"
    );
    let timestamp_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::TimestampTz)
        .expect("fixture has a TIMESTAMPTZ comparison pair");
    let timestamp_ordinal = timestamp_case * 2 + 1;
    let timestamp_literal_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[timestamp_ordinal],
            ordinal: timestamp_ordinal as u32,
            scalar_type: ScalarType::TimestampTz,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::TimestampTz(100)),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &timestamp_literal_predicate,
            3,
        )?,
        Some((3, 1)),
        "TIMESTAMPTZ literals should compare against authorized cells"
    );
    let invalid_date_literal = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[date_ordinal],
            ordinal: date_ordinal as u32,
            scalar_type: ScalarType::Date,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Date(i32::MAX)),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &invalid_date_literal,
            3,
        )?,
        None,
        "DATE estimates must reject values outside the scalar range"
    );
    let incompatible_column_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[2],
            ordinal: 2,
            scalar_type: ScalarType::SmallInt,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[4],
            ordinal: 4,
            scalar_type: ScalarType::Integer,
            nullable: true,
        })),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &incompatible_column_predicate,
            3,
        )?,
        None,
        "same-width estimate values must not permit cross-type comparisons"
    );
    let bytea_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::Bytea)
        .expect("fixture has a BYTEA comparison pair");
    let bytea_left_ordinal = bytea_case * 2;
    let bytea_right_ordinal = bytea_left_ordinal + 1;
    let mut oversized_values: Vec<_> = cases
        .iter()
        .flat_map(|(_, equal, _, _)| [Some(equal.clone()), Some(equal.clone())])
        .collect();
    oversized_values[bytea_left_ordinal] = Some(ScalarValue::Bytea(vec![7; 4097]));
    oversized_values[bytea_right_ordinal] = Some(ScalarValue::Bytea(vec![7; 4097]));
    let oversized_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[bytea_left_ordinal],
            ordinal: bytea_left_ordinal as u32,
            scalar_type: ScalarType::Bytea,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[bytea_right_ordinal],
            ordinal: bytea_right_ordinal as u32,
            scalar_type: ScalarType::Bytea,
            nullable: true,
        })),
    };
    let mut oversized =
        InvocationTransaction::begin(&database, [0x64; 16], [9; 16], 9_999, 8, 65_536)?;
    oversized.stage_put(id(4), &encode_tuple(&columns, 1, &oversized_values)?)?;
    assert_eq!(
        oversized.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &oversized_predicate,
            4,
        )?,
        None,
        "oversized variable-width predicate cells must fall back without partial counts"
    );
    Ok(())
}

#[test]
fn authorized_join_estimate_counts_duplicate_matches_and_private_overlay()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let schema_id = [0x72; 16];
    let left_table = [0x73; 16];
    let right_table = [0x74; 16];
    let left_key = [0x75; 16];
    let right_key = [0x76; 16];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: true,
    }];
    let column_ids = [[left_key], [right_key]];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(left_table)),
            (Operation::Select, AccessTarget::Table(right_table)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: left_table,
                    column_id: left_key,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: right_table,
                    column_id: right_key,
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: left_table,
                column_id: left_key,
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: right_key,
            },
        },
    ];
    let table_policy = |table_id| Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let left_policy = table_policy(left_table);
    let right_policy = table_policy(right_table);
    let policies = [
        left_policy.clone(),
        Policy {
            target: AccessTarget::Column {
                table_id: left_table,
                column_id: left_key,
            },
            ..left_policy
        },
        right_policy.clone(),
        Policy {
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: right_key,
            },
            ..right_policy
        },
    ];
    let left_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids[0],
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let right_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids[1],
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let row =
        |value: Option<i32>| encode_tuple(&columns, 1, &[value.map(ScalarValue::Integer)]).unwrap();
    let mut seed = InvocationTransaction::begin(&database, [0x77; 16], [9; 16], 9_999, 32, 4096)?;
    for (number, value) in [(1, Some(7)), (2, Some(7)), (3, Some(9)), (4, None)] {
        seed.stage_put(numbered_row_id(left_table, number), &row(value))?;
    }
    for (number, value) in [
        (1, Some(7)),
        (2, Some(7)),
        (3, Some(7)),
        (4, Some(9)),
        (5, Some(8)),
        (6, None),
    ] {
        seed.stage_put(numbered_row_id(right_table, number), &row(value))?;
    }
    seed.commit()?;

    let mut planner =
        InvocationTransaction::begin(&database, [0x78; 16], [9; 16], 9_999, 512, 1_048_576)?;
    let left_ids = [left_key];
    let right_ids = [right_key];
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            8,
        )?,
        Some(7),
        "duplicate keys contribute their full SQL join multiplicity and NULLs do not match"
    );

    let hidden_key_policies = [
        policies[0].clone(),
        policies[2].clone(),
        policies[3].clone(),
    ];
    let hidden_left_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids[0],
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &hidden_key_policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &hidden_left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            8,
        )?,
        None,
        "the estimator must not inspect an unreadable join key"
    );
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            1025,
        )?,
        None,
        "join-cardinality work remains capped at 1,024 rows per side"
    );

    planner.stage_put(numbered_row_id(left_table, 5), &row(Some(7)))?;
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            8,
        )?,
        Some(10),
        "the estimate must reflect the invocation's private overlay"
    );
    for number in 100..226 {
        planner.stage_put(numbered_row_id(left_table, number), &row(Some(7)))?;
    }
    for number in 100..225 {
        planner.stage_put(numbered_row_id(right_table, number), &row(Some(7)))?;
    }
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            512,
        )?,
        Some(16_513),
        "a 512-row bound must estimate populations above the old 128-row ceiling"
    );
    planner.commit()?;

    let mut expansion =
        InvocationTransaction::begin(&database, [0x7a; 16], [9; 16], 9_999, 2048, 8 * 1024 * 1024)?;
    for number in 1_000..1_893 {
        expansion.stage_put(numbered_row_id(left_table, number), &row(Some(7)))?;
        expansion.stage_put(numbered_row_id(right_table, number), &row(Some(7)))?;
    }
    expansion.commit()?;

    let mut exact_1024 =
        InvocationTransaction::begin(&database, [0x7b; 16], [9; 16], 9_999, 32, 4096)?;
    assert_eq!(
        exact_1024.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            1024,
        )?,
        Some(1_043_463),
        "the estimator should preserve duplicate join multiplicity at the 1,024-row boundary"
    );
    assert_eq!(
        exact_1024.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            512,
        )?,
        None,
        "populations above the requested bound must return Unknown without partial counts"
    );

    let mut oversized =
        InvocationTransaction::begin(&database, [0x79; 16], [9; 16], 9_999, 512, 1_048_576)?;
    for number in [2_000, 2_001] {
        oversized.stage_put(numbered_row_id(right_table, number), &row(Some(7)))?;
    }
    for number in [2_000, 2_001] {
        oversized.stage_put(numbered_row_id(left_table, number), &row(Some(7)))?;
    }
    assert_eq!(
        oversized.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            1024,
        )?,
        None,
        "a 1,025-row population must produce Unknown without a partial count"
    );
    assert_eq!(
        oversized.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            1025,
        )?,
        None,
        "join-cardinality work remains capped at the declared bound"
    );
    Ok(())
}

#[test]
fn authorized_scan_estimate_returns_unknown_at_candidate_budget()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::statistics::{
        AuthorizedScanEstimate, AuthorizedScanSummary, MAX_ESTIMATE_CANDIDATES,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(0).table;
    let schema_id = [0x23; 16];
    let column_id = [0x22; 16];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: false,
    }];
    let row = |value: i32| encode_tuple(&columns, 1, &[Some(ScalarValue::Integer(value))]).unwrap();

    let mut seed = InvocationTransaction::begin(
        &database,
        [0x51; 16],
        [9; 16],
        9_999,
        MAX_ESTIMATE_CANDIDATES + 1,
        4 * 1024 * 1024,
    )?;
    for index in 1..MAX_ESTIMATE_CANDIDATES {
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: table_id,
                row: (index as u128).to_be_bytes(),
            },
            &row(index as i32),
        )?;
    }
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        },
    ];
    let table_allow = Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_allow = Policy {
        target: AccessTarget::Column {
            table_id,
            column_id,
        },
        ..table_allow.clone()
    };
    let policies = [table_allow, cell_allow];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &[column_id],
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut below_limit =
        InvocationTransaction::begin(&database, [0x52; 16], [9; 16], 9_999, 1, 64)?;
    assert_eq!(
        below_limit.estimate_authorized_scan(table_id, &projection)?,
        AuthorizedScanEstimate::Exact(AuthorizedScanSummary {
            visible_rows: (MAX_ESTIMATE_CANDIDATES - 1) as u64,
            columns: vec![prdb_engine::statistics::ColumnPopulation {
                readable_values: (MAX_ESTIMATE_CANDIDATES - 1) as u64,
                nulls: 0,
                redacted: 0,
            }],
        })
    );

    let mut extra = InvocationTransaction::begin(&database, [0x53; 16], [9; 16], 9_999, 1, 64)?;
    extra.stage_put(
        prdb_engine::overlay::RowId {
            table: table_id,
            row: (MAX_ESTIMATE_CANDIDATES as u128).to_be_bytes(),
        },
        &row(MAX_ESTIMATE_CANDIDATES as i32),
    )?;
    extra.commit()?;
    let mut at_limit = InvocationTransaction::begin(&database, [0x54; 16], [9; 16], 9_999, 1, 64)?;
    assert_eq!(
        at_limit.estimate_authorized_scan(table_id, &projection)?,
        AuthorizedScanEstimate::Unknown
    );
    Ok(())
}

#[test]
fn invocation_auto_indexes_row_writes() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(5);
    let payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"indexed".to_vec()))],
    )?;

    let mut missing = InvocationTransaction::begin(&database, [5; 16], [9; 16], 9_999, 4, 2_048)?;
    let schema = missing.load_catalog_table_schema(row_id.table)?;
    let indexes = missing.load_catalog_indexes(&schema)?;
    assert_eq!(indexes.len(), 3);
    missing.stage_put(row_id, &payload)?;
    let mut index_keys = Vec::new();
    for index in &indexes {
        index_keys.push(
            derive_index_key(
                index.index_id,
                row_id.row,
                schema.schema_version,
                &schema.column_ids,
                &schema.columns,
                &payload,
                &index.column_ids,
            )?
            .key,
        );
    }
    assert_eq!(missing.commit()?, 1);
    {
        let read = database.begin_read()?;
        let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
        for index_key in &index_keys {
            let locator = indexes.get(index_key.as_slice())?.unwrap().value().to_vec();
            assert_eq!(
                IndexLocator::decode_for_key(index_key, &locator)?.row_version,
                1
            );
        }
    }

    let mut without_index_load =
        InvocationTransaction::begin(&database, [12; 16], [9; 16], 9_999, 4, 2_048)?;
    without_index_load.load_catalog_table_schema(row_id.table)?;
    without_index_load.stage_put(row_id, &payload)?;
    assert!(matches!(
        without_index_load.commit(),
        Err(TransactionError::Commit(CommitError::InvalidIndex))
    ));

    let mut forged = InvocationTransaction::begin(&database, [8; 16], [9; 16], 9_999, 4, 2_048)?;
    let schema = forged.load_catalog_table_schema(row_id.table)?;
    let indexes = forged.load_catalog_indexes(&schema)?;
    forged.stage_put(row_id, &payload)?;
    forged.stage_index_membership(
        row_id,
        compose_key(indexes[0].index_id, &[1, 0x7f], row_id.row),
        true,
    )?;
    assert!(matches!(
        forged.commit(),
        Err(TransactionError::Commit(CommitError::InvalidIndex))
    ));

    let small_row_id = id(13);
    let small_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"x".to_vec()))],
    )?;
    let mut bounded = InvocationTransaction::begin(&database, [13; 16], [9; 16], 9_999, 256, 40)?;
    let schema = bounded.load_catalog_table_schema(small_row_id.table)?;
    bounded.load_catalog_indexes(&schema)?;
    bounded.stage_put(small_row_id, &small_payload)?;
    assert!(matches!(
        bounded.commit(),
        Err(TransactionError::IndexOverlayLimitExceeded)
    ));
    {
        let read = database.begin_read()?;
        assert!(
            read.open_table(ROWS)?
                .get(&key(small_row_id)[..])?
                .is_none()
        );
    }
    let updated_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"changed".to_vec()))],
    )?;
    let mut update = InvocationTransaction::begin(&database, [7; 16], [9; 16], 9_999, 4, 2_048)?;
    let schema = update.load_catalog_table_schema(row_id.table)?;
    let indexes_for_update = update.load_catalog_indexes(&schema)?;
    update.stage_put(row_id, &updated_payload)?;
    let mut updated_index_keys = Vec::new();
    for index in &indexes_for_update {
        let new = derive_index_key(
            index.index_id,
            row_id.row,
            schema.schema_version,
            &schema.column_ids,
            &schema.columns,
            &updated_payload,
            &index.column_ids,
        )
        .unwrap();
        updated_index_keys.push(new.key);
    }
    assert_eq!(update.commit()?, 2);
    {
        let read = database.begin_read()?;
        let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
        for index_key in &index_keys {
            assert!(indexes.get(index_key.as_slice())?.is_none());
        }
        for index_key in &updated_index_keys {
            let locator = indexes.get(index_key.as_slice())?.unwrap().value().to_vec();
            assert_eq!(
                IndexLocator::decode_for_key(index_key, &locator)?.row_version,
                2
            );
        }
    }

    let mut delete = InvocationTransaction::begin(&database, [9; 16], [9; 16], 9_999, 4, 2_048)?;
    let schema = delete.load_catalog_table_schema(row_id.table)?;
    delete.load_catalog_indexes(&schema)?;
    delete.stage_delete(row_id)?;
    assert_eq!(delete.commit()?, 3);
    let read = database.begin_read()?;
    let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
    for index_key in &updated_index_keys {
        assert!(indexes.get(index_key.as_slice())?.is_none());
    }
    Ok(())
}

#[test]
fn unique_constraint_uses_its_backing_index_for_invocation_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [0x10; 16];
    let column_id = [0x66; 16];
    let index_id = [0x55; 16];
    let constraint_id = [0x77; 16];
    let range_index_id = [0x56; 16];
    let (index_key, index_value) = index_catalog_record_with_constraint(
        index_id,
        table_id,
        column_id,
        1,
        true,
        Some(constraint_id),
    )?;
    let (constraint_key, constraint_value) =
        unique_constraint_catalog_record(constraint_id, table_id, column_id, index_id)?;
    let (range_index_key, range_index_value) =
        index_catalog_record(range_index_id, table_id, column_id, 1)?;
    {
        let write = database.begin_write()?;
        let mut catalog = write.open_table(CATALOG)?;
        catalog.insert(index_key.as_slice(), index_value.as_slice())?;
        catalog.insert(range_index_key.as_slice(), range_index_value.as_slice())?;
        catalog.insert(constraint_key.as_slice(), constraint_value.as_slice())?;
        drop(catalog);
        write.commit()?;
    }

    let index_profile_rows = std::env::var("PRDB_BENCH_INDEX_ROWS")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("PRDB_BENCH_INDEX_ROWS must be a u64")
        })
        .unwrap_or(4_096);
    let index_profile_matches = std::env::var("PRDB_BENCH_INDEX_MATCHES")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("PRDB_BENCH_INDEX_MATCHES must be a u64")
        })
        .unwrap_or(1);
    let index_profile_text = std::env::var_os("PRDB_BENCH_INDEX_TEXT").is_some();
    assert!(index_profile_matches <= index_profile_rows);
    let benchmark_value = |value: &str| {
        if index_profile_text {
            ScalarValue::Text(value.to_owned())
        } else {
            ScalarValue::Bytea(value.as_bytes().to_vec())
        }
    };
    let mut seed = InvocationTransaction::begin(
        &database,
        [241; 16],
        [9; 16],
        9_999,
        index_profile_rows as usize + 2,
        (index_profile_rows as usize + 2) * 2_048,
    )?;
    let seed_schema = seed.load_catalog_table_schema(table_id)?;
    seed.load_catalog_indexes(&seed_schema)?;
    let payload = encode_tuple(
        &seed_schema.columns,
        seed_schema.schema_version,
        &[Some(benchmark_value("duplicate"))],
    )?;
    seed.stage_put(id(243), &payload)?;
    let alternate_payload = encode_tuple(
        &seed_schema.columns,
        seed_schema.schema_version,
        &[Some(benchmark_value("alternate duplicate"))],
    )?;
    seed.stage_put(id(245), &alternate_payload)?;
    for number in 0..index_profile_rows {
        let value = format!("key-{number:05}");
        let payload = encode_tuple(
            &seed_schema.columns,
            seed_schema.schema_version,
            &[Some(benchmark_value(&value))],
        )?;
        seed.stage_put(numbered_row_id(table_id, number + 1_000), &payload)?;
    }
    assert_eq!(seed.commit()?, 1);

    let mut invocation =
        InvocationTransaction::begin(&database, [242; 16], [9; 16], 9_999, 8, 512)?;
    let schema = invocation.load_catalog_table_schema(table_id)?;
    assert!(
        invocation
            .load_catalog_check_constraints(&schema)?
            .is_empty()
    );
    let indexes = invocation.load_catalog_indexes(&schema)?;
    assert!(
        indexes
            .iter()
            .any(|index| index.index_id == index_id && index.unique)
    );
    let payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(benchmark_value("duplicate"))],
    )?;
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
    let visible_row_count = index_profile_rows + 2;
    let expected_small_population = (visible_row_count
        <= prdb_engine::optimizer_plan::SMALL_POPULATION_SCAN_ROWS)
        .then_some(visible_row_count);
    assert_eq!(
        invocation.estimate_authorized_population_at_most(
            table_id,
            &projection,
            prdb_engine::optimizer_plan::SMALL_POPULATION_SCAN_ROWS,
        )?,
        expected_small_population
    );
    let expected_join_population = (visible_row_count
        <= prdb_engine::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS)
        .then_some(visible_row_count);
    assert_eq!(
        invocation.estimate_authorized_population_at_most(
            table_id,
            &projection,
            prdb_engine::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS,
        )?,
        expected_join_population
    );
    let range_bound = || prdb_engine::transaction::AuthorizedIndexRangeBound {
        value: benchmark_value("duplicate"),
        inclusive: true,
    };
    let target_cell = SanitizedCell::Value(benchmark_value("duplicate"));
    let probe_values = [Some(benchmark_value("duplicate"))];
    let mut reference_matches = Vec::new();
    let mut reference_cursor = None;
    loop {
        let page =
            invocation.scan_authorized(table_id, reference_cursor.as_ref(), 128, &projection)?;
        reference_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
            (cells.first() == Some(&target_cell)).then_some((row_id, cells))
        }));
        let Some(cursor) = page.continuation else {
            break;
        };
        reference_cursor = Some(cursor);
    }
    let unique_probe = invocation.probe_catalog_unique_index(
        table_id,
        index_id,
        &[Some(benchmark_value("duplicate"))],
        &projection,
    )?;
    assert!(matches!(
        &unique_probe,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id, cells }
            if reference_matches.len() == 1
                && *id == reference_matches[0].0
                && cells == &reference_matches[0].1
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(table_id, index_id, &[None], &projection)?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch,
        "SQL equality with NULL cannot match a unique-index key"
    );
    let matching_range = invocation.scan_catalog_index_range(
        table_id,
        range_index_id,
        Some(range_bound()),
        Some(range_bound()),
        &projection,
    )?;
    assert!(matches!(
        &matching_range,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
            if rows.len() == reference_matches.len()
                && rows.iter().zip(&reference_matches).all(|(indexed, reference)| {
                    indexed.0 == reference.0 && indexed.1 == reference.1
                })
    ));
    if std::env::var_os("PRDB_BENCH_INDEX_ACCESS").is_some() {
        const RUNS: usize = 11;
        let mut point_probe_times = Vec::with_capacity(RUNS);
        let mut range_times = Vec::with_capacity(RUNS);
        let mut reference_scan_times = Vec::with_capacity(RUNS);
        let mut small_population_probe_times = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let started = Instant::now();
            let small_population = invocation.estimate_authorized_population_at_most(
                table_id,
                &projection,
                prdb_engine::optimizer_plan::SMALL_POPULATION_SCAN_ROWS,
            )?;
            small_population_probe_times.push(started.elapsed());
            let total_rows = index_profile_rows + 2;
            let expected_population = (total_rows
                <= prdb_engine::optimizer_plan::SMALL_POPULATION_SCAN_ROWS)
                .then_some(total_rows);
            assert_eq!(small_population, expected_population);

            let started = Instant::now();
            let probe = invocation.probe_catalog_unique_index(
                table_id,
                index_id,
                &probe_values,
                &projection,
            )?;
            point_probe_times.push(started.elapsed());
            assert!(matches!(
                probe,
                prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id: found, .. }
                    if found == id(243)
            ));

            let lower = Some(range_bound());
            let upper = Some(range_bound());
            let started = Instant::now();
            let range = invocation.scan_catalog_index_range(
                table_id,
                range_index_id,
                lower,
                upper,
                &projection,
            )?;
            range_times.push(started.elapsed());
            assert!(matches!(
                range,
                prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
                    if rows.len() == 1 && rows[0].0 == id(243)
            ));

            let started = Instant::now();
            let mut cursor = None;
            let mut matches = Vec::new();
            loop {
                let page =
                    invocation.scan_authorized(table_id, cursor.as_ref(), 128, &projection)?;
                matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
                    (cells.first() == Some(&target_cell)).then_some(row_id)
                }));
                let Some(next) = page.continuation else {
                    break;
                };
                cursor = Some(next);
            }
            reference_scan_times.push(started.elapsed());
            assert_eq!(matches, [id(243)]);
        }
        let median_and_p95_ms = |samples: &mut [Duration]| {
            samples.sort_unstable();
            (
                samples[samples.len() / 2].as_secs_f64() * 1_000.0,
                samples[((samples.len() - 1) * 95).div_ceil(100)].as_secs_f64() * 1_000.0,
            )
        };
        let point_probe = median_and_p95_ms(&mut point_probe_times);
        let range = median_and_p95_ms(&mut range_times);
        let reference_scan = median_and_p95_ms(&mut reference_scan_times);
        let small_population_probe = median_and_p95_ms(&mut small_population_probe_times);
        eprintln!(
            "index access benchmark: numbered_rows={index_profile_rows}, total_rows={}, runs={RUNS}, small-population probe median/p95={:.3}/{:.3} ms, point probe median/p95={:.3}/{:.3} ms, range median/p95={:.3}/{:.3} ms, authorized scan+filter median/p95={:.3}/{:.3} ms",
            index_profile_rows + 2,
            small_population_probe.0,
            small_population_probe.1,
            point_probe.0,
            point_probe.1,
            range.0,
            range.1,
            reference_scan.0,
            reference_scan.1,
        );

        if index_profile_matches > 0 {
            let lower_value = format!("key-{:05}", 0);
            let upper_value = format!("key-{:05}", index_profile_matches - 1);
            let lower_key = lower_value.as_bytes().to_vec();
            let upper_key = upper_value.as_bytes().to_vec();
            let fanout_bound = |value: String| {
                Some(prdb_engine::transaction::AuthorizedIndexRangeBound {
                    value: benchmark_value(&value),
                    inclusive: true,
                })
            };
            let lower = fanout_bound(lower_value);
            let upper = fanout_bound(upper_value);
            let expected_rows = (0..index_profile_matches)
                .map(|number| numbered_row_id(table_id, number + 1_000))
                .collect::<Vec<_>>();
            let key_column = prdb_engine::optimizer_binding::BoundColumn {
                column_id,
                ordinal: 0,
                scalar_type: schema.columns[0].scalar_type,
                nullable: schema.columns[0].nullable,
            };
            let range_predicate = prdb_engine::optimizer_binding::BoundPredicate::And(vec![
                prdb_engine::optimizer_binding::BoundPredicate::Compare {
                    operator: prdb_engine::optimizer_request::Comparison::GreaterOrEqual,
                    left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
                        key_column.clone(),
                    )),
                    right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
                        lower_key.iter().copied().map(char::from).collect(),
                    )),
                },
                prdb_engine::optimizer_binding::BoundPredicate::Compare {
                    operator: prdb_engine::optimizer_request::Comparison::LessOrEqual,
                    left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
                        key_column,
                    )),
                    right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
                        upper_key.iter().copied().map(char::from).collect(),
                    )),
                },
            ]);
            let mut predicate_estimate_times = Vec::with_capacity(RUNS);
            let mut range_fanout_times = Vec::with_capacity(RUNS);
            let mut scan_fanout_times = Vec::with_capacity(RUNS);
            for _ in 0..RUNS {
                let started = Instant::now();
                let estimate = invocation.estimate_authorized_predicate_at_most(
                    table_id,
                    &projection,
                    &range_predicate,
                    prdb_engine::statistics::MAX_PREDICATE_ESTIMATE_ROWS,
                )?;
                predicate_estimate_times.push(started.elapsed());
                assert_eq!(
                    estimate,
                    Some((index_profile_rows + 2, index_profile_matches))
                );

                let started = Instant::now();
                let indexed = invocation.scan_catalog_index_range(
                    table_id,
                    range_index_id,
                    lower.clone(),
                    upper.clone(),
                    &projection,
                )?;
                range_fanout_times.push(started.elapsed());
                assert!(matches!(
                    indexed,
                    prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
                        if rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>() == expected_rows
                ));

                let started = Instant::now();
                let mut cursor = None;
                let mut scan_matches = Vec::new();
                loop {
                    let page =
                        invocation.scan_authorized(table_id, cursor.as_ref(), 128, &projection)?;
                    scan_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
                        let matches = match cells.first() {
                            Some(SanitizedCell::Value(ScalarValue::Text(value))) => {
                                value.as_bytes() >= lower_key.as_slice()
                                    && value.as_bytes() <= upper_key.as_slice()
                            }
                            Some(SanitizedCell::Value(ScalarValue::Bytea(value))) => {
                                value.as_slice() >= lower_key.as_slice()
                                    && value.as_slice() <= upper_key.as_slice()
                            }
                            _ => false,
                        };
                        matches.then_some(row_id)
                    }));
                    let Some(next) = page.continuation else {
                        break;
                    };
                    cursor = Some(next);
                }
                scan_fanout_times.push(started.elapsed());
                assert_eq!(scan_matches, expected_rows);
            }
            let predicate_estimate = median_and_p95_ms(&mut predicate_estimate_times);
            let range_fanout = median_and_p95_ms(&mut range_fanout_times);
            let scan_fanout = median_and_p95_ms(&mut scan_fanout_times);
            eprintln!(
                "index selectivity benchmark: total_rows={}, matching_rows={}, predicate estimate median/p95={:.3}/{:.3} ms, range median/p95={:.3}/{:.3} ms, authorized scan+filter median/p95={:.3}/{:.3} ms",
                index_profile_rows + 2,
                index_profile_matches,
                predicate_estimate.0,
                predicate_estimate.1,
                range_fanout.0,
                range_fanout.1,
                scan_fanout.0,
                scan_fanout.1,
            );
        }
    }
    if index_profile_text {
        return Ok(());
    }
    let hidden_context = AuthorizationContext {
        principal_id,
        role_ids: std::collections::HashSet::new(),
        scope: [
            (Operation::Usage, AccessTarget::Schema(schema.schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
        ]
        .into_iter()
        .collect(),
    };
    let hidden_projection = AuthorizedProjection {
        columns: projection.columns,
        column_ids: projection.column_ids,
        schema_version: projection.schema_version,
        schema_id: projection.schema_id,
        context: &hidden_context,
        grants: &grants[..2],
        policies: projection.policies,
        checks: projection.checks,
        policy_driving_columns: projection.policy_driving_columns,
        dependencies: projection.dependencies,
    };
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &hidden_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable
    );
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(
                b"absent key with another size".to_vec()
            ))],
            &hidden_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "a hidden key must not distinguish a matching value from a missing value"
    );
    assert_eq!(
        invocation.probe_catalog_unique_index(table_id, index_id, &[None], &hidden_projection)?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "key readability must be checked before a NULL probe can expose its outcome"
    );
    let wrong_key_types = [
        ScalarValue::Boolean(true),
        ScalarValue::SmallInt(7),
        ScalarValue::Integer(7),
        ScalarValue::BigInt(7),
        ScalarValue::Decimal(7),
        ScalarValue::Text("duplicate".to_owned()),
        ScalarValue::Varchar("duplicate".to_owned()),
        ScalarValue::Uuid([7; 16]),
        ScalarValue::Date(7),
        ScalarValue::TimestampTz(7),
    ];
    for value in wrong_key_types {
        assert_eq!(
            invocation.probe_catalog_unique_index(
                table_id,
                index_id,
                &[Some(value)],
                &hidden_projection,
            )?,
            prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
            "a hidden BYTEA key must not reveal whether a mismatched equality type is accepted"
        );
    }
    let conditional_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Bytea(
                    b"duplicate".to_vec(),
                )))),
            )),
            ..policies[1].clone()
        },
    ];
    let conditional_projection = AuthorizedProjection {
        columns: projection.columns,
        column_ids: projection.column_ids,
        schema_version: projection.schema_version,
        schema_id: projection.schema_id,
        context: projection.context,
        grants: projection.grants,
        policies: &conditional_policies,
        checks: projection.checks,
        policy_driving_columns: projection.policy_driving_columns,
        dependencies: projection.dependencies,
    };
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &conditional_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable
    );
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(
                b"not the conditional key".to_vec()
            ))],
            &conditional_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "a conditionally readable key must always fall back before inspecting the predicate value"
    );
    let failing_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::And(vec![
                Expr::Equal(
                    Box::new(Expr::Divide(
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(1)))),
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                    )),
                    Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                ),
                Expr::Equal(
                    Box::new(Expr::CurrentPrincipalId),
                    Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                ),
            ])),
            ..policies[1].clone()
        },
    ];
    let failing_projection = AuthorizedProjection {
        columns: projection.columns,
        column_ids: projection.column_ids,
        schema_version: projection.schema_version,
        schema_id: projection.schema_id,
        context: projection.context,
        grants: projection.grants,
        policies: &failing_policies,
        checks: projection.checks,
        policy_driving_columns: &[column_id],
        dependencies: projection.dependencies,
    };
    let mut failing_invocation =
        InvocationTransaction::begin(&database, [0x48; 16], [9; 16], 9_999, 8, 512)?;
    failing_invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        failing_invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &failing_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "an erroring policy predicate must force the authorized reference scan"
    );
    assert!(matches!(
        failing_invocation.scan_authorized(table_id, None, 8, &failing_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [0x48; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted,
        "the failing reference predicate must durably abort the invocation"
    );
    let modulo_error_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::And(vec![
                Expr::Equal(
                    Box::new(Expr::Modulo(
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(1)))),
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                    )),
                    Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                ),
                Expr::Equal(
                    Box::new(Expr::CurrentPrincipalId),
                    Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                ),
            ])),
            ..policies[1].clone()
        },
    ];
    let modulo_error_projection = AuthorizedProjection {
        policies: &modulo_error_policies,
        ..failing_projection
    };
    let mut modulo_error_invocation =
        InvocationTransaction::begin(&database, [0x49; 16], [9; 16], 9_999, 8, 512)?;
    modulo_error_invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        modulo_error_invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &modulo_error_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "a modulo-erroring policy must force the authorized reference scan"
    );
    assert!(matches!(
        modulo_error_invocation.scan_authorized(table_id, None, 8, &modulo_error_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [0x49; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted,
        "the modulo-erroring reference predicate must durably abort the invocation"
    );
    let negation_error_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::And(vec![
                Expr::Equal(
                    Box::new(Expr::Negate(Box::new(Expr::Literal(Some(
                        ScalarValue::SmallInt(i16::MIN),
                    ))))),
                    Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                ),
                Expr::Equal(
                    Box::new(Expr::CurrentPrincipalId),
                    Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                ),
            ])),
            ..policies[1].clone()
        },
    ];
    let negation_error_projection = AuthorizedProjection {
        columns: projection.columns,
        column_ids: projection.column_ids,
        schema_version: projection.schema_version,
        schema_id: projection.schema_id,
        context: projection.context,
        grants: projection.grants,
        policies: &negation_error_policies,
        checks: projection.checks,
        policy_driving_columns: &[column_id],
        dependencies: projection.dependencies,
    };
    let mut negation_error_invocation =
        InvocationTransaction::begin(&database, [0x4a; 16], [9; 16], 9_999, 8, 512)?;
    negation_error_invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        negation_error_invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &negation_error_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "an overflowing-negation policy must force the authorized reference scan"
    );
    assert!(matches!(
        negation_error_invocation.scan_authorized(table_id, None, 8, &negation_error_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [0x4a; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted,
        "the overflowing-negation reference predicate must durably abort the invocation"
    );
    let addition_error_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::And(vec![
                Expr::Equal(
                    Box::new(Expr::Add(
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(i16::MAX)))),
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(1)))),
                    )),
                    Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                ),
                Expr::Equal(
                    Box::new(Expr::CurrentPrincipalId),
                    Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                ),
            ])),
            ..policies[1].clone()
        },
    ];
    let addition_error_projection = AuthorizedProjection {
        policies: &addition_error_policies,
        ..failing_projection
    };
    let mut addition_error_invocation =
        InvocationTransaction::begin(&database, [0x4b; 16], [9; 16], 9_999, 8, 512)?;
    addition_error_invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        addition_error_invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &addition_error_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "an overflowing-addition policy must force the authorized reference scan"
    );
    assert!(matches!(
        addition_error_invocation.scan_authorized(table_id, None, 8, &addition_error_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [0x4b; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted,
        "the overflowing-addition reference predicate must durably abort the invocation"
    );
    for (offset, (label, arithmetic)) in [
        (
            "multiplication",
            Expr::Multiply(
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(i16::MAX)))),
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(2)))),
            ),
        ),
        (
            "subtraction",
            Expr::Subtract(
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(i16::MIN)))),
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(1)))),
            ),
        ),
        (
            "division",
            Expr::Divide(
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(i16::MIN)))),
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(-1)))),
            ),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let invocation_id = [0x4c + offset as u8; 16];
        let arithmetic_error_policies = [
            policies[0].clone(),
            Policy {
                using: Some(Expr::And(vec![
                    Expr::Equal(
                        Box::new(arithmetic),
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                    ),
                    Expr::Equal(
                        Box::new(Expr::CurrentPrincipalId),
                        Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                    ),
                ])),
                ..policies[1].clone()
            },
        ];
        let arithmetic_error_projection = AuthorizedProjection {
            policies: &arithmetic_error_policies,
            ..failing_projection
        };
        let mut arithmetic_error_invocation =
            InvocationTransaction::begin(&database, invocation_id, [9; 16], 9_999, 8, 512)?;
        arithmetic_error_invocation.load_catalog_table_schema(table_id)?;
        assert_eq!(
            arithmetic_error_invocation.probe_catalog_unique_index(
                table_id,
                index_id,
                &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
                &arithmetic_error_projection,
            )?,
            prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
            "an overflowing-{label} policy must force the authorized reference scan"
        );
        assert!(matches!(
            arithmetic_error_invocation.scan_authorized(
                table_id,
                None,
                8,
                &arithmetic_error_projection,
            ),
            Err(TransactionError::Authorization(
                prdb_engine::authorization::AuthorizationError::EvaluationFailed
            ))
        ));
        assert_eq!(
            prdb_engine::outcome::lookup_outcome(&database, invocation_id, [9; 16], 1,)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted,
            "the overflowing-{label} reference predicate must durably abort the invocation"
        );
    }
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match {
            id: id(243),
            cells: vec![SanitizedCell::Value(ScalarValue::Bytea(
                b"duplicate".to_vec()
            ))],
        },
    );
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &conditional_projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Unavailable,
        "conditional key permissions must use the reference scan"
    );
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(vec![(
            id(243),
            vec![SanitizedCell::Value(ScalarValue::Bytea(
                b"duplicate".to_vec()
            ))],
        )]),
    );
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &hidden_projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Unavailable
    );
    invocation.stage_put(
        id(243),
        &encode_tuple(
            &schema.columns,
            schema.schema_version,
            &[Some(ScalarValue::Bytea(b"moved-away".to_vec()))],
        )?,
    )?;
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(Vec::new()),
        "staged key changes must shadow the pinned index candidate"
    );
    let moved_bound = || prdb_engine::transaction::AuthorizedIndexRangeBound {
        value: ScalarValue::Bytea(b"moved-away".to_vec()),
        inclusive: true,
    };
    assert!(matches!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(moved_bound()),
            Some(moved_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
            if rows.len() == 1 && rows[0].0 == id(243)
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch
    );
    assert!(matches!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"moved-away".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id: found, .. }
            if found == id(243)
    ));
    invocation.stage_delete(id(243))?;
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(Vec::new()),
        "staged deletes must shadow pinned range candidates"
    );
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch
    );
    invocation.stage_put(id(243), &payload)?;
    invocation.stage_put(id(244), &payload)?;
    assert!(matches!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
            if rows.len() == 2
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
    );
    let first_unique_error = invocation
        .commit()
        .expect_err("duplicate indexed values must fail the first commit");
    assert!(matches!(
        &first_unique_error,
        TransactionError::Commit(CommitError::UniqueViolation)
    ));

    let mut alternate_invocation =
        InvocationTransaction::begin(&database, [0x47; 16], [9; 16], 9_999, 8, 512)?;
    let alternate_schema = alternate_invocation.load_catalog_table_schema(table_id)?;
    alternate_invocation.load_catalog_check_constraints(&alternate_schema)?;
    alternate_invocation.load_catalog_indexes(&alternate_schema)?;
    alternate_invocation.stage_put(id(246), &alternate_payload)?;
    let alternate_unique_error = alternate_invocation
        .commit()
        .expect_err("the second duplicate indexed value must fail its commit");
    assert_eq!(
        format!("{first_unique_error:?}"),
        format!("{alternate_unique_error:?}"),
        "unique diagnostics must not include conflicting values or row identities"
    );
    assert!(matches!(
        alternate_unique_error,
        TransactionError::Commit(CommitError::UniqueViolation)
    ));

    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(243))[..])?.is_some());
    assert!(read.open_table(ROWS)?.get(&key(id(244))[..])?.is_none());
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [242; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn authorized_composite_integer_varchar_unique_probe_matches_reference()
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

#[test]
fn catalog_foreign_keys_check_child_and_parent_mutations() -> Result<(), Box<dyn std::error::Error>>
{
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

#[test]
fn catalog_schema_validates_rows_without_secondary_indexes()
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

#[test]
fn failed_commit_validation_records_aborted_outcome() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn fenced_invocation_commit_waits_for_ddl_and_aborts_on_timeout()
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

#[test]
fn replacing_new_index_membership_discards_the_intermediate_key()
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

#[test]
fn missing_index_removal_aborts_with_a_durable_outcome() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn deleting_new_row_releases_index_overlay_budget() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn index_overlay_budget_aborts_the_entire_invocation() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn unsupported_database_versions_reject_transactions_and_commits()
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

#[test]
fn deleting_same_snapshot_row_twice_aborts_invocation() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn stale_invocation_aborts_and_insert_then_delete_commits_as_noop()
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

#[test]
fn statement_error_aborts_earlier_writes_and_prevents_finalization()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;

    let mut invocation = InvocationTransaction::begin(&database, [3; 16], [9; 16], 9_999, 4, 64)?;
    invocation.stage_put(id(1), b"must-abort")?;
    assert!(matches!(
        invocation.stage_delete(id(2)),
        Err(TransactionError::NotFound)
    ));
    assert!(matches!(
        invocation.read(id(1)),
        Err(TransactionError::Aborted)
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));

    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    let row = RowRecord::decode(rows.get(&key(id(1))[..])?.unwrap().value())?;
    assert_eq!(row.row_version, 1);
    assert_eq!(row.payload, b"snapshot");
    assert_eq!(
        EngineMetadata::decode(
            read.open_table(ENGINE_META)?
                .get(&b"format"[..])?
                .unwrap()
                .value()
        )?
        .commit_sequence,
        1
    );
    let aborted = read
        .open_table(prdb_engine::schema::OUTCOMES)?
        .get(&[3; 16][..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        InvocationOutcome::decode(&aborted)?,
        InvocationOutcome {
            status: OutcomeStatus::Aborted,
            invocation_id: [3; 16],
            principal_id: [9; 16],
            commit_sequence: 0,
            retention_deadline_us: 9_999,
        }
    );
    Ok(())
}

#[test]
fn admission_requires_ready_metadata_and_a_fresh_valid_invocation_id()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;
    assert!(matches!(
        InvocationTransaction::begin(&database, [1; 16], [9; 16], 9_999, 1, 1),
        Err(TransactionError::DuplicateInvocation)
    ));
    assert!(matches!(
        InvocationTransaction::begin(&database, [0; 16], [9; 16], 9_999, 1, 1),
        Err(TransactionError::InvalidContext)
    ));

    let write = database.begin_write()?;
    let next = EngineMetadata {
        database_id: [0x33; 16],
        catalog_version: 2,
        expression_version: 1,
        builtin_semantics_version: 1,
        commit_sequence: 1,
        lifecycle: Lifecycle::Initializing,
    }
    .encode()?;
    write
        .open_table(ENGINE_META)?
        .insert(&b"format"[..], &next[..])?;
    write.commit()?;
    assert!(matches!(
        InvocationTransaction::begin(&database, [2; 16], [9; 16], 9_999, 1, 1),
        Err(TransactionError::NotReady)
    ));
    Ok(())
}

#[test]
fn concurrent_same_row_writers_have_one_commit_and_one_durable_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;
    let barrier = Arc::new(Barrier::new(3));

    let (left, right) = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_database = &database;
        let left = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(left_database, [10; 16], [9; 16], 9_999, 2, 32)?;
            transaction.stage_put(id(1), b"left")?;
            left_barrier.wait();
            transaction.commit()
        });
        let right_barrier = barrier.clone();
        let right_database = &database;
        let right = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(right_database, [11; 16], [9; 16], 9_999, 2, 32)?;
            transaction.stage_put(id(1), b"right")?;
            right_barrier.wait();
            transaction.commit()
        });
        barrier.wait();
        (left.join().unwrap(), right.join().unwrap())
    });

    let outcomes = [left, right];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(TransactionError::Commit(CommitError::Conflict))))
            .count(),
        1
    );
    assert_eq!(
        commit_sequence(&database)?,
        2,
        "only the committed writer advances the data sequence"
    );
    let read = database.begin_read()?;
    let row = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(id(1))[..])?
            .unwrap()
            .value(),
    )?;
    assert_eq!(row.row_version, 2);
    assert!(row.payload == b"left" || row.payload == b"right");
    let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
    let left = InvocationOutcome::decode(outcomes.get(&[10; 16][..])?.unwrap().value())?;
    let right = InvocationOutcome::decode(outcomes.get(&[11; 16][..])?.unwrap().value())?;
    assert_eq!(
        [left.status, right.status]
            .iter()
            .filter(|status| **status == OutcomeStatus::Committed)
            .count(),
        1
    );
    assert_eq!(
        [left.status, right.status]
            .iter()
            .filter(|status| **status == OutcomeStatus::Aborted)
            .count(),
        1
    );
    Ok(())
}

#[test]
fn concurrent_authorized_updates_have_one_commit_and_one_durable_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(2);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: false,
    }];
    let column_ids = [[0x51; 16]];
    commit_rows(
        &database,
        [20; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Integer(10))],
            )?),
        }],
    )?;

    let table = AccessTarget::Table(row_id.table);
    let column = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[55; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, table),
            (Operation::Update, table),
            (Operation::Select, column),
            (Operation::Update, column),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(row_id.table),
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Select,
            target: table,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Update,
            target: table,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Select,
            target: column,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Update,
            target: column,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Select,
            target: table,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Update,
            target: table,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Select,
            target: column,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Update,
            target: column,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let barrier = Arc::new(Barrier::new(3));
    let outcomes = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_database = &database;
        let left_projection = &projection;
        let left = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(left_database, [21; 16], [9; 16], 9_999, 2, 128)?;
            transaction.stage_patch_authorized(
                row_id,
                left_projection,
                &[FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Integer(20)),
                }],
            )?;
            left_barrier.wait();
            transaction.commit()
        });
        let right_barrier = barrier.clone();
        let right_database = &database;
        let right_projection = &projection;
        let right = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(right_database, [22; 16], [9; 16], 9_999, 2, 128)?;
            transaction.stage_patch_authorized(
                row_id,
                right_projection,
                &[FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Integer(30)),
                }],
            )?;
            right_barrier.wait();
            transaction.commit()
        });
        barrier.wait();
        [left.join().unwrap(), right.join().unwrap()]
    });

    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(TransactionError::Commit(CommitError::Conflict))))
            .count(),
        1
    );
    assert_eq!(commit_sequence(&database)?, 2);
    let read = database.begin_read()?;
    let record = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(row_id)[..])?
            .unwrap()
            .value(),
    )?;
    assert_eq!(record.row_version, 2);
    assert!(matches!(
        decode_tuple(&columns, 1, &record.payload)?
            .values
            .as_slice(),
        [Some(ScalarValue::Integer(20 | 30))]
    ));
    let outcome_table = read.open_table(prdb_engine::schema::OUTCOMES)?;
    let left = InvocationOutcome::decode(outcome_table.get(&[21; 16][..])?.unwrap().value())?;
    let right = InvocationOutcome::decode(outcome_table.get(&[22; 16][..])?.unwrap().value())?;
    assert_eq!(
        [left.status, right.status]
            .iter()
            .filter(|status| **status == OutcomeStatus::Committed)
            .count(),
        1
    );
    assert_eq!(
        [left.status, right.status]
            .iter()
            .filter(|status| **status == OutcomeStatus::Aborted)
            .count(),
        1
    );
    Ok(())
}

#[test]
fn snapshot_isolation_allows_documented_disjoint_row_write_skew()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    commit_rows(
        &database,
        [1; 16],
        [9; 16],
        9_999,
        &[
            RowWrite {
                key: key(id(1)),
                expected_version: None,
                payload: Some(b"on-call".to_vec()),
            },
            RowWrite {
                key: key(id(2)),
                expected_version: None,
                payload: Some(b"on-call".to_vec()),
            },
        ],
    )?;

    // Each transaction observes both people on call, then independently takes
    // one person off call. Their write sets are disjoint, so snapshot isolation
    // permits both commits even though the resulting state violates the
    // application invariant that at least one person remains on call.
    let barrier = Arc::new(Barrier::new(3));
    let (left, right) = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_database = &database;
        let left = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(left_database, [12; 16], [9; 16], 9_999, 2, 32)?;
            assert_eq!(
                transaction.read(id(1))?.as_deref(),
                Some(b"on-call".as_slice())
            );
            assert_eq!(
                transaction.read(id(2))?.as_deref(),
                Some(b"on-call".as_slice())
            );
            transaction.stage_put(id(1), b"off-call")?;
            left_barrier.wait();
            transaction.commit()
        });
        let right_barrier = barrier.clone();
        let right_database = &database;
        let right = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(right_database, [13; 16], [9; 16], 9_999, 2, 32)?;
            assert_eq!(
                transaction.read(id(1))?.as_deref(),
                Some(b"on-call".as_slice())
            );
            assert_eq!(
                transaction.read(id(2))?.as_deref(),
                Some(b"on-call".as_slice())
            );
            transaction.stage_put(id(2), b"off-call")?;
            right_barrier.wait();
            transaction.commit()
        });
        barrier.wait();
        (left.join().unwrap(), right.join().unwrap())
    });

    let mut commit_sequences = [left?, right?];
    commit_sequences.sort();
    assert_eq!(commit_sequences, [2, 3]);
    assert_eq!(commit_sequence(&database)?, 3);
    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    for row_id in [id(1), id(2)] {
        let record = RowRecord::decode(rows.get(&key(row_id)[..])?.unwrap().value())?;
        assert_eq!(record.payload, b"off-call");
    }
    let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
    for invocation_id in [[12; 16], [13; 16]] {
        assert_eq!(
            InvocationOutcome::decode(outcomes.get(&invocation_id[..])?.unwrap().value())?.status,
            OutcomeStatus::Committed
        );
    }
    Ok(())
}

#[test]
fn field_patch_merges_with_pinned_tuple_and_preserves_untouched_values()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        },
    ];
    let original = vec![
        Some(ScalarValue::Text("private unchanged cell".into())),
        Some(ScalarValue::Integer(10)),
        None,
    ];
    let payload = encode_tuple(&columns, 1, &original)?;
    commit_rows(
        &database,
        [1; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(1)),
            expected_version: None,
            payload: Some(payload),
        }],
    )?;

    let mut transaction = InvocationTransaction::begin(&database, [2; 16], [9; 16], 9_999, 4, 64)?;
    transaction.stage_patch(
        id(1),
        &columns,
        1,
        &[FieldPatch {
            ordinal: 1,
            value: Some(ScalarValue::Integer(11)),
        }],
    )?;
    let staged_bytes = transaction.read(id(1))?.unwrap();
    let staged = decode_tuple(&columns, 1, &staged_bytes)?;
    assert_eq!(staged.values[0], original[0]);
    assert_eq!(staged.values[1], Some(ScalarValue::Integer(11)));
    assert_eq!(staged.values[2], None);
    transaction.commit()?;

    let read = database.begin_read()?;
    let bytes = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(id(1))[..])?
            .unwrap()
            .value(),
    )?;
    let committed = decode_tuple(&columns, 1, &bytes.payload)?;
    assert_eq!(committed.values[0], original[0]);
    assert_eq!(committed.values[1], Some(ScalarValue::Integer(11)));
    assert_eq!(committed.values[2], None);
    assert_eq!(committed.schema_version, 1);
    Ok(())
}

#[test]
fn authorized_patch_checks_changed_cells_and_freezes_policy_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Uuid,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
    ];
    let column_ids = [[51; 16], [52; 16], [53; 16]];
    let row_id = id(50);
    let second_row_id = prdb_engine::overlay::RowId {
        table: row_id.table,
        row: [0x51; 16],
    };
    let original = [
        Some(ScalarValue::Text("Alice".to_owned())),
        Some(ScalarValue::Uuid([9; 16])),
        Some(ScalarValue::Text("hidden unchanged".to_owned())),
    ];
    commit_rows(
        &database,
        [54; 16],
        [9; 16],
        9_999,
        &[
            RowWrite {
                key: key(row_id),
                expected_version: None,
                payload: Some(encode_tuple(&columns, 1, &original)?),
            },
            RowWrite {
                key: key(second_row_id),
                expected_version: None,
                payload: Some(encode_tuple(&columns, 1, &original)?),
            },
        ],
    )?;
    let table_target = AccessTarget::Table(row_id.table);
    let name_target = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[55; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, table_target),
            (Operation::Update, table_target),
            (Operation::Select, name_target),
            (Operation::Update, name_target),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(row_id.table),
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Select,
            target: table_target,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Update,
            target: table_target,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Select,
            target: name_target,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Update,
            target: name_target,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Select,
            target: table_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Update,
            target: table_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Select,
            target: name_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Update,
            target: name_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[Expr::NotEqual(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Text(
                "forbidden".to_owned(),
            )))),
        )],
        policy_driving_columns: &[column_ids[1]],
        dependencies: &[],
    };
    let hidden_row_policies = [
        Policy {
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(false)))),
            ..policies[0].clone()
        },
        policies[1].clone(),
        policies[2].clone(),
        policies[3].clone(),
    ];
    let hidden_row_projection = AuthorizedProjection {
        policies: &hidden_row_policies,
        ..projection
    };
    let absent_row_id = prdb_engine::overlay::RowId {
        table: row_id.table,
        row: [0xef; 16],
    };
    let update_patch = [FieldPatch {
        ordinal: 0,
        value: Some(ScalarValue::Text("attempted update".to_owned())),
    }];
    let mut hidden_row_update =
        InvocationTransaction::begin(&database, [59; 16], [9; 16], 9_999, 2, 128)?;
    let hidden_update_error = hidden_row_update
        .stage_patch_authorized(row_id, &hidden_row_projection, &update_patch)
        .expect_err("a row hidden by its SELECT policy cannot be updated");
    let mut absent_row_update =
        InvocationTransaction::begin(&database, [60; 16], [9; 16], 9_999, 2, 128)?;
    let absent_update_error = absent_row_update
        .stage_patch_authorized(absent_row_id, &hidden_row_projection, &update_patch)
        .expect_err("an absent row cannot be updated");
    assert_eq!(
        format!("{hidden_update_error:?}"),
        format!("{absent_update_error:?}"),
        "UPDATE diagnostics must not distinguish a policy-hidden row from an absent row"
    );
    assert!(matches!(hidden_update_error, TransactionError::NotFound));
    assert!(matches!(absent_update_error, TransactionError::NotFound));

    let mut invocation = InvocationTransaction::begin(&database, [56; 16], [9; 16], 9_999, 2, 128)?;
    invocation.stage_patch_authorized(
        row_id,
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Text("Alice updated".to_owned())),
        }],
    )?;
    invocation.commit()?;

    let mut constraint_denied =
        InvocationTransaction::begin(&database, [58; 16], [9; 16], 9_999, 2, 128)?;
    let first_constraint_error = constraint_denied
        .stage_patch_authorized(
            row_id,
            &projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text("forbidden".to_owned())),
            }],
        )
        .expect_err("the CHECK expression must reject its first forbidden value");
    let alternate_checks = [Expr::NotEqual(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Text(
            "classified".to_owned(),
        )))),
    )];
    let alternate_projection = AuthorizedProjection {
        checks: &alternate_checks,
        ..projection
    };
    let mut second_constraint_denied =
        InvocationTransaction::begin(&database, [0x5a; 16], [9; 16], 9_999, 2, 128)?;
    let second_constraint_error = second_constraint_denied
        .stage_patch_authorized(
            second_row_id,
            &alternate_projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text("classified".to_owned())),
            }],
        )
        .expect_err("the CHECK expression must reject its second forbidden value");
    assert_eq!(
        format!("{first_constraint_error:?}"),
        format!("{second_constraint_error:?}"),
        "CHECK diagnostics must not include rejected values or row identities"
    );
    assert!(matches!(
        first_constraint_error,
        TransactionError::ConstraintViolation
    ));
    assert!(matches!(
        second_constraint_error,
        TransactionError::ConstraintViolation
    ));

    let mut denied = InvocationTransaction::begin(&database, [57; 16], [9; 16], 9_999, 2, 128)?;
    assert!(matches!(
        denied.stage_patch_authorized(
            row_id,
            &projection,
            &[FieldPatch {
                ordinal: 1,
                value: Some(ScalarValue::Uuid([8; 16])),
            }]
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));
    let read = database.begin_read()?;
    let record = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(row_id)[..])?
            .unwrap()
            .value(),
    )?;
    let updated = decode_tuple(&columns, 1, &record.payload)?;
    assert_eq!(
        updated.values,
        [
            Some(ScalarValue::Text("Alice updated".to_owned())),
            Some(ScalarValue::Uuid([9; 16])),
            Some(ScalarValue::Text("hidden unchanged".to_owned())),
        ]
    );
    Ok(())
}

#[test]
fn authorized_delete_requires_visibility_and_an_explicit_delete_grant()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(60);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[61; 16]];
    commit_rows(
        &database,
        [62; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text("visible but not deletable".into()))],
            )?),
        }],
    )?;
    let target = AccessTarget::Table(row_id.table);
    let cell_target = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[63; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, target),
            (Operation::Delete, target),
            (Operation::Select, cell_target),
        ]),
    };
    let row_select_policy = Policy {
        subjects: vec![Subject::Role([63; 16])],
        operation: Operation::Select,
        target,
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_select_policy = Policy {
        subjects: vec![Subject::Role([63; 16])],
        operation: Operation::Select,
        target: cell_target,
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let delete_policy = Policy {
        subjects: vec![Subject::Role([63; 16])],
        operation: Operation::Delete,
        target,
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let select_grant = Grant {
        subject: Subject::Role([63; 16]),
        operation: Operation::Select,
        target: cell_target,
    };
    let usage_grant = Grant {
        subject: Subject::Role([63; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(row_id.table),
    };
    let select_only_grants = [select_grant, usage_grant];
    let denied_policies = [
        row_select_policy.clone(),
        cell_select_policy.clone(),
        delete_policy.clone(),
    ];
    let denied_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &select_only_grants,
        policies: &denied_policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut denied = InvocationTransaction::begin(&database, [64; 16], [9; 16], 9_999, 2, 64)?;
    assert!(matches!(
        denied.stage_delete_authorized(row_id, &denied_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));

    let delete_grant = Grant {
        subject: Subject::Role([63; 16]),
        operation: Operation::Delete,
        target,
    };
    let mut allowed = InvocationTransaction::begin(&database, [65; 16], [9; 16], 9_999, 2, 64)?;
    let allowed_grants = [select_grant, delete_grant, usage_grant];
    let allowed_policies = [row_select_policy, cell_select_policy, delete_policy];
    let allowed_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &allowed_grants,
        policies: &allowed_policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };

    let hidden_row_policies = [
        Policy {
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(false)))),
            ..allowed_policies[0].clone()
        },
        allowed_policies[1].clone(),
        allowed_policies[2].clone(),
    ];
    let hidden_row_projection = AuthorizedProjection {
        policies: &hidden_row_policies,
        ..allowed_projection
    };
    let hidden_row_id = prdb_engine::overlay::RowId {
        table: row_id.table,
        row: row_id.row,
    };
    let absent_row_id = prdb_engine::overlay::RowId {
        table: row_id.table,
        row: [0xee; 16],
    };
    let mut hidden_row_delete =
        InvocationTransaction::begin(&database, [66; 16], [9; 16], 9_999, 2, 64)?;
    let hidden_row_error = hidden_row_delete
        .stage_delete_authorized(hidden_row_id, &hidden_row_projection)
        .expect_err("a row hidden by its SELECT policy cannot be deleted");
    let mut absent_row_delete =
        InvocationTransaction::begin(&database, [67; 16], [9; 16], 9_999, 2, 64)?;
    let absent_row_error = absent_row_delete
        .stage_delete_authorized(absent_row_id, &hidden_row_projection)
        .expect_err("an absent row cannot be deleted");
    assert_eq!(
        format!("{hidden_row_error:?}"),
        format!("{absent_row_error:?}"),
        "DELETE diagnostics must not distinguish a policy-hidden row from an absent row"
    );
    assert!(matches!(hidden_row_error, TransactionError::NotFound));
    assert!(matches!(absent_row_error, TransactionError::NotFound));

    allowed.stage_delete_authorized(row_id, &allowed_projection)?;
    allowed.commit()?;
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(row_id)[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[64; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn authorized_insert_materializes_defaults_before_policy_and_check_validation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(110);
    let column_id = [0x66; 16];
    let table = AccessTarget::Table(row_id.table);
    let schema = AccessTarget::Schema(row_id.table);
    let column = AccessTarget::Column {
        table_id: row_id.table,
        column_id,
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[114; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, schema),
            (Operation::Insert, table),
            (Operation::Insert, column),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Role([114; 16]),
            operation: Operation::Usage,
            target: schema,
        },
        Grant {
            subject: Subject::Role([114; 16]),
            operation: Operation::Insert,
            target: table,
        },
        Grant {
            subject: Subject::Role([114; 16]),
            operation: Operation::Insert,
            target: column,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role([114; 16])],
            operation: Operation::Insert,
            target: table,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::CurrentPrincipalId),
            )),
        },
        Policy {
            subjects: vec![Subject::Role([114; 16])],
            operation: Operation::Insert,
            target: column,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Uuid,
        nullable: false,
    }];
    let column_ids = [column_id];
    let checks = [Expr::Equal(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
    )];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &checks,
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut invalid_default =
        InvocationTransaction::begin(&database, [115; 16], [9; 16], 9_999, 1, 128)?;
    assert!(matches!(
        invalid_default.stage_insert_authorized_with_defaults(
            id(116),
            &projection,
            &[InsertInput::Default],
            &[Some(Expr::Field(0))],
            &[],
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::InvalidPolicy
        ))
    ));
    let default_expression_id = [117; 16];
    let check_expression_id = [121; 16];
    let boolean_type_id = [46; 16];
    let mut default_expression_key = 7u16.to_be_bytes().to_vec();
    default_expression_key.push(1);
    default_expression_key.extend_from_slice(&default_expression_id);
    let mut check_expression_key = 7u16.to_be_bytes().to_vec();
    check_expression_key.push(1);
    check_expression_key.extend_from_slice(&check_expression_id);
    let default_expression =
        expression_catalog_payload(default_expression_id, &Expr::CurrentPrincipalId)?;
    let type_id = [45; 16];
    let schema_id = row_id.table;
    let storage_id = [145; 16];
    let type_key = catalog_id_key(4, type_id);
    let table_key = catalog_id_key(5, row_id.table);
    let column_key = catalog_id_key(6, column_id);
    let type_payload = catalog_record_payload(
        4,
        &type_key,
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
    let table_payload = |schema_version: u64| {
        catalog_record_payload(
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
                    payload: schema_version.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: storage_id.to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )
    };
    let initial_table_payload = table_payload(1u64)?;
    let updated_table_payload = table_payload(2u64)?;
    let column_payload = catalog_record_payload(
        6,
        &column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: row_id.table.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 0u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: format!("object_{}", column_id[0]).into_bytes(),
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
                payload: vec![0],
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::Id,
                payload: default_expression_id.to_vec(),
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
    let initial_column_payload = catalog_record_payload(
        6,
        &column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: row_id.table.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 0u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: format!("object_{}", column_id[0]).into_bytes(),
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
    let check_expression = expression_catalog_payload_full(
        check_expression_id,
        2,
        boolean_type_id,
        [0; 8],
        &[column_ids[0]],
        &Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
        ),
    )?;
    let default_audit = AuditEvent {
        event_id: [119; 16],
        actor_id: Some([9; 16]),
        correlation_id: [120; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![
            row_id.table,
            type_id,
            column_id,
            default_expression_id,
            check_expression_id,
        ],
    };
    let mut catalog_writes = vec![
        CatalogWrite {
            key: default_expression_key,
            expected_value: None,
            payload: Some(default_expression),
        },
        CatalogWrite {
            key: check_expression_key,
            expected_value: None,
            payload: Some(check_expression),
        },
        CatalogWrite {
            key: type_key,
            expected_value: None,
            payload: Some(type_payload),
        },
        boolean_type_catalog_write(boolean_type_id)?,
        CatalogWrite {
            key: table_key,
            expected_value: Some(initial_table_payload),
            payload: Some(updated_table_payload),
        },
        CatalogWrite {
            key: column_key,
            expected_value: Some(initial_column_payload),
            payload: Some(column_payload),
        },
        object_catalog_write_for(default_expression_id, 5),
        object_catalog_write_for(check_expression_id, 5),
        object_catalog_write_for(type_id, 2),
        object_catalog_write_for(boolean_type_id, 2),
        dependency_catalog_write(column_id, default_expression_id, 2)?,
        dependency_catalog_write(check_expression_id, column_id, 2)?,
    ];
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        for index_id in [[0x55; 16], [1; 16], [2; 16]] {
            let (key, payload) = index_catalog_record(index_id, row_id.table, column_id, 2)?;
            let expected_value = catalog
                .get(key.as_slice())?
                .ok_or("missing test index definition")?
                .value()
                .to_vec();
            catalog_writes.push(object_catalog_write_for(index_id, 7));
            catalog_writes.push(CatalogWrite {
                key,
                expected_value: Some(expected_value),
                payload: Some(payload),
            });
        }
    }
    commit_catalog_and_audit(
        &database,
        [118; 16],
        [9; 16],
        9_999,
        &catalog_writes,
        &[default_audit],
    )?;
    let mut invocation =
        InvocationTransaction::begin(&database, [113; 16], [9; 16], 9_999, 1, 512)?;
    let catalog_schema = invocation.load_catalog_table_schema(row_id.table)?;
    invocation.load_catalog_indexes(&catalog_schema)?;
    let catalog_checks = invocation.read_catalog_bound_expressions(
        &[check_expression_id],
        prdb_engine::expression::ExpressionPurpose::Check,
        &catalog_schema.columns,
        &catalog_schema.column_ids,
        &[[46; 16]],
        &[ScalarType::Boolean],
    )?;
    let catalog_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: catalog_schema.schema_version,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &catalog_checks,
        policy_driving_columns: &[],
        dependencies: &[],
    };
    invocation.stage_insert_authorized_from_catalog_defaults(
        row_id,
        &catalog_projection,
        &[InsertInput::Default],
        &catalog_schema,
    )?;
    invocation.commit()?;

    let missing_column_grant = AuthorizedProjection {
        columns: catalog_projection.columns,
        column_ids: catalog_projection.column_ids,
        schema_version: catalog_projection.schema_version,
        schema_id: catalog_projection.schema_id,
        context: catalog_projection.context,
        grants: &grants[..2],
        policies: catalog_projection.policies,
        checks: catalog_projection.checks,
        policy_driving_columns: catalog_projection.policy_driving_columns,
        dependencies: catalog_projection.dependencies,
    };
    let denied_row_id = id(122);
    let mut denied_insert =
        InvocationTransaction::begin(&database, [123; 16], [9; 16], 9_999, 1, 512)?;
    assert!(matches!(
        denied_insert.stage_insert_authorized_from_catalog_defaults(
            denied_row_id,
            &missing_column_grant,
            &[InsertInput::Default],
            &catalog_schema,
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));
    assert!(matches!(
        denied_insert.commit(),
        Err(TransactionError::Aborted)
    ));

    let read = database.begin_read()?;
    let record = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(row_id)[..])?
            .unwrap()
            .value(),
    )?;
    assert_eq!(
        decode_tuple(&columns, catalog_schema.schema_version, &record.payload)?.values,
        [Some(ScalarValue::Uuid([9; 16]))]
    );
    assert!(read.open_table(ROWS)?.get(&key(id(116))[..])?.is_none());
    assert!(
        read.open_table(ROWS)?
            .get(&key(denied_row_id)[..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[123; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[115; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn authorized_insert_implicit_null_requires_column_insert_grant()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [0x31; 16];
    let schema_id = [0x32; 16];
    let principal_id = [0x33; 16];
    let value_column_id = [0x34; 16];
    let nullable_column_id = [0x35; 16];
    let table = AccessTarget::Table(table_id);
    let schema = AccessTarget::Schema(schema_id);
    let value_column = AccessTarget::Column {
        table_id,
        column_id: value_column_id,
    };
    let nullable_column = AccessTarget::Column {
        table_id,
        column_id: nullable_column_id,
    };
    let context = AuthorizationContext {
        principal_id,
        role_ids: Default::default(),
        scope: [
            (Operation::Usage, schema),
            (Operation::Insert, table),
            (Operation::Insert, value_column),
            (Operation::Insert, nullable_column),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Usage,
            target: schema,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Insert,
            target: table,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Insert,
            target: value_column,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Insert,
            target: nullable_column,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Insert,
            target: table,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Insert,
            target: value_column,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Insert,
            target: nullable_column,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
    ];
    let column_ids = [value_column_id, nullable_column_id];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let insert_inputs = [
        InsertInput::Value(Some(ScalarValue::Integer(7))),
        InsertInput::Default,
    ];
    let defaults = [None, None];
    let inserted = prdb_engine::overlay::RowId {
        table: table_id,
        row: [0x36; 16],
    };
    let mut allowed =
        InvocationTransaction::begin(&database, [0x37; 16], principal_id, 9_999, 2, 128)?;
    allowed.stage_insert_authorized_with_defaults(
        inserted,
        &projection,
        &insert_inputs,
        &defaults,
        &[],
    )?;
    allowed.commit()?;

    let denied = prdb_engine::overlay::RowId {
        table: table_id,
        row: [0x38; 16],
    };
    let projection_without_nullable_column_grant = AuthorizedProjection {
        grants: &grants[..3],
        ..projection
    };
    let mut denied_insert =
        InvocationTransaction::begin(&database, [0x39; 16], principal_id, 9_999, 2, 128)?;
    assert!(matches!(
        denied_insert.stage_insert_authorized_with_defaults(
            denied,
            &projection_without_nullable_column_grant,
            &insert_inputs,
            &defaults,
            &[],
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));
    assert!(matches!(
        denied_insert.commit(),
        Err(TransactionError::Aborted)
    ));

    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    let stored = RowRecord::decode(
        rows.get(&key(inserted)[..])?
            .ok_or("authorized implicit-NULL insert is missing")?
            .value(),
    )?;
    assert_eq!(
        decode_tuple(&columns, 1, &stored.payload)?.values,
        [Some(ScalarValue::Integer(7)), None]
    );
    assert!(rows.get(&key(denied)[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[0x39; 16][..])?
                .ok_or("implicit-NULL denial outcome is missing")?
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn catalog_insert_allocates_generated_identity_and_rejects_caller_override()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(212);
    let column_id = [213; 16];
    let table = AccessTarget::Table(row_id.table);
    let schema_target = AccessTarget::Schema(row_id.table);
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: Default::default(),
        scope: [
            (Operation::Usage, schema_target),
            (Operation::Insert, table),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: schema_target,
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: table,
        },
    ];
    let policies = [Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Insert,
        target: table,
        effect: Effect::Allow,
        using: None,
        with_check: Some(Expr::NotEqual(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Uuid([0; 16])))),
        )),
    }];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Uuid,
        nullable: false,
    }];
    let column_ids = [column_id];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let schema = prdb_engine::transaction::CatalogTableSchema {
        table_id: row_id.table,
        schema_id: row_id.table,
        schema_version: 1,
        write_epoch: 1,
        storage_object_id: [214; 16],
        columns: columns.to_vec(),
        column_ids: column_ids.to_vec(),
        column_type_ids: vec![[215; 16]],
        column_default_masks: vec![7],
        default_expression_ids: vec![None],
        generation_modes: vec![1],
    };
    let mut override_invocation =
        InvocationTransaction::begin(&database, [216; 16], [9; 16], 9_999, 2, 128)?;
    assert!(matches!(
        override_invocation.stage_insert_authorized_from_catalog_defaults(
            row_id,
            &projection,
            &[InsertInput::Value(Some(ScalarValue::Uuid([217; 16])))],
            &schema,
        ),
        Err(TransactionError::InvalidContext)
    ));
    assert!(matches!(
        override_invocation.commit(),
        Err(TransactionError::Aborted)
    ));

    let mut invocation =
        InvocationTransaction::begin(&database, [218; 16], [9; 16], 9_999, 2, 128)?;
    invocation.stage_insert_authorized_from_catalog_defaults(
        row_id,
        &projection,
        &[InsertInput::Default],
        &schema,
    )?;
    let payload = invocation.read(row_id)?.expect("generated row is staged");
    let tuple = decode_tuple(&columns, 1, &payload)?;
    assert!(matches!(
        tuple.values.first(),
        Some(Some(ScalarValue::Uuid(value))) if *value != [0; 16]
    ));
    invocation.abort()?;
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [218; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(row_id)[..])?.is_none());
    Ok(())
}

#[test]
fn composite_index_prefix_scan_reconciles_snapshot_and_private_overlay()
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

#[test]
fn schema_loader_uses_each_columns_type_parameters() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn catalog_rejects_deleting_expression_used_by_existing_check()
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

#[test]
fn invocation_loads_and_guards_catalog_table_schema() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [131; 16];
    let schema_id = [132; 16];
    let storage_id = [133; 16];
    let varchar_type_id = [134; 16];
    let uuid_type_id = [135; 16];
    let column_text_id = [136; 16];
    let column_uuid_id = [137; 16];
    let principal_id = [128; 16];
    let boolean_type_id = [149; 16];
    let policy_id = [150; 16];
    let policy_expression_id = [161; 16];
    let role_id = [168; 16];
    let cell_policy_id = [169; 16];
    let cell_policy_expression_id = [170; 16];
    let cell_allow_policy_id = [171; 16];
    let cell_allow_expression_id = [172; 16];
    let check_constraint_id = [173; 16];
    let check_expression_id = [174; 16];
    let mut writes = Vec::new();
    let type_fields = |kind: u16, parameter_rule: u16, default_mask: u32| {
        vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: kind.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: parameter_rule.to_le_bytes().to_vec(),
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
                payload: default_mask.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
        ]
    };
    for (type_id, kind, parameter_rule, default_mask) in [
        (varchar_type_id, 7u16, 2u16, 1u32),
        (uuid_type_id, 9u16, 0u16, 7u32),
        (boolean_type_id, 1u16, 0u16, 1u32),
    ] {
        let key = catalog_id_key(4, type_id);
        writes.push(CatalogWrite {
            key: key.clone(),
            expected_value: None,
            payload: Some(catalog_record_payload(
                4,
                &key,
                &type_fields(kind, parameter_rule, default_mask),
            )?),
        });
    }
    let schema_key = catalog_id_key(3, schema_id);
    writes.push(CatalogWrite {
        key: schema_key.clone(),
        expected_value: None,
        payload: Some(catalog_record_payload(
            3,
            &schema_key,
            &[CatalogField {
                tag: 1,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            }],
        )?),
    });
    let table_key = catalog_id_key(5, table_id);
    writes.push(CatalogWrite {
        key: table_key.clone(),
        expected_value: None,
        payload: Some(catalog_record_payload(
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
                    payload: 2u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U64,
                    payload: 3u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: storage_id.to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )?),
    });
    for (column_id, name, ordinal, type_id, parameters) in [
        (
            column_text_id,
            b"object_136".as_slice(),
            1u32,
            varchar_type_id,
            {
                let mut bytes = [0; 8];
                bytes[4..].copy_from_slice(&32u32.to_le_bytes());
                bytes
            },
        ),
        (
            column_uuid_id,
            b"object_137".as_slice(),
            0u32,
            uuid_type_id,
            [0; 8],
        ),
    ] {
        let key = catalog_id_key(6, column_id);
        writes.push(CatalogWrite {
            key: key.clone(),
            expected_value: None,
            payload: Some(catalog_record_payload(
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
                        payload: type_id.to_vec(),
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::Bytes,
                        payload: parameters.to_vec(),
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Bool,
                        payload: vec![u8::from(ordinal == 0)],
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
            )?),
        });
    }
    let (principal_key, principal_payload) =
        principal_catalog_record_for(principal_id, 1, b"reader", true, 1);
    writes.push(CatalogWrite {
        key: principal_key,
        expected_value: None,
        payload: Some(principal_payload),
    });
    let (principal_object_key, principal_object_payload) =
        principal_object_record_for(principal_id, 1);
    writes.push(CatalogWrite {
        key: principal_object_key,
        expected_value: None,
        payload: Some(principal_object_payload),
    });
    let (role_key, role_payload) =
        principal_catalog_record_for(role_id, 2, b"reader_role", false, 1);
    writes.push(CatalogWrite {
        key: role_key,
        expected_value: None,
        payload: Some(role_payload),
    });
    let (role_object_key, role_object_payload) = principal_object_record_for(role_id, 2);
    writes.push(CatalogWrite {
        key: role_object_key,
        expected_value: None,
        payload: Some(role_object_payload),
    });
    let (membership_key, membership_payload) =
        membership_catalog_record(principal_id, role_id, principal_id, 1);
    writes.push(CatalogWrite {
        key: membership_key,
        expected_value: None,
        payload: Some(membership_payload),
    });
    writes.push(dependency_catalog_write(principal_id, role_id, 5)?);
    let mut grant_authority_edges = std::collections::HashSet::new();
    for (privilege, scope, target) in [
        (1, 1, schema_id),
        (2, 1, table_id),
        (2, 2, column_uuid_id),
        (4, 2, column_uuid_id),
    ] {
        let (key, payload) = grant_catalog_record(
            principal_id,
            if scope == 1 { target } else { table_id },
            privilege,
            scope,
            target,
            principal_id,
            1,
        );
        writes.push(CatalogWrite {
            key,
            expected_value: None,
            payload: Some(payload),
        });
        if grant_authority_edges.insert((principal_id, target)) {
            writes.push(dependency_catalog_write(principal_id, target, 5)?);
        }
    }
    let (role_grant_key, role_grant_payload) =
        grant_catalog_record(role_id, table_id, 3, 2, column_text_id, principal_id, 1);
    writes.push(CatalogWrite {
        key: role_grant_key,
        expected_value: None,
        payload: Some(role_grant_payload),
    });
    writes.push(dependency_catalog_write(role_id, column_text_id, 5)?);
    let policy_expression_key = catalog_id_key(7, policy_expression_id);
    writes.push(CatalogWrite {
        key: policy_expression_key.clone(),
        expected_value: None,
        payload: Some(expression_catalog_payload_full(
            policy_expression_id,
            3,
            boolean_type_id,
            [0; 8],
            &[column_uuid_id],
            &Expr::Equal(Box::new(Expr::Field(0)), Box::new(Expr::CurrentPrincipalId)),
        )?),
    });
    let policy_key = catalog_id_key(13, policy_id);
    let policy_payload = catalog_record_payload(
        13,
        &policy_key,
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
                payload: policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    writes.push(CatalogWrite {
        key: policy_key.clone(),
        expected_value: None,
        payload: Some(policy_payload.clone()),
    });
    let (policy_target_key, policy_target_payload) =
        policy_target_catalog_record(policy_id, 1, principal_id);
    writes.push(CatalogWrite {
        key: policy_target_key,
        expected_value: None,
        payload: Some(policy_target_payload),
    });
    writes.push(dependency_catalog_write(policy_id, principal_id, 5)?);
    let (role_policy_target_key, role_policy_target_payload) =
        policy_target_catalog_record(policy_id, 2, role_id);
    writes.push(CatalogWrite {
        key: role_policy_target_key,
        expected_value: None,
        payload: Some(role_policy_target_payload),
    });
    writes.push(dependency_catalog_write(policy_id, role_id, 5)?);
    let cell_policy_expression_key = catalog_id_key(7, cell_policy_expression_id);
    writes.push(CatalogWrite {
        key: cell_policy_expression_key.clone(),
        expected_value: None,
        payload: Some(expression_catalog_payload_full(
            cell_policy_expression_id,
            3,
            boolean_type_id,
            [0; 8],
            &[column_uuid_id],
            &Expr::IsNull(Box::new(Expr::Field(0))),
        )?),
    });
    let cell_policy_key = catalog_id_key(13, cell_policy_id);
    writes.push(CatalogWrite {
        key: cell_policy_key.clone(),
        expected_value: None,
        payload: Some(catalog_record_payload(
            13,
            &cell_policy_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U16,
                    payload: 2u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::U16,
                    payload: 2u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Id,
                    payload: cell_policy_expression_id.to_vec(),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ],
        )?),
    });
    for (target_kind, target_id) in [(2, role_id), (3, column_uuid_id)] {
        let (key, payload) = policy_target_catalog_record(cell_policy_id, target_kind, target_id);
        writes.push(CatalogWrite {
            key,
            expected_value: None,
            payload: Some(payload),
        });
        if target_kind == 2 {
            writes.push(dependency_catalog_write(cell_policy_id, target_id, 5)?);
        }
    }
    let check_expression_key = catalog_id_key(7, check_expression_id);
    writes.push(CatalogWrite {
        key: check_expression_key.clone(),
        expected_value: None,
        payload: Some(expression_catalog_payload_full(
            check_expression_id,
            2,
            boolean_type_id,
            [0; 8],
            &[column_uuid_id],
            &Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Uuid(principal_id)))),
            ),
        )?),
    });
    let (check_constraint_key, check_constraint_payload) =
        check_constraint_catalog_record(check_constraint_id, table_id, check_expression_id);
    writes.push(CatalogWrite {
        key: check_constraint_key,
        expected_value: None,
        payload: Some(check_constraint_payload),
    });
    let (not_null_constraint_key, not_null_constraint_payload) =
        not_null_constraint_catalog_record([180; 16], table_id, column_text_id);
    writes.push(CatalogWrite {
        key: not_null_constraint_key,
        expected_value: None,
        payload: Some(not_null_constraint_payload),
    });
    let cell_allow_expression_key = catalog_id_key(7, cell_allow_expression_id);
    writes.push(CatalogWrite {
        key: cell_allow_expression_key.clone(),
        expected_value: None,
        payload: Some(expression_catalog_payload_full(
            cell_allow_expression_id,
            3,
            boolean_type_id,
            [0; 8],
            &[],
            &Expr::Literal(Some(ScalarValue::Boolean(true))),
        )?),
    });
    let cell_allow_policy_key = catalog_id_key(13, cell_allow_policy_id);
    writes.push(CatalogWrite {
        key: cell_allow_policy_key.clone(),
        expected_value: None,
        payload: Some(catalog_record_payload(
            13,
            &cell_allow_policy_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U16,
                    payload: 2u16.to_le_bytes().to_vec(),
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
                    payload: cell_allow_expression_id.to_vec(),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ],
        )?),
    });
    for (target_kind, target_id) in [(2, role_id), (3, column_uuid_id)] {
        let (key, payload) =
            policy_target_catalog_record(cell_allow_policy_id, target_kind, target_id);
        writes.push(CatalogWrite {
            key,
            expected_value: None,
            payload: Some(payload),
        });
        if target_kind == 2 {
            writes.push(dependency_catalog_write(
                cell_allow_policy_id,
                target_id,
                5,
            )?);
        }
    }
    for (object_id, object_kind) in [
        (varchar_type_id, 2),
        (uuid_type_id, 2),
        (boolean_type_id, 2),
        (schema_id, 1),
        (table_id, 3),
        (column_text_id, 4),
        (column_uuid_id, 4),
        (policy_expression_id, 5),
        (cell_policy_expression_id, 5),
        (check_expression_id, 5),
        (cell_allow_expression_id, 5),
        (check_constraint_id, 6),
        ([180; 16], 6),
        (policy_id, 10),
        (cell_policy_id, 10),
        (cell_allow_policy_id, 10),
    ] {
        writes.push(object_catalog_write_for(object_id, object_kind));
    }
    for (dependent_id, referenced_id) in [
        (policy_expression_id, column_uuid_id),
        (cell_policy_expression_id, column_uuid_id),
        (check_expression_id, column_uuid_id),
        (policy_id, policy_expression_id),
        (cell_policy_id, cell_policy_expression_id),
        (check_constraint_id, check_expression_id),
        (cell_allow_policy_id, cell_allow_expression_id),
    ] {
        writes.push(dependency_catalog_write(dependent_id, referenced_id, 2)?);
    }
    let mut audit_targets = vec![
        principal_id,
        table_id,
        schema_id,
        storage_id,
        varchar_type_id,
        uuid_type_id,
        column_text_id,
        column_uuid_id,
        boolean_type_id,
        policy_id,
        policy_expression_id,
        role_id,
        cell_policy_id,
        cell_policy_expression_id,
        cell_allow_policy_id,
        cell_allow_expression_id,
        check_constraint_id,
        check_expression_id,
    ];
    audit_targets.sort_unstable();
    let audit = AuditEvent {
        event_id: [138; 16],
        actor_id: Some([9; 16]),
        correlation_id: [139; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: audit_targets,
    };
    commit_catalog_and_audit(&database, [140; 16], [9; 16], 9_999, &writes, &[audit])?;

    let new_column_id = [164; 16];
    let new_column_key = catalog_id_key(6, new_column_id);
    let mut varchar_parameters = [0; 8];
    varchar_parameters[4..].copy_from_slice(&32u32.to_le_bytes());
    let new_column_payload = catalog_record_payload(
        6,
        &new_column_key,
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
                payload: format!("object_{}", new_column_id[0]).into_bytes(),
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
                payload: 2u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let audit = AuditEvent {
        event_id: [165; 16],
        actor_id: Some(principal_id),
        correlation_id: [166; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![new_column_id],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [167; 16],
            principal_id,
            9_999,
            &[
                CatalogWrite {
                    key: new_column_key,
                    expected_value: None,
                    payload: Some(new_column_payload),
                },
                object_catalog_write_for(new_column_id, 4)
            ],
            &[audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let (unbumped_constraint_key, unbumped_constraint_payload) =
        check_constraint_catalog_record([175; 16], table_id, check_expression_id);
    let unbumped_constraint_dependency =
        dependency_catalog_write([175; 16], check_expression_id, 2)?;
    let audit = AuditEvent {
        event_id: [176; 16],
        actor_id: Some(principal_id),
        correlation_id: [177; 16],
        occurred_at_us: 1_700_000_000_000_002,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[175; 16]],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [178; 16],
            principal_id,
            9_999,
            &[
                CatalogWrite {
                    key: unbumped_constraint_key,
                    expected_value: None,
                    payload: Some(unbumped_constraint_payload),
                },
                object_catalog_write_for([175; 16], 6),
                unbumped_constraint_dependency,
            ],
            &[audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let (_, new_policy_target) = policy_target_catalog_record(policy_id, 2, [162; 16]);
    let updated_policy_payload = catalog_record_payload(
        13,
        &policy_key,
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
                payload: policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 2u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let audit = AuditEvent {
        event_id: [145; 16],
        actor_id: Some(principal_id),
        correlation_id: [152; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![policy_id, [162; 16]],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [153; 16],
            principal_id,
            9_999,
            &[
                CatalogWrite {
                    key: policy_key.clone(),
                    expected_value: Some(policy_payload),
                    payload: Some(updated_policy_payload),
                },
                CatalogWrite {
                    key: policy_target_catalog_record(policy_id, 2, [162; 16]).0,
                    expected_value: None,
                    payload: Some(new_policy_target),
                },
            ],
            &[audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let (unbumped_grant_key, unbumped_grant) =
        grant_catalog_record(principal_id, table_id, 3, 1, table_id, principal_id, 1);
    let audit = AuditEvent {
        event_id: [146; 16],
        actor_id: Some(principal_id),
        correlation_id: [147; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![principal_id, table_id],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [148; 16],
            principal_id,
            9_999,
            &[CatalogWrite {
                key: unbumped_grant_key,
                expected_value: None,
                payload: Some(unbumped_grant),
            }],
            &[audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let mut invocation =
        InvocationTransaction::begin(&database, [141; 16], principal_id, 9_999, 1, 128)?;
    for (family, object_id) in [
        (4, varchar_type_id),
        (4, uuid_type_id),
        (5, table_id),
        (6, column_text_id),
        (6, column_uuid_id),
    ] {
        assert!(
            invocation
                .read_catalog_record(&catalog_id_key(family, object_id))?
                .is_some()
        );
    }
    let loaded = invocation.load_catalog_table_schema(table_id)?;
    let checks = invocation.load_catalog_check_constraints(&loaded)?;
    assert_eq!(
        checks,
        [Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Uuid(principal_id)))),
        )]
    );
    let (roles, grants) = invocation.load_catalog_grants(principal_id, &loaded)?;
    assert_eq!(roles, std::collections::HashSet::from([role_id]));
    assert_eq!(grants.len(), 5);
    assert!(grants.contains(&Grant {
        subject: Subject::Principal(principal_id),
        operation: Operation::Usage,
        target: AccessTarget::Schema(schema_id),
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Role(role_id),
        operation: Operation::Insert,
        target: AccessTarget::Column {
            table_id,
            column_id: column_text_id,
        },
    }));
    let policies = invocation.load_catalog_policies(principal_id, &loaded)?;
    assert_eq!(policies.len(), 3);
    assert!(policies.contains(&Policy {
        subjects: vec![Subject::Principal(principal_id), Subject::Role(role_id)],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::CurrentPrincipalId),
        )),
        with_check: None,
    }));
    assert!(policies.contains(&Policy {
        subjects: vec![Subject::Role(role_id)],
        operation: Operation::Select,
        target: AccessTarget::Column {
            table_id,
            column_id: column_uuid_id,
        },
        effect: Effect::Restrict,
        using: Some(Expr::IsNull(Box::new(Expr::Field(0)))),
        with_check: None,
    }));
    assert!(policies.contains(&Policy {
        subjects: vec![Subject::Role(role_id)],
        operation: Operation::Select,
        target: AccessTarget::Column {
            table_id,
            column_id: column_uuid_id,
        },
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    }));
    let query_context = AuthorizationContext {
        principal_id,
        role_ids: roles.clone(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: column_uuid_id,
                },
            ),
        ]),
    };
    let projection = prdb_engine::authorization::project_row(
        &query_context,
        &grants,
        &policies,
        schema_id,
        table_id,
        &loaded.column_ids,
        &[
            Some(ScalarValue::Uuid(principal_id)),
            Some(ScalarValue::Varchar("private".to_owned())),
        ],
    )?
    .unwrap();
    assert!(matches!(projection[0], SanitizedCell::Redacted));
    assert!(matches!(projection[1], SanitizedCell::Redacted));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal(principal_id),
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal(principal_id),
        operation: Operation::Update,
        target: AccessTarget::Column {
            table_id,
            column_id: column_uuid_id,
        },
    }));
    assert_eq!(loaded.table_id, table_id);
    assert_eq!(loaded.schema_id, schema_id);
    assert_eq!(loaded.schema_version, 2);
    assert_eq!(loaded.column_ids, [column_uuid_id, column_text_id]);
    assert_eq!(loaded.column_type_ids, [uuid_type_id, varchar_type_id]);
    assert_eq!(loaded.column_default_masks, [7, 1]);
    assert_eq!(loaded.columns[0].scalar_type, ScalarType::Uuid);
    assert!(loaded.columns[0].nullable);
    assert_eq!(
        loaded.columns[1].scalar_type,
        ScalarType::Varchar { max_chars: 32 }
    );
    assert!(!loaded.columns[1].nullable);

    let changed_column_key = catalog_id_key(6, column_text_id);
    let changed_column = catalog_record_payload(
        6,
        &changed_column_key,
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
                payload: b"object_136".to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: varchar_type_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: {
                    let mut parameters = [0; 8];
                    parameters[4..].copy_from_slice(&16u32.to_le_bytes());
                    parameters.to_vec()
                },
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
    let audit = AuditEvent {
        event_id: [142; 16],
        actor_id: Some([9; 16]),
        correlation_id: [143; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![table_id, column_text_id],
    };
    let key = changed_column_key;
    let current = database.begin_read()?;
    let previous = current
        .open_table(CATALOG)?
        .get(&key[..])?
        .unwrap()
        .value()
        .to_vec();
    let table_key = catalog_id_key(5, table_id);
    let previous_table = current
        .open_table(CATALOG)?
        .get(&table_key[..])?
        .unwrap()
        .value()
        .to_vec();
    drop(current);
    commit_catalog_and_audit(
        &database,
        [144; 16],
        [9; 16],
        9_999,
        &[
            CatalogWrite {
                key,
                expected_value: Some(previous),
                payload: Some(changed_column),
            },
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(previous_table),
                payload: Some(catalog_record_payload(
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
                            payload: 3u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U64,
                            payload: 4u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: storage_id.to_vec(),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                    ],
                )?),
            },
        ],
        &[audit],
    )?;
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    Ok(())
}

#[test]
fn catalog_authority_rejects_disabled_or_role_invocation_principals()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let records = [
        principal_catalog_record_for([219; 16], 1, b"disabled", false, 1),
        principal_catalog_record_for([220; 16], 2, b"role", false, 1),
        principal_catalog_record_for([221; 16], 1, b"enabled", true, 1),
    ];
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(CATALOG)?;
            for (key, value) in &records {
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
            for (principal_id, kind) in [([219; 16], 1), ([220; 16], 2), ([221; 16], 1)] {
                let (key, value) = principal_object_record_for(principal_id, kind);
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
        }
        {
            let mut principal_names = write.open_table(PRINCIPAL_NAMES)?;
            for principal_id in [[219; 16], [220; 16], [221; 16]] {
                principal_names.insert(
                    principal_test_name(principal_id).as_bytes(),
                    &principal_id[..],
                )?;
            }
        }
        write.commit()?;
    }
    for (invocation_id, principal_id) in [([222; 16], [219; 16]), ([223; 16], [220; 16])] {
        let mut transaction =
            InvocationTransaction::begin(&database, invocation_id, principal_id, 9_999, 1, 64)?;
        assert!(matches!(
            transaction.load_catalog_role_closure(principal_id),
            Err(TransactionError::Authorization(
                prdb_engine::authorization::AuthorizationError::Denied
            ))
        ));
        assert_eq!(
            prdb_engine::outcome::lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted
        );
    }
    let mut valid = InvocationTransaction::begin(&database, [224; 16], [221; 16], 9_999, 1, 64)?;
    assert!(valid.load_catalog_role_closure([221; 16])?.is_empty());
    valid.stage_put(id(225), b"pending")?;
    let (principal_key, disabled_payload) =
        principal_catalog_record_for([221; 16], 1, b"enabled", false, 2);
    let audit = AuditEvent {
        event_id: [226; 16],
        actor_id: Some([9; 16]),
        correlation_id: [227; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![[221; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [228; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: principal_key,
            expected_value: Some(records[2].1.clone()),
            payload: Some(disabled_payload),
        }],
        &[audit],
    )?;
    assert!(matches!(
        valid.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&key(id(225))[..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn table_epochs_cannot_move_back_after_catalog_changes() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [239; 16];
    let table_key = catalog_id_key(5, table_id);
    let field = |tag, wire_type, payload| CatalogField {
        tag,
        wire_type,
        payload,
    };
    let table_payload = |schema_version: u64, write_epoch: u64| {
        catalog_record_payload(
            5,
            &table_key,
            &[
                field(1, WireType::Id, vec![240; 16]),
                field(2, WireType::U64, schema_version.to_le_bytes().to_vec()),
                field(3, WireType::U64, write_epoch.to_le_bytes().to_vec()),
                field(4, WireType::Id, vec![241; 16]),
                field(5, WireType::U16, 1u16.to_le_bytes().to_vec()),
            ],
        )
    };
    let initial = table_payload(1, 1)?;
    let schema_bumped = table_payload(2, 1)?;
    let write_bumped = table_payload(2, 2)?;
    {
        let write = database.begin_write()?;
        {
            write
                .open_table(CATALOG)?
                .insert(table_key.as_slice(), initial.as_slice())?;
            let (object_key, object_value) = object_catalog_record_for(table_id, 3);
            write
                .open_table(CATALOG)?
                .insert(object_key.as_slice(), object_value.as_slice())?;
        }
        write.commit()?;
    }
    let mut stale = InvocationTransaction::begin(&database, [242; 16], [9; 16], 9_999, 1, 64)?;
    stale.read_catalog_record(&table_key)?;
    stale.stage_put(id(243), b"pending")?;
    let column_key = catalog_id_key(6, [244; 16]);
    let column = catalog_record_payload(
        6,
        &column_key,
        &[
            field(1, WireType::Id, table_id.to_vec()),
            field(2, WireType::U32, 0u32.to_le_bytes().to_vec()),
            field(3, WireType::Text, format!("object_{}", 244).into_bytes()),
            field(4, WireType::Id, vec![245; 16]),
            field(5, WireType::Bytes, vec![0; 8]),
            field(6, WireType::Bool, vec![1]),
            field(8, WireType::U16, 0u16.to_le_bytes().to_vec()),
            field(9, WireType::U64, 2u64.to_le_bytes().to_vec()),
        ],
    )?;
    let audit = |n| AuditEvent {
        event_id: [n; 16],
        actor_id: Some([9; 16]),
        correlation_id: [n + 1; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![table_id],
    };
    commit_catalog_and_audit(
        &database,
        [246; 16],
        [9; 16],
        9_999,
        &[
            CatalogWrite {
                key: column_key,
                expected_value: None,
                payload: Some(column),
            },
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(initial.clone()),
                payload: Some(schema_bumped.clone()),
            },
            object_catalog_write_for([244; 16], 4),
        ],
        &[audit(247)],
    )?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [249; 16],
            [9; 16],
            9_999,
            &[CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(schema_bumped.clone()),
                payload: Some(initial)
            },],
            &[audit(250)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    commit_catalog_and_audit(
        &database,
        [252; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(schema_bumped.clone()),
            payload: Some(write_bumped.clone()),
        }],
        &[audit(253)],
    )?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [30; 16],
            [9; 16],
            9_999,
            &[CatalogWrite {
                key: table_key,
                expected_value: Some(write_bumped),
                payload: Some(schema_bumped)
            },],
            &[audit(31)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert!(matches!(
        stale.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&key(id(243))[..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn new_membership_cannot_be_hidden_by_restoring_principal_epoch()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user = [230; 16];
    let role = [231; 16];
    let old = principal_catalog_record_for(user, 1, b"user", true, 1);
    let new = principal_catalog_record_for(user, 1, b"user", true, 2);
    let role_record = principal_catalog_record_for(role, 2, b"role", false, 1);
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(CATALOG)?;
            for (key, value) in [&old, &role_record] {
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
            for (principal_id, kind) in [(user, 1), (role, 2)] {
                let (key, value) = principal_object_record_for(principal_id, kind);
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
        }
        {
            let mut principal_names = write.open_table(PRINCIPAL_NAMES)?;
            for principal_id in [user, role] {
                principal_names.insert(
                    principal_test_name(principal_id).as_bytes(),
                    &principal_id[..],
                )?;
            }
        }
        write.commit()?;
    }
    let mut stale = InvocationTransaction::begin(&database, [232; 16], user, 9_999, 1, 64)?;
    assert!(stale.load_catalog_role_closure(user)?.is_empty());
    stale.stage_put(id(233), b"pending")?;
    let (membership_key, membership_payload) = membership_catalog_record(user, role, user, 1);
    let audit = |n| AuditEvent {
        event_id: [n; 16],
        actor_id: Some(user),
        correlation_id: [n + 1; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![user, role],
    };
    commit_catalog_and_audit(
        &database,
        [234; 16],
        user,
        9_999,
        &[
            CatalogWrite {
                key: membership_key,
                expected_value: None,
                payload: Some(membership_payload),
            },
            dependency_catalog_write(user, role, 5)?,
            CatalogWrite {
                key: new.0.clone(),
                expected_value: Some(old.1.clone()),
                payload: Some(new.1.clone()),
            },
        ],
        &[audit(235)],
    )?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [237; 16],
            user,
            9_999,
            &[CatalogWrite {
                key: old.0.clone(),
                expected_value: Some(new.1),
                payload: Some(old.1)
            },],
            &[audit(238)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert!(matches!(
        stale.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&key(id(233))[..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn invocation_loads_and_guards_transitive_catalog_role_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user_id = [151; 16];
    let first_role = [152; 16];
    let second_role = [153; 16];
    let principals = [
        principal_catalog_record_for(user_id, 1, b"user", true, 1),
        principal_catalog_record_for(first_role, 2, b"first", false, 1),
        principal_catalog_record_for(second_role, 2, b"second", false, 1),
    ];
    let memberships = [
        membership_catalog_record(user_id, first_role, user_id, 1),
        membership_catalog_record(first_role, second_role, user_id, 1),
    ];
    let mut writes = Vec::new();
    for (key, payload) in principals.iter().chain(&memberships) {
        writes.push(CatalogWrite {
            key: key.clone(),
            expected_value: None,
            payload: Some(payload.clone()),
        });
    }
    for (principal_id, kind) in [(user_id, 1), (first_role, 2), (second_role, 2)] {
        let (key, payload) = principal_object_record_for(principal_id, kind);
        writes.push(CatalogWrite {
            key,
            expected_value: None,
            payload: Some(payload),
        });
    }
    writes.push(dependency_catalog_write(user_id, first_role, 5)?);
    writes.push(dependency_catalog_write(first_role, second_role, 5)?);
    let audit = AuditEvent {
        event_id: [154; 16],
        actor_id: Some(user_id),
        correlation_id: [155; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![user_id, first_role, second_role],
    };
    commit_catalog_and_audit(&database, [156; 16], user_id, 9_999, &writes, &[audit])?;

    let mut invocation =
        InvocationTransaction::begin(&database, [157; 16], user_id, 9_999, 1, 128)?;
    let closure = invocation.load_catalog_role_closure(user_id)?;
    assert_eq!(
        closure,
        std::collections::HashSet::from([first_role, second_role])
    );

    let (membership_key, updated_membership) =
        membership_catalog_record(user_id, first_role, user_id, 2);
    let read = database.begin_read()?;
    let previous = read
        .open_table(CATALOG)?
        .get(&membership_key[..])?
        .unwrap()
        .value()
        .to_vec();
    drop(read);
    let audit = AuditEvent {
        event_id: [158; 16],
        actor_id: Some(user_id),
        correlation_id: [159; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![user_id, first_role],
    };
    let updated_user = principal_catalog_record_for(user_id, 1, b"user", true, 2);
    commit_catalog_and_audit(
        &database,
        [160; 16],
        user_id,
        9_999,
        &[
            CatalogWrite {
                key: membership_key,
                expected_value: Some(previous),
                payload: Some(updated_membership),
            },
            CatalogWrite {
                key: updated_user.0.clone(),
                expected_value: Some(principals[0].1.clone()),
                payload: Some(updated_user.1.clone()),
            },
        ],
        &[audit],
    )?;
    let rollback_audit = AuditEvent {
        event_id: [161; 16],
        actor_id: Some(user_id),
        correlation_id: [162; 16],
        occurred_at_us: 1_700_000_000_000_002,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![user_id],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [163; 16],
            user_id,
            9_999,
            &[CatalogWrite {
                key: updated_user.0,
                expected_value: Some(updated_user.1),
                payload: Some(principals[0].1.clone())
            }],
            &[rollback_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    Ok(())
}

#[test]
fn authorized_insert_checks_non_generated_fields_and_allows_generated_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(70);
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Uuid,
            nullable: false,
        },
    ];
    let column_ids = [[71; 16], [72; 16]];
    let values = [
        Some(ScalarValue::Text("new row".to_owned())),
        Some(ScalarValue::Uuid([73; 16])),
    ];
    let table = AccessTarget::Table(row_id.table);
    let name = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[74; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Insert, table),
            (Operation::Insert, name),
        ]),
    };
    let usage_grant = Grant {
        subject: Subject::Role([74; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(row_id.table),
    };
    let table_grant = Grant {
        subject: Subject::Role([74; 16]),
        operation: Operation::Insert,
        target: table,
    };
    let name_grant = Grant {
        subject: Subject::Role([74; 16]),
        operation: Operation::Insert,
        target: name,
    };
    let policies = [
        Policy {
            subjects: vec![Subject::Role([74; 16])],
            operation: Operation::Insert,
            target: table,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Role([74; 16])],
            operation: Operation::Insert,
            target: name,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let denied_grants = [usage_grant, table_grant];
    let denied_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &denied_grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut denied = InvocationTransaction::begin(&database, [75; 16], [9; 16], 9_999, 2, 128)?;
    assert!(matches!(
        denied.stage_insert_authorized(row_id, &denied_projection, &values, &[column_ids[1]]),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));

    let allowed_grants = [usage_grant, table_grant, name_grant];
    let allowed_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &allowed_grants,
        policies: &policies,
        checks: &[Expr::NotEqual(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Text("invalid".to_owned())))),
        )],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut invalid_values = values.clone();
    invalid_values[0] = Some(ScalarValue::Text("invalid".to_owned()));
    let mut constraint_denied =
        InvocationTransaction::begin(&database, [77; 16], [9; 16], 9_999, 2, 128)?;
    assert!(matches!(
        constraint_denied.stage_insert_authorized(
            row_id,
            &allowed_projection,
            &invalid_values,
            &[column_ids[1]],
        ),
        Err(TransactionError::ConstraintViolation)
    ));
    let outcome_read = database.begin_read()?;
    assert_eq!(
        InvocationOutcome::decode(
            outcome_read
                .open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[77; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    let mut allowed = InvocationTransaction::begin(&database, [76; 16], [9; 16], 9_999, 2, 128)?;
    allowed.stage_insert_authorized(row_id, &allowed_projection, &values, &[column_ids[1]])?;
    allowed.commit()?;

    let read = database.begin_read()?;
    let record = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(row_id)[..])?
            .unwrap()
            .value(),
    )?;
    assert_eq!(decode_tuple(&columns, 1, &record.payload)?.values, values);
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[75; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn authorized_write_revalidates_projection_catalog_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(80);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[81; 16]];
    commit_rows(
        &database,
        [82; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text("before".to_owned()))],
            )?),
        }],
    )?;
    let (principal_key, principal_v1) = principal_catalog_record(1);
    let (object_key, object_value) = principal_object_record_for([7; 16], 1);
    let write = database.begin_write()?;
    write
        .open_table(CATALOG)?
        .insert(&principal_key[..], &principal_v1[..])?;
    write
        .open_table(CATALOG)?
        .insert(&object_key[..], &object_value[..])?;
    write
        .open_table(PRINCIPAL_NAMES)?
        .insert(principal_test_name([7; 16]).as_bytes(), &[7; 16][..])?;
    write.commit()?;

    let table = AccessTarget::Table(row_id.table);
    let cell = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[83; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, table),
            (Operation::Update, table),
            (Operation::Select, cell),
            (Operation::Update, cell),
        ]),
    };
    let column_grants = [Operation::Select, Operation::Update].map(|operation| Grant {
        subject: Subject::Role([83; 16]),
        operation,
        target: cell,
    });
    let mut policies = Vec::new();
    for operation in [Operation::Select, Operation::Update] {
        policies.push(Policy {
            subjects: vec![Subject::Role([83; 16])],
            operation,
            target: table,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: (operation == Operation::Update)
                .then_some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        });
        policies.push(Policy {
            subjects: vec![Subject::Role([83; 16])],
            operation,
            target: cell,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: (operation == Operation::Update)
                .then_some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        });
    }
    let select_table_grant = Grant {
        subject: Subject::Role([83; 16]),
        operation: Operation::Select,
        target: table,
    };
    let update_table_grant = Grant {
        subject: Subject::Role([83; 16]),
        operation: Operation::Update,
        target: table,
    };
    let usage_grant = Grant {
        subject: Subject::Role([83; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(row_id.table),
    };
    let grants = [
        column_grants[0],
        column_grants[1],
        select_table_grant,
        update_table_grant,
        usage_grant,
    ];
    let dependencies = [AuthorityDependency {
        key: principal_key.clone(),
        expected_value: Some(principal_v1.clone()),
    }];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &dependencies,
    };
    let mut invocation = InvocationTransaction::begin(&database, [84; 16], [9; 16], 9_999, 2, 128)?;
    invocation.stage_patch_authorized(
        row_id,
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Text("stale authorization".to_owned())),
        }],
    )?;

    let (_, principal_v2) = principal_catalog_record(2);
    let authority_change = AuditEvent {
        event_id: [85; 16],
        actor_id: Some([9; 16]),
        correlation_id: [86; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![[7; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [87; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: principal_key,
            expected_value: Some(principal_v1),
            payload: Some(principal_v2),
        }],
        &[authority_change],
    )?;

    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    let read = database.begin_read()?;
    let tuple = decode_tuple(
        &columns,
        1,
        &RowRecord::decode(
            read.open_table(ROWS)?
                .get(&key(row_id)[..])?
                .unwrap()
                .value(),
        )?
        .payload,
    )?;
    assert_eq!(tuple.values, [Some(ScalarValue::Text("before".to_owned()))]);
    assert_eq!(commit_sequence(&database)?, 2);
    Ok(())
}
