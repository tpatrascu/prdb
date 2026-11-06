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
use prdb_engine::schema::{self, CATALOG, ENGINE_META, ROWS};
use prdb_engine::transaction::{InsertInput, InvocationTransaction, TransactionError};
use prdb_engine::tuple::{
    ColumnSchema, FieldPatch, ScalarType, ScalarValue, decode_tuple, encode_tuple,
};
use redb::{Database, ReadableDatabase};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

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
                    payload: 8u16.to_le_bytes().to_vec(),
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
    {
        let write = database.begin_write()?;
        let mut catalog = write.open_table(CATALOG)?;
        catalog.insert(index_key.as_slice(), index_value.as_slice())?;
        catalog.insert(constraint_key.as_slice(), constraint_value.as_slice())?;
        drop(catalog);
        write.commit()?;
    }

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
        &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
    )?;
    invocation.stage_put(id(243), &payload)?;
    invocation.stage_put(id(244), &payload)?;
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::UniqueViolation))
    ));

    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(243))[..])?.is_none());
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
    assert!(matches!(
        child.commit(),
        Err(TransactionError::Commit(CommitError::ForeignKeyViolation))
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
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(encode_tuple(&columns, 1, &original)?),
        }],
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
    assert!(matches!(
        constraint_denied.stage_patch_authorized(
            row_id,
            &projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text("forbidden".to_owned())),
            }]
        ),
        Err(TransactionError::ConstraintViolation)
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
fn catalog_insert_rejects_caller_values_for_generated_identity()
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
        with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
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
    for (invocation_id, input) in [
        (
            [216; 16],
            InsertInput::Value(Some(ScalarValue::Uuid([217; 16]))),
        ),
        ([218; 16], InsertInput::Default),
    ] {
        let mut invocation =
            InvocationTransaction::begin(&database, invocation_id, [9; 16], 9_999, 2, 128)?;
        assert!(matches!(
            invocation.stage_insert_authorized_from_catalog_defaults(
                row_id,
                &projection,
                &[input],
                &schema,
            ),
            Err(TransactionError::GeneratedIdentityUnsupported)
        ));
        assert!(matches!(
            invocation.commit(),
            Err(TransactionError::Aborted)
        ));
        assert_eq!(
            prdb_engine::outcome::lookup_outcome(&database, invocation_id, [9; 16], 1)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted
        );
    }
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(row_id)[..])?.is_none());
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
