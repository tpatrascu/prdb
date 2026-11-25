#![allow(dead_code)]
use prdb_engine::authorization::Expr;
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::commit::{CatalogWrite, RowWrite, commit_rows};
use prdb_engine::expression::encode_expression;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::schema::{self, CATALOG, ENGINE_META};
use redb::{Database, ReadableDatabase};
use std::sync::atomic::{AtomicU64, Ordering};

pub static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct Fixture(pub std::path::PathBuf);

impl Fixture {
    pub fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-txn-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    pub fn path(&self) -> std::path::PathBuf {
        self.0.join("transaction.redb")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

pub fn id(row: u8) -> prdb_engine::overlay::RowId {
    prdb_engine::overlay::RowId {
        table: [0x10; 16],
        row: [row; 16],
    }
}

pub fn builtin_owner_id() -> [u8; 16] {
    builtin_id(3)
}

pub fn builtin_id(code: u16) -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = code as u8;
    id
}

pub fn key(id: prdb_engine::overlay::RowId) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&id.table);
    key[16..].copy_from_slice(&id.row);
    key
}

pub fn numbered_row_id(table: [u8; 16], number: u64) -> prdb_engine::overlay::RowId {
    prdb_engine::overlay::RowId {
        table,
        row: u128::from(number).to_be_bytes(),
    }
}

pub fn expression_catalog_payload(
    expression_id: [u8; 16],
    expression: &Expr,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    expression_catalog_payload_with_purpose(expression_id, 1, expression)
}

pub fn expression_catalog_payload_with_purpose(
    expression_id: [u8; 16],
    purpose: u16,
    expression: &Expr,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    expression_catalog_payload_full(expression_id, purpose, [45; 16], [0; 8], &[], expression)
}

pub fn expression_catalog_payload_full(
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

pub fn catalog_id_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

pub fn catalog_tombstone_write(
    object_id: [u8; 16],
) -> Result<CatalogWrite, Box<dyn std::error::Error>> {
    let key = catalog_id_key(17, object_id);
    Ok(CatalogWrite {
        payload: Some(catalog_record_payload(17, &key, &[])?),
        key,
        expected_value: None,
    })
}

pub fn dependency_catalog_write(
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

pub fn catalog_record_payload(
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

pub fn index_catalog_record(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_id: [u8; 16],
    schema_version: u64,
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    index_catalog_record_with_constraint(index_id, table_id, column_id, schema_version, false, None)
}

pub fn index_catalog_record_with_constraint(
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

pub fn unique_constraint_catalog_record(
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

pub fn principal_catalog_record(authority_epoch: u64) -> (Vec<u8>, Vec<u8>) {
    principal_catalog_record_for([7; 16], 1, b"user", true, authority_epoch)
}

pub fn principal_catalog_record_for(
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

pub fn principal_object_record_for(
    principal_id: [u8; 16],
    principal_kind: u16,
) -> (Vec<u8>, Vec<u8>) {
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

pub fn object_catalog_record_for(object_id: [u8; 16], object_kind: u16) -> (Vec<u8>, Vec<u8>) {
    object_catalog_record_with_name(object_id, object_kind, &format!("object_{}", object_id[0]))
}

pub fn principal_test_name(principal_id: [u8; 16]) -> String {
    if principal_id == builtin_owner_id() {
        return "database_admin".to_owned();
    }
    let suffix = principal_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("principal_{suffix}")
}

pub fn object_catalog_record_with_name(
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

pub fn object_catalog_write_for(object_id: [u8; 16], object_kind: u16) -> CatalogWrite {
    let (key, payload) = object_catalog_record_for(object_id, object_kind);
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn boolean_type_catalog_write(
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

pub fn membership_catalog_record(
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

pub fn grant_catalog_record(
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

pub fn policy_target_catalog_record(
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

pub fn policy_catalog_record(
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

pub fn check_constraint_catalog_record(
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

pub fn not_null_constraint_catalog_record(
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

pub fn initialize(path: &std::path::Path) -> Result<Database, Box<dyn std::error::Error>> {
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

pub fn install_relation_discovery_catalog(
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

pub fn install_second_relation_column(
    database: &Database,
) -> Result<(), Box<dyn std::error::Error>> {
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

pub fn stage_initial_row(database: &Database) -> Result<(), Box<dyn std::error::Error>> {
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

pub fn commit_sequence(database: &Database) -> Result<u64, Box<dyn std::error::Error>> {
    let transaction = database.begin_read()?;
    let bytes = transaction
        .open_table(ENGINE_META)?
        .get(&b"format"[..])?
        .unwrap()
        .value()
        .to_vec();
    Ok(EngineMetadata::decode(&bytes)?.commit_sequence)
}
