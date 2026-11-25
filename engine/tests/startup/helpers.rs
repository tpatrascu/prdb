#![allow(dead_code)]
use prdb_engine::authorization::Expr;
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::expression::encode_expression;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ENGINE_META, PRINCIPAL_NAMES, ROWS};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadableTable};
use std::sync::atomic::{AtomicU64, Ordering};

pub static NEXT: AtomicU64 = AtomicU64::new(0);

pub fn builtin_owner_id() -> [u8; 16] {
    builtin_id(3)
}

pub fn builtin_id(code: u16) -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = code as u8;
    id
}

pub struct Fixture(pub std::path::PathBuf);

impl Fixture {
    pub fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-startup-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self) -> std::path::PathBuf {
        self.0.join("startup.redb")
    }

    pub fn database(&self, commit_sequence: u64) -> Database {
        let database = Database::create(self.path()).unwrap();
        let write = database.begin_write().unwrap();
        schema::create_tables(&write).unwrap();
        let metadata = EngineMetadata {
            database_id: [0x11; 16],
            catalog_version: 2,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence,
            lifecycle: Lifecycle::Ready,
        }
        .encode()
        .unwrap();
        let (database_key, database_value) = database_record([0x11; 16], 2, 1, 1);
        write
            .open_table(CATALOG)
            .unwrap()
            .insert(&database_key[..], &database_value[..])
            .unwrap();
        let owner_id = builtin_owner_id();
        let (principal_key, principal_value) =
            principal_record_with_name(owner_id, 2, false, 1, "database_admin");
        let (owner_object_key, owner_object_value) =
            object_record_named(owner_id, 9, owner_id, "database_admin");
        write
            .open_table(CATALOG)
            .unwrap()
            .insert(&principal_key[..], &principal_value[..])
            .unwrap();
        write
            .open_table(PRINCIPAL_NAMES)
            .unwrap()
            .insert(&b"database_admin"[..], &owner_id[..])
            .unwrap();
        write
            .open_table(CATALOG)
            .unwrap()
            .insert(&owner_object_key[..], &owner_object_value[..])
            .unwrap();
        for (code, name) in [(1u16, "pg_catalog"), (2, "information_schema")] {
            let schema_id = builtin_id(code);
            let (schema_key, schema_value) = schema_record(schema_id);
            let (object_key, object_value) = object_record_named(schema_id, 1, owner_id, name);
            write
                .open_table(CATALOG)
                .unwrap()
                .insert(&schema_key[..], &schema_value[..])
                .unwrap();
            write
                .open_table(CATALOG)
                .unwrap()
                .insert(&object_key[..], &object_value[..])
                .unwrap();
        }
        for builtin_kind in 1u16..=11 {
            let type_id = builtin_id(100 + builtin_kind);
            let (type_key, type_value) = builtin_type_record(type_id, builtin_kind);
            let (object_key, object_value) = object_record(type_id, 2);
            write
                .open_table(CATALOG)
                .unwrap()
                .insert(&type_key[..], &type_value[..])
                .unwrap();
            write
                .open_table(CATALOG)
                .unwrap()
                .insert(&object_key[..], &object_value[..])
                .unwrap();
        }
        write
            .open_table(ENGINE_META)
            .unwrap()
            .insert(&b"format"[..], &metadata[..])
            .unwrap();
        write.commit().unwrap();
        database
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

pub fn row_key(table_id: [u8; 16], row_id: [u8; 16]) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&table_id);
    key[16..].copy_from_slice(&row_id);
    key
}

pub fn builtin_mapping() -> Vec<u8> {
    let codes = [1u16, 2, 3]
        .into_iter()
        .chain(101u16..=111)
        .chain(201u16..=217);
    let mut mapping = 31u32.to_le_bytes().to_vec();
    for code in codes {
        mapping.extend_from_slice(&code.to_le_bytes());
        mapping.extend_from_slice(&0u16.to_le_bytes());
        mapping.extend_from_slice(&builtin_id(code));
    }
    mapping
}

pub fn database_record(
    database_id: [u8; 16],
    catalog_version: u32,
    expression_version: u32,
    builtin_semantics_version: u32,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 1u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&database_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Text,
            payload: b"test_database".to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U32,
            payload: catalog_version.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Bytes,
            payload: builtin_mapping(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U32,
            payload: expression_version.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::U32,
            payload: builtin_semantics_version.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(1).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn object_record(id: [u8; 16], kind: u16) -> (Vec<u8>, Vec<u8>) {
    object_record_with_parent(id, kind, None)
}

pub fn object_record_with_parent(
    id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
) -> (Vec<u8>, Vec<u8>) {
    object_record_with_parent_and_owner(id, kind, parent_id, builtin_owner_id())
}

pub fn object_record_with_owner(id: [u8; 16], kind: u16, owner_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    object_record_with_parent_and_owner(id, kind, None, owner_id)
}

pub fn object_record_with_parent_and_owner(
    id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
    owner_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 2u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let mut fields = vec![CatalogField {
        tag: 1,
        wire_type: WireType::U16,
        payload: kind.to_le_bytes().to_vec(),
    }];
    if let Some(parent_id) = parent_id {
        fields.push(CatalogField {
            tag: 2,
            wire_type: WireType::Id,
            payload: parent_id.to_vec(),
        });
    }
    fields.extend([
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: format!("object_{}", id[0]).into_bytes(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: owner_id.to_vec(),
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
    ]);
    let payload = encode_envelope(&family_schema(2).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn object_record_named(
    id: [u8; 16],
    kind: u16,
    owner_id: [u8; 16],
    name: &str,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 2u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: kind.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: name.as_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: owner_id.to_vec(),
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
    let payload = encode_envelope(&family_schema(2).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn dependency_record(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    kind: u16,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 15u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&dependent_id);
    key.push(1);
    key.extend_from_slice(&referenced_id);
    key.push(1);
    key.extend_from_slice(&kind.to_be_bytes());
    let payload = encode_envelope(&family_schema(15).unwrap(), &key, &[]).unwrap();
    (key, payload)
}

pub fn id_list(ids: &[[u8; 16]]) -> Vec<u8> {
    let mut encoded = (ids.len() as u32).to_le_bytes().to_vec();
    encoded.extend(ids.iter().flatten());
    encoded
}

pub fn constraint_record(
    id: [u8; 16],
    table_id: [u8; 16],
    kind: u16,
    source_columns: &[[u8; 16]],
    referenced_constraint_id: Option<[u8; 16]>,
    referenced_columns: &[[u8; 16]],
    backing_index_id: Option<[u8; 16]>,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 8u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
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
            wire_type: WireType::Ids,
            payload: id_list(source_columns),
        },
    ];
    if let Some(referenced_constraint_id) = referenced_constraint_id {
        fields.push(CatalogField {
            tag: 5,
            wire_type: WireType::Id,
            payload: referenced_constraint_id.to_vec(),
        });
    }
    fields.extend([
        CatalogField {
            tag: 6,
            wire_type: WireType::Ids,
            payload: id_list(referenced_columns),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::U16,
            payload: u16::from(kind == 5).to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 8,
            wire_type: WireType::U16,
            payload: u16::from(kind == 5).to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::U16,
            payload: u16::from(kind == 5).to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
    ]);
    if let Some(backing_index_id) = backing_index_id {
        fields.push(CatalogField {
            tag: 11,
            wire_type: WireType::Id,
            payload: backing_index_id.to_vec(),
        });
    }
    let payload = encode_envelope(&family_schema(8).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn check_constraint_record(
    id: [u8; 16],
    table_id: [u8; 16],
    expression_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 8u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
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
            payload: id_list(&[]),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: expression_id.to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::Ids,
            payload: id_list(&[]),
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
    let payload = encode_envelope(&family_schema(8).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn policy_record(
    id: [u8; 16],
    table_id: [u8; 16],
    using_expression_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    policy_record_with_kind(id, table_id, using_expression_id, 1)
}

pub fn policy_record_with_kind(
    id: [u8; 16],
    table_id: [u8; 16],
    using_expression_id: [u8; 16],
    kind: u16,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 13u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [
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
            payload: using_expression_id.to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(13).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn reverse_index_key(row_key: &[u8; 32], index_key: &[u8]) -> Vec<u8> {
    let mut key = row_key.to_vec();
    key.extend_from_slice(&index_key[..index_key.len() - 16]);
    key
}

pub fn insert_row(database: &Database, key: &[u8; 32], version: u64) {
    let payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"tuple-fixture".to_vec()))],
    )
    .unwrap();
    insert_row_payload(database, key, version, &payload);
}

pub fn insert_row_payload(database: &Database, key: &[u8; 32], version: u64, payload: &[u8]) {
    let bytes = RowRecord {
        row_version: version,
        payload: payload.to_vec(),
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(ROWS)
        .unwrap()
        .insert(&key[..], &bytes[..])
        .unwrap();
    write.commit().unwrap();
}

pub fn index_definition(index_id: [u8; 16], table_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    index_record_for_columns(index_id, table_id, &[[0x66; 16]])
}

pub fn index_record_for_columns(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
) -> (Vec<u8>, Vec<u8>) {
    index_record_for_columns_with_unique(index_id, table_id, column_ids, false)
}

pub fn index_record_for_columns_with_unique(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
    unique: bool,
) -> (Vec<u8>, Vec<u8>) {
    let schema = family_schema(9).unwrap();
    let mut key = 9u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&index_id);
    let columns = id_list(column_ids);
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
            payload: vec![u8::from(unique)],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Id,
            payload: [0x77; 16].to_vec(),
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
    (
        key.clone(),
        encode_envelope(&schema, &key, &fields).unwrap(),
    )
}

pub fn owned_index_record(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_id: [u8; 16],
    constraint_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 9u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&index_id);
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
            payload: vec![1],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::Id,
            payload: constraint_id.to_vec(),
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
    let payload = encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn put_catalog(database: &Database, key: &[u8], value: &[u8]) {
    let write = database.begin_write().unwrap();
    if key.starts_with(&10u16.to_be_bytes()) && key.len() == 19 {
        let mut names = write.open_table(PRINCIPAL_NAMES).unwrap();
        if let Some(current) = write.open_table(CATALOG).unwrap().get(key).unwrap() {
            let record = decode_registered_envelope(current.value()).unwrap();
            let old_name = record.fields.iter().find(|field| field.tag == 2).unwrap();
            names.remove(old_name.payload.as_slice()).unwrap();
        }
        let record = decode_registered_envelope(value).unwrap();
        let name = record.fields.iter().find(|field| field.tag == 2).unwrap();
        names.insert(name.payload.as_slice(), &key[3..19]).unwrap();
    }
    write
        .open_table(CATALOG)
        .unwrap()
        .insert(key, value)
        .unwrap();
    write.commit().unwrap();
}

pub fn identity_tombstone_record(id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let mut key = 17u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let value = encode_envelope(&family_schema(17).unwrap(), &key, &[]).unwrap();
    (key, value)
}

pub fn principal_record(
    id: [u8; 16],
    kind: u16,
    login_enabled: bool,
    epoch: u64,
) -> (Vec<u8>, Vec<u8>) {
    principal_record_with_name(id, kind, login_enabled, epoch, &format!("object_{}", id[0]))
}

pub fn principal_record_with_name(
    id: [u8; 16],
    kind: u16,
    login_enabled: bool,
    epoch: u64,
    name: &str,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 10u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: kind.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Text,
            payload: name.as_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Bool,
            payload: vec![u8::from(login_enabled)],
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::U64,
            payload: epoch.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(10).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn credential_verifier_record(
    token_id: [u8; 16],
    user_id: [u8; 16],
    algorithm: u16,
    profile_version: u16,
    verifier: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 16u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&token_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: user_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U16,
            payload: algorithm.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U16,
            payload: profile_version.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bytes,
            payload: verifier.to_vec(),
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
    ];
    (
        key.clone(),
        encode_envelope(&family_schema(16).unwrap(), &key, &fields).unwrap(),
    )
}

pub fn column_record(id: [u8; 16], name: &str) -> (Vec<u8>, Vec<u8>) {
    column_record_for_table(id, [0x55; 16], name)
}

pub fn column_record_for_table(id: [u8; 16], table_id: [u8; 16], name: &str) -> (Vec<u8>, Vec<u8>) {
    column_record_with_schema(id, table_id, name, None, None, true)
}

pub fn column_record_for_table_at_ordinal(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    ordinal: u32,
) -> (Vec<u8>, Vec<u8>) {
    column_record_with_schema_at_ordinal(id, table_id, name, ordinal, None, None, true)
}

pub fn column_record_for_table_with_nullability(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    nullable: bool,
) -> (Vec<u8>, Vec<u8>) {
    column_record_with_schema(id, table_id, name, None, None, nullable)
}

pub fn column_record_with_schema(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
    nullable: bool,
) -> (Vec<u8>, Vec<u8>) {
    column_record_with_schema_at_ordinal(
        id,
        table_id,
        name,
        0,
        default_expression_id,
        add_column_default_id,
        nullable,
    )
}

pub fn column_record_with_schema_at_ordinal(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    ordinal: u32,
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
    nullable: bool,
) -> (Vec<u8>, Vec<u8>) {
    column_record_with_type_at_ordinal(
        id,
        table_id,
        name,
        ordinal,
        builtin_id(108),
        default_expression_id,
        add_column_default_id,
        nullable,
    )
}

pub fn column_record_for_table_with_type(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    type_id: [u8; 16],
    nullable: bool,
) -> (Vec<u8>, Vec<u8>) {
    column_record_with_type_at_ordinal(id, table_id, name, 0, type_id, None, None, nullable)
}

#[allow(clippy::too_many_arguments)]
pub fn column_record_with_type_at_ordinal(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    ordinal: u32,
    type_id: [u8; 16],
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
    nullable: bool,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 6u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let mut fields = vec![
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
            payload: name.as_bytes().to_vec(),
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
            payload: vec![u8::from(nullable)],
        },
    ];
    if let Some(expression_id) = default_expression_id {
        fields.push(CatalogField {
            tag: 7,
            wire_type: WireType::Id,
            payload: expression_id.to_vec(),
        });
    }
    fields.extend([
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
    ]);
    if let Some(expression_id) = add_column_default_id {
        fields.push(CatalogField {
            tag: 10,
            wire_type: WireType::Id,
            payload: expression_id.to_vec(),
        });
    }
    let payload = encode_envelope(&family_schema(6).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn expression_record(
    id: [u8; 16],
    purpose: u16,
    dependencies: &[[u8; 16]],
    expression: &Expr,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 7u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let mut dependency_list = (dependencies.len() as u32).to_le_bytes().to_vec();
    dependency_list.extend(dependencies.iter().flatten());
    let ir = encode_expression(expression).unwrap();
    let fields = [
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
            payload: [0x66; 16].to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bytes,
            payload: vec![0; 8],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::Bytes,
            payload: ir,
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::Ids,
            payload: dependency_list,
        },
    ];
    let payload = encode_envelope(&family_schema(7).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn membership_record(
    member_id: [u8; 16],
    role_id: [u8; 16],
    grantor_id: [u8; 16],
    epoch: u64,
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
            payload: epoch.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(11).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn grant_record(
    grantee_id: [u8; 16],
    object_id: [u8; 16],
    privilege: u16,
    scope: u16,
    target_id: [u8; 16],
    grantor_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 12u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&grantee_id);
    key.push(1);
    key.extend_from_slice(&object_id);
    key.push(1);
    key.extend_from_slice(&privilege.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&scope.to_be_bytes());
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
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(12).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn policy_target_record(
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
    let payload = encode_envelope(&family_schema(14).unwrap(), &key, &[]).unwrap();
    (key, payload)
}

pub fn table_record(id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    table_record_for_schema(id, [0x61; 16])
}

pub fn table_record_for_schema(id: [u8; 16], schema_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let mut key = 5u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [
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
            payload: builtin_owner_id().to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(5).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn schema_record(id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    schema_record_with_search_path(id, &[])
}

pub fn schema_record_with_search_path(
    id: [u8; 16],
    search_path: &[[u8; 16]],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 3u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [CatalogField {
        tag: 1,
        wire_type: WireType::Ids,
        payload: id_list(search_path),
    }];
    let payload = encode_envelope(&family_schema(3).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn builtin_type_record(id: [u8; 16], builtin_kind: u16) -> (Vec<u8>, Vec<u8>) {
    let parameter_rule = match builtin_kind {
        5 => 1,
        7 => 2,
        _ => 0,
    };
    let default_mask = if builtin_kind == 9 { 7 } else { 1 };
    builtin_type_record_with_profile(id, builtin_kind, parameter_rule, 1, 1, default_mask, 1)
}

pub fn builtin_type_record_with_profile(
    id: [u8; 16],
    builtin_kind: u16,
    parameter_rule: u16,
    scalar_encoding: u32,
    comparison_version: u32,
    default_mask: u32,
    cast_profile: u32,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 4u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: builtin_kind.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U16,
            payload: parameter_rule.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U32,
            payload: scalar_encoding.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::U32,
            payload: comparison_version.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U32,
            payload: default_mask.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::U32,
            payload: cast_profile.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(4).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn put_table_and_column(
    database: &Database,
    table_id: [u8; 16],
    column_id: [u8; 16],
    nullable: bool,
) {
    put_table(database, table_id);
    let column_object = object_record(column_id, 4);
    put_catalog(database, &column_object.0, &column_object.1);
    let column = column_record_for_table_with_nullability(
        column_id,
        table_id,
        &format!("object_{}", column_id[0]),
        nullable,
    );
    put_catalog(database, &column.0, &column.1);
}

pub fn put_table(database: &Database, table_id: [u8; 16]) {
    let table_object = object_record(table_id, 3);
    put_catalog(database, &table_object.0, &table_object.1);
    let table = table_record(table_id);
    put_catalog(database, &table.0, &table.1);
}
