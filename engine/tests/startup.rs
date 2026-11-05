use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::authorization::Expr;
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::expression::encode_expression;
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{
    self, AUDIT, CATALOG, ENGINE_META, INDEXES, OUTCOMES, ROW_INDEXES, ROWS,
};
use prdb_engine::startup::{StartupError, validate_persisted_records};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::Database;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn builtin_owner_id() -> [u8; 16] {
    builtin_id(3)
}

fn builtin_id(code: u16) -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = code as u8;
    id
}

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-startup-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> std::path::PathBuf {
        self.0.join("startup.redb")
    }

    fn database(&self, commit_sequence: u64) -> Database {
        let database = Database::create(self.path()).unwrap();
        let write = database.begin_write().unwrap();
        schema::create_tables(&write).unwrap();
        let metadata = EngineMetadata {
            database_id: [0x11; 16],
            catalog_version: 1,
            expression_version: 1,
            builtin_semantics_version: 1,
            commit_sequence,
            lifecycle: Lifecycle::Ready,
        }
        .encode()
        .unwrap();
        let (database_key, database_value) = database_record([0x11; 16], 1, 1, 1);
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
            .open_table(CATALOG)
            .unwrap()
            .insert(&owner_object_key[..], &owner_object_value[..])
            .unwrap();
        for code in [1u16, 2] {
            let schema_id = builtin_id(code);
            let (schema_key, schema_value) = schema_record(schema_id);
            let (object_key, object_value) = object_record(schema_id, 1);
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

fn row_key(table_id: [u8; 16], row_id: [u8; 16]) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&table_id);
    key[16..].copy_from_slice(&row_id);
    key
}

fn builtin_mapping() -> Vec<u8> {
    let codes = [1u16, 2, 3]
        .into_iter()
        .chain(101u16..=111)
        .chain(201u16..=216);
    let mut mapping = 30u32.to_le_bytes().to_vec();
    for code in codes {
        mapping.extend_from_slice(&code.to_le_bytes());
        mapping.extend_from_slice(&0u16.to_le_bytes());
        mapping.extend_from_slice(&builtin_id(code));
    }
    mapping
}

fn database_record(
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

fn object_record(id: [u8; 16], kind: u16) -> (Vec<u8>, Vec<u8>) {
    object_record_with_parent(id, kind, None)
}

fn object_record_with_parent(
    id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
) -> (Vec<u8>, Vec<u8>) {
    object_record_with_parent_and_owner(id, kind, parent_id, builtin_owner_id())
}

fn object_record_with_owner(id: [u8; 16], kind: u16, owner_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    object_record_with_parent_and_owner(id, kind, None, owner_id)
}

fn object_record_with_parent_and_owner(
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

fn object_record_named(
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

fn dependency_record(
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

fn id_list(ids: &[[u8; 16]]) -> Vec<u8> {
    let mut encoded = (ids.len() as u32).to_le_bytes().to_vec();
    encoded.extend(ids.iter().flatten());
    encoded
}

fn constraint_record(
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

fn check_constraint_record(
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

fn policy_record(
    id: [u8; 16],
    table_id: [u8; 16],
    using_expression_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    policy_record_with_kind(id, table_id, using_expression_id, 1)
}

fn policy_record_with_kind(
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

fn reverse_index_key(row_key: &[u8; 32], index_key: &[u8]) -> Vec<u8> {
    let mut key = row_key.to_vec();
    key.extend_from_slice(&index_key[..index_key.len() - 16]);
    key
}

fn insert_row(database: &Database, key: &[u8; 32], version: u64) {
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

fn insert_row_payload(database: &Database, key: &[u8; 32], version: u64, payload: &[u8]) {
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

fn index_definition(index_id: [u8; 16], table_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    index_record_for_columns(index_id, table_id, &[[0x66; 16]])
}

fn index_record_for_columns(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
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

fn owned_index_record(
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

fn put_catalog(database: &Database, key: &[u8], value: &[u8]) {
    let write = database.begin_write().unwrap();
    write
        .open_table(CATALOG)
        .unwrap()
        .insert(key, value)
        .unwrap();
    write.commit().unwrap();
}

fn principal_record(
    id: [u8; 16],
    kind: u16,
    login_enabled: bool,
    epoch: u64,
) -> (Vec<u8>, Vec<u8>) {
    principal_record_with_name(id, kind, login_enabled, epoch, &format!("object_{}", id[0]))
}

fn principal_record_with_name(
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

fn credential_verifier_record(token_id: [u8; 16], user_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
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
    ];
    (
        key.clone(),
        encode_envelope(&family_schema(16).unwrap(), &key, &fields).unwrap(),
    )
}

fn column_record(id: [u8; 16], name: &str) -> (Vec<u8>, Vec<u8>) {
    column_record_for_table(id, [0x55; 16], name)
}

fn column_record_for_table(id: [u8; 16], table_id: [u8; 16], name: &str) -> (Vec<u8>, Vec<u8>) {
    column_record_with_schema(id, table_id, name, None, None, true)
}

fn column_record_for_table_at_ordinal(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    ordinal: u32,
) -> (Vec<u8>, Vec<u8>) {
    column_record_with_schema_at_ordinal(id, table_id, name, ordinal, None, None, true)
}

fn column_record_for_table_with_nullability(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    nullable: bool,
) -> (Vec<u8>, Vec<u8>) {
    column_record_with_schema(id, table_id, name, None, None, nullable)
}

fn column_record_with_schema(
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

fn column_record_with_schema_at_ordinal(
    id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    ordinal: u32,
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
            payload: builtin_id(108).to_vec(),
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

fn expression_record(
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

fn membership_record(
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

fn grant_record(
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

fn policy_target_record(
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

fn table_record(id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let mut key = 5u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: [0x61; 16].to_vec(),
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

fn schema_record(id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let mut key = 3u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    let fields = [CatalogField {
        tag: 1,
        wire_type: WireType::Ids,
        payload: id_list(&[]),
    }];
    let payload = encode_envelope(&family_schema(3).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

fn builtin_type_record(id: [u8; 16], builtin_kind: u16) -> (Vec<u8>, Vec<u8>) {
    let parameter_rule = match builtin_kind {
        5 => 1,
        7 => 2,
        _ => 0,
    };
    let default_mask = if builtin_kind == 9 { 7 } else { 1 };
    builtin_type_record_with_profile(id, builtin_kind, parameter_rule, 1, 1, default_mask, 1)
}

fn builtin_type_record_with_profile(
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

fn put_table_and_column(
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

fn put_table(database: &Database, table_id: [u8; 16]) {
    let table_object = object_record(table_id, 3);
    put_catalog(database, &table_object.0, &table_object.1);
    let table = table_record(table_id);
    put_catalog(database, &table.0, &table.1);
}

#[test]
fn validates_ready_image_records_and_references() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(3);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let key = row_key(table_id, row_id);
    insert_row(&database, &key, 2);
    put_table_and_column(&database, table_id, [0x66; 16], false);

    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&prdb_engine::index_key::encode_bytes(b"tuple-fixture"));
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 2,
    }
    .encode();
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 3,
        retention_deadline_us: 9_999,
    }
    .encode()?;
    let audit = AuditEvent {
        event_id: [0x88; 16],
        actor_id: Some([0x66; 16]),
        correlation_id: [0x99; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[0xaa; 16]],
    }
    .encode()?;
    let write = database.begin_write()?;
    write
        .open_table(CATALOG)?
        .insert(&catalog_key[..], &catalog_value[..])?;
    write
        .open_table(INDEXES)?
        .insert(&index_key[..], &locator[..])?;
    write
        .open_table(ROW_INDEXES)?
        .insert(&reverse_index_key(&key, &index_key)[..], &b""[..])?;
    write
        .open_table(OUTCOMES)?
        .insert(&[0x55; 16][..], &outcome[..])?;
    write
        .open_table(AUDIT)?
        .insert(&[0x88; 16][..], &audit[..])?;
    write.commit()?;

    let report = validate_persisted_records(&database)?;
    assert_eq!(report.metadata.commit_sequence, 3);
    assert_eq!(report.rows, 1);
    assert_eq!(report.index_entries, 1);
    assert_eq!(report.catalog_records, 35);
    assert_eq!(report.outcomes, 1);
    assert_eq!(report.audit_events, 1);
    Ok(())
}

#[test]
fn rejects_missing_membership_for_catalog_index() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    insert_row(&database, &row_key(table_id, row_id), 1);
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (index_key, index_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &index_key, &index_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    );
}

#[test]
fn rejects_index_key_that_does_not_match_its_row_tuple() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let row_key = row_key(table_id, row_id);
    insert_row(&database, &row_key, 1);
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &catalog_key, &catalog_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&prdb_engine::index_key::encode_bytes(b"different value"));
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write
        .open_table(ROW_INDEXES)
        .unwrap()
        .insert(&reverse_index_key(&row_key, &index_key)[..], &b""[..])
        .unwrap();
    write.commit().unwrap();

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    );
}

#[test]
fn rejects_ready_image_without_database_bootstrap_record() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, _) = database_record([0x11; 16], 1, 1, 1);
    let write = database.begin_write().unwrap();
    write.open_table(CATALOG).unwrap().remove(&key[..]).unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn rejects_bootstrap_versions_that_disagree_with_engine_metadata() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = database_record([0x11; 16], 2, 1, 1);
    put_catalog(&database, &key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn rejects_noncanonical_bootstrap_mappings() {
    for corrupt in ["missing", "unsorted", "reserved", "duplicate_id"] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        let (key, _) = database_record([0x11; 16], 1, 1, 1);
        let mut mapping = builtin_mapping();
        match corrupt {
            "missing" => {
                mapping[..4].copy_from_slice(&29u32.to_le_bytes());
                mapping.truncate(mapping.len() - 20);
            }
            "unsorted" => mapping[24..26].copy_from_slice(&1u16.to_le_bytes()),
            "reserved" => mapping[6..8].copy_from_slice(&1u16.to_le_bytes()),
            "duplicate_id" => {
                let first_id = mapping[8..24].to_vec();
                mapping[28..44].copy_from_slice(&first_id);
            }
            _ => unreachable!(),
        }
        let fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Text,
                payload: b"test_database".to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Bytes,
                payload: mapping,
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
        ];
        let payload = encode_envelope(&family_schema(1).unwrap(), &key, &fields).unwrap();
        put_catalog(&database, &key, &payload);
        assert!(
            matches!(
                validate_persisted_records(&database),
                Err(StartupError::InvalidCatalog)
            ),
            "corrupt mapping case: {corrupt}"
        );
    }
}

#[test]
fn rejects_credential_verifiers_without_a_supported_profile() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let user_id = [70; 16];
    let (user_key, user_value) = principal_record(user_id, 1, true, 1);
    let (object_key, object_value) = object_record(user_id, 8);
    let (verifier_key, verifier_value) = credential_verifier_record([71; 16], user_id);
    for (key, value) in [
        (user_key, user_value),
        (object_key, object_value),
        (verifier_key, verifier_value),
    ] {
        put_catalog(&database, &key, &value);
    }
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
}

#[test]
fn mapped_builtin_ids_resolve_to_their_expected_typed_records()
-> Result<(), Box<dyn std::error::Error>> {
    let valid_fixture = Fixture::new();
    let valid_database = valid_fixture.database(0);
    assert!(validate_persisted_records(&valid_database).is_ok());

    let missing_fixture = Fixture::new();
    let missing_database = missing_fixture.database(0);
    let type_key = 4u16
        .to_be_bytes()
        .into_iter()
        .chain([1])
        .chain(builtin_id(101))
        .collect::<Vec<_>>();
    let write = missing_database.begin_write()?;
    assert!(
        write
            .open_table(CATALOG)?
            .remove(type_key.as_slice())?
            .is_some()
    );
    write.commit()?;
    assert_eq!(
        validate_persisted_records(&missing_database),
        Err(StartupError::InvalidCatalog),
    );

    let mistyped_fixture = Fixture::new();
    let mistyped_database = mistyped_fixture.database(0);
    let (type_key, type_value) = builtin_type_record(builtin_id(101), 2);
    put_catalog(&mistyped_database, &type_key, &type_value);
    assert_eq!(
        validate_persisted_records(&mistyped_database),
        Err(StartupError::InvalidCatalog),
    );

    let misnamed_fixture = Fixture::new();
    let misnamed_database = misnamed_fixture.database(0);
    let admin_role_id = builtin_id(3);
    let (principal_key, principal_value) =
        principal_record_with_name(admin_role_id, 2, false, 1, "other_role");
    let (object_key, object_value) =
        object_record_named(admin_role_id, 9, admin_role_id, "other_role");
    put_catalog(&misnamed_database, &principal_key, &principal_value);
    put_catalog(&misnamed_database, &object_key, &object_value);
    assert_eq!(
        validate_persisted_records(&misnamed_database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

#[test]
fn rejects_objects_without_matching_typed_records() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (object_key, object_value) = object_record([0x51; 16], 1);
    put_catalog(&database, &object_key, &object_value);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

#[test]
fn rejects_builtin_type_records_with_unassigned_profiles() -> Result<(), Box<dyn std::error::Error>>
{
    let profiles = [
        (1, 1, 1, 1, 1),
        (0, 2, 1, 1, 1),
        (0, 1, 2, 1, 1),
        (0, 1, 1, 7, 1),
        (0, 1, 1, 1, 2),
    ];
    for (parameter_rule, scalar_encoding, comparison_version, default_mask, cast_profile) in
        profiles
    {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        let (key, value) = builtin_type_record_with_profile(
            builtin_id(101),
            1,
            parameter_rule,
            scalar_encoding,
            comparison_version,
            default_mask,
            cast_profile,
        );
        put_catalog(&database, &key, &value);
        assert_eq!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog),
        );
    }

    let uuid_fixture = Fixture::new();
    let uuid_database = uuid_fixture.database(0);
    let (key, value) = builtin_type_record_with_profile(builtin_id(109), 9, 0, 1, 1, 1, 1);
    put_catalog(&uuid_database, &key, &value);
    assert_eq!(
        validate_persisted_records(&uuid_database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

#[test]
fn rejects_dependency_edges_with_missing_catalog_objects() {
    for present_ids in [[true, false], [false, true]] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        if present_ids[0] {
            let (key, value) = object_record([0x21; 16], 3);
            put_catalog(&database, &key, &value);
        }
        if present_ids[1] {
            let (key, value) = object_record([0x22; 16], 4);
            put_catalog(&database, &key, &value);
        }
        let (key, value) = dependency_record([0x21; 16], [0x22; 16], 1);
        put_catalog(&database, &key, &value);
        assert!(matches!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog)
        ));
    }
}

#[test]
fn validates_foreign_key_target_edges_against_constraint_records() {
    let populate = |database: &Database,
                    edge_target: Option<[u8; 16]>,
                    add_alternate_target: bool,
                    fk_referenced_columns: &[[u8; 16]]| {
        let fk_id = [90; 16];
        let target_id = [91; 16];
        let alternate_id = [95; 16];
        put_table_and_column(database, [100; 16], [102; 16], false);
        put_table_and_column(database, [101; 16], [103; 16], false);
        let (extra_column_object_key, extra_column_object_value) = object_record([104; 16], 4);
        let (extra_column_key, extra_column_value) =
            column_record_for_table_at_ordinal([104; 16], [100; 16], "object_104", 1);
        put_catalog(
            database,
            &extra_column_object_key,
            &extra_column_object_value,
        );
        put_catalog(database, &extra_column_key, &extra_column_value);
        let (fk_key, fk_value) = object_record(fk_id, 6);
        let (target_key, target_value) = object_record(target_id, 6);
        put_catalog(database, &fk_key, &fk_value);
        put_catalog(database, &target_key, &target_value);
        if add_alternate_target {
            let (key, value) = object_record(alternate_id, 6);
            put_catalog(database, &key, &value);
        }
        let (fk_key, fk_value) = constraint_record(
            fk_id,
            [101; 16],
            5,
            &[[103; 16]],
            Some(target_id),
            fk_referenced_columns,
            None,
        );
        let (target_key, target_value) = constraint_record(
            target_id,
            [100; 16],
            2,
            &[[102; 16]],
            None,
            &[],
            Some([94; 16]),
        );
        put_catalog(database, &fk_key, &fk_value);
        put_catalog(database, &target_key, &target_value);
        let (target_object_key, target_object_value) = object_record([94; 16], 7);
        let (target_index_key, target_index_value) =
            owned_index_record([94; 16], [100; 16], [102; 16], target_id);
        put_catalog(database, &target_object_key, &target_object_value);
        put_catalog(database, &target_index_key, &target_index_value);
        let (key, value) = dependency_record(target_id, [94; 16], 4);
        put_catalog(database, &key, &value);
        if add_alternate_target {
            let (key, value) = constraint_record(
                alternate_id,
                [100; 16],
                3,
                &[[102; 16]],
                None,
                &[],
                Some([96; 16]),
            );
            put_catalog(database, &key, &value);
            let (object_key, object_value) = object_record([96; 16], 7);
            let (index_key, index_value) =
                owned_index_record([96; 16], [100; 16], [102; 16], alternate_id);
            put_catalog(database, &object_key, &object_value);
            put_catalog(database, &index_key, &index_value);
            let (key, value) = dependency_record(alternate_id, [96; 16], 4);
            put_catalog(database, &key, &value);
        }
        if let Some(edge_target) = edge_target {
            let (key, value) = dependency_record(fk_id, edge_target, 3);
            put_catalog(database, &key, &value);
        }
    };

    let missing_edge_fixture = Fixture::new();
    let missing_edge_database = missing_edge_fixture.database(0);
    populate(&missing_edge_database, None, false, &[[102; 16]]);
    assert_eq!(
        validate_persisted_records(&missing_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let mismatched_edge_fixture = Fixture::new();
    let mismatched_edge_database = mismatched_edge_fixture.database(0);
    populate(
        &mismatched_edge_database,
        Some([95; 16]),
        true,
        &[[102; 16]],
    );
    assert_eq!(
        validate_persisted_records(&mismatched_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let valid_edge_fixture = Fixture::new();
    let valid_edge_database = valid_edge_fixture.database(0);
    populate(&valid_edge_database, Some([91; 16]), false, &[[102; 16]]);
    assert!(validate_persisted_records(&valid_edge_database).is_ok());

    let mismatched_columns_fixture = Fixture::new();
    let mismatched_columns_database = mismatched_columns_fixture.database(0);
    populate(
        &mismatched_columns_database,
        Some([91; 16]),
        false,
        &[[104; 16]],
    );
    assert_eq!(
        validate_persisted_records(&mismatched_columns_database),
        Err(StartupError::InvalidCatalog),
    );
}

#[test]
fn validates_backing_index_edges_against_constraint_and_index_records() {
    let populate = |database: &Database,
                    index_edge: Option<[u8; 16]>,
                    add_alternate_index: bool| {
        let constraint_id = [110; 16];
        let index_id = [111; 16];
        let alternate_constraint_id = [112; 16];
        let alternate_index_id = [113; 16];
        put_table_and_column(database, [100; 16], [102; 16], false);
        let (constraint_object_key, constraint_object_value) = object_record(constraint_id, 6);
        let (index_object_key, index_object_value) = object_record(index_id, 7);
        put_catalog(database, &constraint_object_key, &constraint_object_value);
        put_catalog(database, &index_object_key, &index_object_value);
        let (constraint_key, constraint_value) = constraint_record(
            constraint_id,
            [100; 16],
            2,
            &[[102; 16]],
            None,
            &[],
            Some(index_id),
        );
        let (index_key, index_value) =
            owned_index_record(index_id, [100; 16], [102; 16], constraint_id);
        put_catalog(database, &constraint_key, &constraint_value);
        put_catalog(database, &index_key, &index_value);
        if add_alternate_index {
            let (object_key, object_value) = object_record(alternate_constraint_id, 6);
            put_catalog(database, &object_key, &object_value);
            let (object_key, object_value) = object_record(alternate_index_id, 7);
            put_catalog(database, &object_key, &object_value);
            let (constraint_key, constraint_value) = constraint_record(
                alternate_constraint_id,
                [100; 16],
                3,
                &[[102; 16]],
                None,
                &[],
                Some(alternate_index_id),
            );
            let (index_key, index_value) = owned_index_record(
                alternate_index_id,
                [100; 16],
                [102; 16],
                alternate_constraint_id,
            );
            put_catalog(database, &constraint_key, &constraint_value);
            put_catalog(database, &index_key, &index_value);
            let (key, value) = dependency_record(alternate_constraint_id, alternate_index_id, 4);
            put_catalog(database, &key, &value);
        }
        if let Some(index_id) = index_edge {
            let (key, value) = dependency_record(constraint_id, index_id, 4);
            put_catalog(database, &key, &value);
        }
    };

    let missing_edge_fixture = Fixture::new();
    let missing_edge_database = missing_edge_fixture.database(0);
    populate(&missing_edge_database, None, false);
    assert_eq!(
        validate_persisted_records(&missing_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let mismatched_edge_fixture = Fixture::new();
    let mismatched_edge_database = mismatched_edge_fixture.database(0);
    populate(&mismatched_edge_database, Some([113; 16]), true);
    assert_eq!(
        validate_persisted_records(&mismatched_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let valid_edge_fixture = Fixture::new();
    let valid_edge_database = valid_edge_fixture.database(0);
    populate(&valid_edge_database, Some([111; 16]), false);
    assert!(validate_persisted_records(&valid_edge_database).is_ok());
}

#[test]
fn validates_expression_reference_edges_against_typed_catalog_records() {
    let populate = |database: &Database, edges: &[([u8; 16], [u8; 16])]| {
        let column_id = [120; 16];
        let default_expression_id = [121; 16];
        let field_expression_id = [122; 16];
        let check_constraint_id = [123; 16];
        let check_expression_id = [124; 16];
        let policy_id = [125; 16];
        let policy_expression_id = [126; 16];
        let add_column_default_id = [127; 16];
        let (key, value) = object_record(column_id, 4);
        put_catalog(database, &key, &value);
        for expression_id in [
            default_expression_id,
            field_expression_id,
            check_expression_id,
            policy_expression_id,
            add_column_default_id,
        ] {
            let (key, value) = object_record(expression_id, 5);
            put_catalog(database, &key, &value);
        }
        let (key, value) = object_record(check_constraint_id, 6);
        put_catalog(database, &key, &value);
        let table = object_record([130; 16], 3);
        put_catalog(database, &table.0, &table.1);
        let table = table_record([130; 16]);
        put_catalog(database, &table.0, &table.1);
        let (key, value) = object_record(policy_id, 10);
        put_catalog(database, &key, &value);
        let (key, value) = column_record_with_schema(
            column_id,
            [130; 16],
            "object_120",
            Some(default_expression_id),
            Some(add_column_default_id),
            true,
        );
        put_catalog(database, &key, &value);
        let (key, value) =
            expression_record(default_expression_id, 1, &[], &Expr::CurrentPrincipalId);
        put_catalog(database, &key, &value);
        let (key, value) = expression_record(field_expression_id, 2, &[column_id], &Expr::Field(0));
        put_catalog(database, &key, &value);
        let (key, value) =
            expression_record(check_expression_id, 2, &[], &Expr::CurrentPrincipalId);
        put_catalog(database, &key, &value);
        let (key, value) =
            expression_record(policy_expression_id, 3, &[], &Expr::CurrentPrincipalId);
        put_catalog(database, &key, &value);
        let (key, value) = expression_record(add_column_default_id, 1, &[], &Expr::Literal(None));
        put_catalog(database, &key, &value);
        let (key, value) =
            check_constraint_record(check_constraint_id, [130; 16], check_expression_id);
        put_catalog(database, &key, &value);
        let (key, value) = policy_record(policy_id, [130; 16], policy_expression_id);
        put_catalog(database, &key, &value);
        for (dependent_id, referenced_id) in edges {
            let (key, value) = dependency_record(*dependent_id, *referenced_id, 2);
            put_catalog(database, &key, &value);
        }
    };

    let missing_edge_fixture = Fixture::new();
    let missing_edge_database = missing_edge_fixture.database(0);
    populate(&missing_edge_database, &[([122; 16], [120; 16])]);
    assert_eq!(
        validate_persisted_records(&missing_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let mismatched_edge_fixture = Fixture::new();
    let mismatched_edge_database = mismatched_edge_fixture.database(0);
    populate(
        &mismatched_edge_database,
        &[
            ([120; 16], [121; 16]),
            ([120; 16], [127; 16]),
            ([120; 16], [122; 16]),
            ([122; 16], [120; 16]),
            ([123; 16], [124; 16]),
            ([125; 16], [126; 16]),
        ],
    );
    assert_eq!(
        validate_persisted_records(&mismatched_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let valid_edge_fixture = Fixture::new();
    let valid_edge_database = valid_edge_fixture.database(0);
    populate(
        &valid_edge_database,
        &[
            ([120; 16], [121; 16]),
            ([120; 16], [127; 16]),
            ([122; 16], [120; 16]),
            ([123; 16], [124; 16]),
            ([125; 16], [126; 16]),
        ],
    );
    assert!(validate_persisted_records(&valid_edge_database).is_ok());
}

#[test]
fn rejects_objects_with_missing_owner_principals() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = object_record_with_owner([0x51; 16], 3, [0x52; 16]);
    put_catalog(&database, &key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn accepts_owned_child_dependency_with_catalog_object_endpoints()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        object_record([0x21; 16], 3),
        object_record_with_parent([0x22; 16], 4, Some([0x21; 16])),
        table_record([0x21; 16]),
        column_record_for_table([0x22; 16], [0x21; 16], "object_34"),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    let (key, value) = dependency_record([0x22; 16], [0x21; 16], 1);
    put_catalog(&database, &key, &value);
    let report = validate_persisted_records(&database)?;
    assert_eq!(report.catalog_records, 34);
    Ok(())
}

#[test]
fn rejects_owned_child_edges_that_disagree_with_object_parents() {
    for edge_parent in [None, Some([0x23; 16])] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        for (key, value) in [
            object_record([0x21; 16], 3),
            object_record([0x23; 16], 3),
            object_record_with_parent([0x22; 16], 4, Some([0x21; 16])),
        ] {
            put_catalog(&database, &key, &value);
        }
        if let Some(parent_id) = edge_parent {
            let (key, value) = dependency_record([0x22; 16], parent_id, 1);
            put_catalog(&database, &key, &value);
        }
        assert!(matches!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog)
        ));
    }
}

#[test]
fn rejects_owned_child_parent_cycles() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for (key, value) in [
        object_record_with_parent([0x31; 16], 3, Some([0x32; 16])),
        object_record_with_parent([0x32; 16], 4, Some([0x31; 16])),
    ] {
        put_catalog(&database, &key, &value);
    }
    for (dependent, parent) in [([0x31; 16], [0x32; 16]), ([0x32; 16], [0x31; 16])] {
        let (key, value) = dependency_record(dependent, parent, 1);
        put_catalog(&database, &key, &value);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn rejects_unknown_dependency_edge_kinds() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for (key, value) in [object_record([0x41; 16], 3), object_record([0x42; 16], 4)] {
        put_catalog(&database, &key, &value);
    }
    let (mut key, mut value) = dependency_record([0x41; 16], [0x42; 16], 5);
    key[37..39].copy_from_slice(&6u16.to_be_bytes());
    value[32 + 37..32 + 39].copy_from_slice(&6u16.to_be_bytes());
    put_catalog(&database, &key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn rejects_typed_catalog_records_without_a_matching_object_record() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = index_definition([0x44; 16], [0x22; 16]);
    put_catalog(&database, &key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn rejects_typed_catalog_records_with_a_different_object_kind() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let index = index_definition([0x44; 16], [0x22; 16]);
    let object = object_record([0x44; 16], 3);
    put_catalog(&database, &index.0, &index.1);
    put_catalog(&database, &object.0, &object.1);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn rejects_principal_records_with_missing_or_mismatched_objects() {
    for object_kind in [None, Some(9)] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        let principal = principal_record([0x31; 16], 1, true, 1);
        put_catalog(&database, &principal.0, &principal.1);
        if let Some(kind) = object_kind {
            let object = object_record([0x31; 16], kind);
            put_catalog(&database, &object.0, &object.1);
        }
        assert!(matches!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog)
        ));
    }
}

#[test]
fn rejects_column_and_principal_names_that_disagree_with_objects() {
    let principal_fixture = Fixture::new();
    let principal_database = principal_fixture.database(0);
    let owner_id = builtin_owner_id();
    let (principal_key, principal_value) =
        principal_record_with_name(owner_id, 2, false, 1, "renamed_role");
    put_catalog(&principal_database, &principal_key, &principal_value);
    assert_eq!(
        validate_persisted_records(&principal_database),
        Err(StartupError::InvalidCatalog),
    );

    let column_fixture = Fixture::new();
    let column_database = column_fixture.database(0);
    let column_id = [75; 16];
    let (column_key, column_value) = column_record(column_id, "different_column_name");
    let (object_key, object_value) = object_record(column_id, 4);
    put_catalog(&column_database, &column_key, &column_value);
    put_catalog(&column_database, &object_key, &object_value);
    assert_eq!(
        validate_persisted_records(&column_database),
        Err(StartupError::InvalidCatalog),
    );
}

#[test]
fn rejects_row_versions_beyond_the_commit_frontier() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    insert_row(&database, &row_key([0x22; 16], [0x33; 16]), 2);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::SequenceAhead)
    ));
}

#[test]
fn rejects_rows_with_invalid_or_mismatched_typed_payloads() {
    let malformed = b"not a TUP1 tuple".to_vec();
    let wrong_schema_version = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        }],
        2,
        &[Some(ScalarValue::Bytea(b"value".to_vec()))],
    )
    .unwrap();
    let wrong_column_count = encode_tuple(
        &[
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: false,
            },
        ],
        1,
        &[
            Some(ScalarValue::Bytea(b"first".to_vec())),
            Some(ScalarValue::Bytea(b"second".to_vec())),
        ],
    )
    .unwrap();
    let null_in_nonnullable_column = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[None],
    )
    .unwrap();

    for payload in [
        malformed,
        wrong_schema_version,
        wrong_column_count,
        null_in_nonnullable_column,
    ] {
        let fixture = Fixture::new();
        let database = fixture.database(1);
        let table_id = [0x22; 16];
        put_table_and_column(&database, table_id, [0x66; 16], false);
        insert_row_payload(&database, &row_key(table_id, [0x33; 16]), 1, &payload);
        assert_eq!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidRow)
        );
    }
}

#[test]
fn rejects_rows_without_a_catalog_table_schema() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    insert_row(&database, &row_key([0x22; 16], [0x33; 16]), 1);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidRow)
    );
}

#[test]
fn rejects_zero_object_ids_in_row_keys() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    insert_row(&database, &row_key([0; 16], [0x33; 16]), 1);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidRow)
    ));
}

#[test]
fn rejects_index_without_its_catalog_definition() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let row_id = [0x33; 16];
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

#[test]
fn rejects_index_without_its_referenced_row() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &catalog_key, &catalog_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write
        .open_table(ROW_INDEXES)
        .unwrap()
        .insert(
            &reverse_index_key(&row_key(table_id, row_id), &index_key)[..],
            &b""[..],
        )
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

#[test]
fn rejects_a_locator_whose_version_differs_from_its_row() {
    let fixture = Fixture::new();
    let database = fixture.database(2);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let key = row_key(table_id, row_id);
    insert_row(&database, &key, 2);
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &catalog_key, &catalog_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write
        .open_table(ROW_INDEXES)
        .unwrap()
        .insert(&reverse_index_key(&key, &index_key)[..], &b""[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

#[test]
fn rejects_catalog_record_stored_under_a_different_key() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = index_definition([0x44; 16], [0x22; 16]);
    let mut wrong_key = key;
    wrong_key[18] ^= 1;
    put_catalog(&database, &wrong_key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn rejects_a_committed_outcome_beyond_the_commit_frontier() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 2,
        retention_deadline_us: 9_999,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(OUTCOMES)
        .unwrap()
        .insert(&[0x55; 16][..], &outcome[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidOutcome)
    ));
}

#[test]
fn initializing_database_is_not_ready_for_normal_startup() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let metadata = EngineMetadata {
        database_id: [0x11; 16],
        catalog_version: 1,
        expression_version: 1,
        builtin_semantics_version: 1,
        commit_sequence: 0,
        lifecycle: Lifecycle::Initializing,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(ENGINE_META)
        .unwrap()
        .insert(&b"format"[..], &metadata[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::NotReady)
    ));
}

#[test]
fn rejects_outcome_key_value_mismatch() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 1,
        retention_deadline_us: 9_999,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(OUTCOMES)
        .unwrap()
        .insert(&[0x54; 16][..], &outcome[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidOutcome)
    ));
}

#[test]
fn accepts_pinned_acyclic_membership_graph() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 2, false, 1),
        principal_record([3; 16], 2, false, 1),
        membership_record([1; 16], [2; 16], [1; 16], 1),
        membership_record([2; 16], [3; 16], [1; 16], 1),
        dependency_record([1; 16], [2; 16], 5),
        dependency_record([2; 16], [3; 16], 5),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for (principal_id, kind) in [([1; 16], 8), ([2; 16], 9), ([3; 16], 9)] {
        let object = object_record(principal_id, kind);
        put_catalog(&database, &object.0, &object.1);
    }
    let report = validate_persisted_records(&database)?;
    assert_eq!(report.catalog_records, 39);
    Ok(())
}

#[test]
fn authority_dependencies_must_exactly_match_membership_records() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([11; 16], 1, true, 1),
        principal_record([12; 16], 2, false, 1),
        membership_record([11; 16], [12; 16], [11; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for (principal_id, kind) in [([11; 16], 8), ([12; 16], 9)] {
        let object = object_record(principal_id, kind);
        put_catalog(&database, &object.0, &object.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));

    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([11; 16], 1, true, 1),
        principal_record([12; 16], 2, false, 1),
        membership_record([11; 16], [12; 16], [11; 16], 1),
        dependency_record([11; 16], [12; 16], 5),
        dependency_record([12; 16], [11; 16], 5),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for (principal_id, kind) in [([11; 16], 8), ([12; 16], 9)] {
        let object = object_record(principal_id, kind);
        put_catalog(&database, &object.0, &object.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

#[test]
fn authority_dependencies_cover_grants_and_policy_recipients()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = |database: &Database, include_edges: bool| {
        let user_id = [13; 16];
        let table_id = [14; 16];
        let policy_id = [15; 16];
        let user = principal_record(user_id, 1, true, 1);
        put_catalog(database, &user.0, &user.1);
        for (object_id, kind) in [(user_id, 8), (table_id, 3), (policy_id, 10)] {
            let record = object_record(object_id, kind);
            put_catalog(database, &record.0, &record.1);
        }
        let table = table_record(table_id);
        put_catalog(database, &table.0, &table.1);
        let grant = grant_record(user_id, table_id, 2, 1, table_id, builtin_owner_id());
        put_catalog(database, &grant.0, &grant.1);
        let system_grant = grant_record(user_id, [0x11; 16], 12, 1, [0x11; 16], builtin_owner_id());
        put_catalog(database, &system_grant.0, &system_grant.1);
        let policy_key = {
            let mut key = 13u16.to_be_bytes().to_vec();
            key.push(1);
            key.extend_from_slice(&policy_id);
            key
        };
        let policy_fields = [
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
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ];
        let policy =
            encode_envelope(&family_schema(13).unwrap(), &policy_key, &policy_fields).unwrap();
        put_catalog(database, &policy_key, &policy);
        let target = policy_target_record(policy_id, 1, user_id);
        put_catalog(database, &target.0, &target.1);
        if include_edges {
            for (dependent, referenced) in [
                (user_id, table_id),
                (user_id, [0x11; 16]),
                (policy_id, user_id),
            ] {
                let edge = dependency_record(dependent, referenced, 5);
                put_catalog(database, &edge.0, &edge.1);
            }
        }
    };

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, false);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, true);
    validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn policy_column_targets_resolve_to_cell_policy_table_columns()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = |database: &Database, policy_kind: u16, column_table: Option<[u8; 16]>| {
        let table_id = [140; 16];
        let other_table_id = [141; 16];
        let column_id = [142; 16];
        let expression_id = [143; 16];
        let policy_id = [144; 16];

        for table_id_to_add in [table_id, other_table_id] {
            let object = object_record(table_id_to_add, 3);
            put_catalog(database, &object.0, &object.1);
            let table = table_record(table_id_to_add);
            put_catalog(database, &table.0, &table.1);
        }

        let policy_object = object_record(policy_id, 10);
        put_catalog(database, &policy_object.0, &policy_object.1);
        let policy = policy_record_with_kind(policy_id, table_id, expression_id, policy_kind);
        put_catalog(database, &policy.0, &policy.1);

        let expression_object = object_record(expression_id, 5);
        put_catalog(database, &expression_object.0, &expression_object.1);
        let expression = expression_record(expression_id, 3, &[], &Expr::CurrentPrincipalId);
        put_catalog(database, &expression.0, &expression.1);
        let expression_edge = dependency_record(policy_id, expression_id, 2);
        put_catalog(database, &expression_edge.0, &expression_edge.1);

        let column_object = object_record(column_id, 4);
        put_catalog(database, &column_object.0, &column_object.1);
        if let Some(column_table) = column_table {
            let column = column_record_for_table(column_id, column_table, "object_142");
            put_catalog(database, &column.0, &column.1);
        }

        let recipient = policy_target_record(policy_id, 2, builtin_owner_id());
        put_catalog(database, &recipient.0, &recipient.1);
        let column_target = policy_target_record(policy_id, 3, column_id);
        put_catalog(database, &column_target.0, &column_target.1);
        let authority_edge = dependency_record(policy_id, builtin_owner_id(), 5);
        put_catalog(database, &authority_edge.0, &authority_edge.1);
    };

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, 2, Some([140; 16]));
    assert!(validate_persisted_records(&database).is_ok());

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, 2, None);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, 2, Some([141; 16]));
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, 1, Some([140; 16]));
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

#[test]
fn constraints_resolve_to_source_columns_on_their_table() -> Result<(), Box<dyn std::error::Error>>
{
    let seed = |database: &Database,
                constraint_table: [u8; 16],
                column_table: Option<[u8; 16]>,
                column_nullable: bool| {
        let table_id = [150; 16];
        let other_table_id = [151; 16];
        let column_id = [152; 16];
        let constraint_id = [153; 16];

        for table_id_to_add in [table_id, other_table_id] {
            let object = object_record(table_id_to_add, 3);
            put_catalog(database, &object.0, &object.1);
            let table = table_record(table_id_to_add);
            put_catalog(database, &table.0, &table.1);
        }
        let column_object = object_record(column_id, 4);
        put_catalog(database, &column_object.0, &column_object.1);
        if let Some(column_table) = column_table {
            let column = column_record_for_table_with_nullability(
                column_id,
                column_table,
                "object_152",
                column_nullable,
            );
            put_catalog(database, &column.0, &column.1);
        }
        let constraint_object = object_record(constraint_id, 6);
        put_catalog(database, &constraint_object.0, &constraint_object.1);
        let constraint = constraint_record(
            constraint_id,
            constraint_table,
            1,
            &[[152; 16]],
            None,
            &[],
            None,
        );
        put_catalog(database, &constraint.0, &constraint.1);
    };

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [150; 16], Some([150; 16]), false);
    assert!(validate_persisted_records(&database).is_ok());

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [150; 16], None, false);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [150; 16], Some([151; 16]), false);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [154; 16], Some([150; 16]), false);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [150; 16], Some([150; 16]), true);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

#[test]
fn indexes_resolve_to_columns_on_their_table() -> Result<(), Box<dyn std::error::Error>> {
    let seed = |database: &Database, index_table: [u8; 16], column_table: Option<[u8; 16]>| {
        let table_id = [160; 16];
        let other_table_id = [161; 16];
        let index_id = [162; 16];
        let column_id = [163; 16];
        put_table(database, table_id);
        put_table(database, other_table_id);
        if let Some(column_table) = column_table {
            let column_object = object_record(column_id, 4);
            put_catalog(database, &column_object.0, &column_object.1);
            let column = column_record_for_table(column_id, column_table, "object_163");
            put_catalog(database, &column.0, &column.1);
        }
        let index_object = object_record(index_id, 7);
        put_catalog(database, &index_object.0, &index_object.1);
        let index = index_record_for_columns(index_id, index_table, &[column_id]);
        put_catalog(database, &index.0, &index.1);
    };

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [160; 16], Some([160; 16]));
    assert!(validate_persisted_records(&database).is_ok());

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [160; 16], None);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [160; 16], Some([161; 16]));
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [164; 16], Some([160; 16]));
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

#[test]
fn rejects_cycles_and_invalid_membership_endpoints_at_startup() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 2, false, 1),
        principal_record([3; 16], 2, false, 1),
        membership_record([2; 16], [3; 16], [1; 16], 1),
        membership_record([3; 16], [2; 16], [1; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for (principal_id, kind) in [([1; 16], 8), ([2; 16], 9), ([3; 16], 9)] {
        let object = object_record(principal_id, kind);
        put_catalog(&database, &object.0, &object.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));

    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 1, true, 1),
        membership_record([1; 16], [2; 16], [1; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for principal_id in [[1; 16], [2; 16]] {
        let object = object_record(principal_id, 8);
        put_catalog(&database, &object.0, &object.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}
