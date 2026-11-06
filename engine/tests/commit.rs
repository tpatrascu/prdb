use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::commit::{
    CatalogWrite, CommitError, CommitRequest, ForeignKeyCheck, IndexWrite, RowWrite,
    commit_catalog_and_audit, commit_catalog_and_audit_with_fences, commit_rows,
    commit_rows_and_indexes, commit_storage_changes,
};
use prdb_engine::credentials::hash_token_secret;
use prdb_engine::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ENGINE_META, ROWS};
use prdb_engine::storage_manager::{
    StorageManager, StorageManagerConfig, StorageManagerInitError, TokenIssueRequest,
};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-commit-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("commit.redb")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn key(n: u8) -> [u8; 32] {
    let mut key = [0x10; 32];
    key[31] = n;
    key
}

fn table_row_key(table_id: [u8; 16], row_id: [u8; 16]) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&table_id);
    key[16..].copy_from_slice(&row_id);
    key
}

fn id(n: u8) -> [u8; 16] {
    [n; 16]
}

fn id_catalog_key(family: u16, object_id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

fn identity_tombstone_write(object_id: [u8; 16]) -> CatalogWrite {
    let key = id_catalog_key(17, object_id);
    let payload = encode_envelope(&family_schema(17).unwrap(), &key, &[]).unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn catalog_delete(mut write: CatalogWrite, current_value: Vec<u8>) -> CatalogWrite {
    write.expected_value = Some(current_value);
    write.payload = None;
    write
}

fn catalog_expect(mut write: CatalogWrite, current_value: Vec<u8>) -> CatalogWrite {
    write.expected_value = Some(current_value);
    write
}

fn builtin_id(code: u16) -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = code as u8;
    id
}

fn builtin_owner_id() -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = 3;
    id
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

fn bytea_tuple(value: &[u8]) -> Vec<u8> {
    encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(value.to_vec()))],
    )
    .unwrap()
}

fn two_bytea_tuple(first: &[u8], second: &[u8]) -> Vec<u8> {
    encode_tuple(
        &[
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
        ],
        1,
        &[
            Some(ScalarValue::Bytea(first.to_vec())),
            Some(ScalarValue::Bytea(second.to_vec())),
        ],
    )
    .unwrap()
}

fn index_definition(index_id: [u8; 16], table_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    index_definition_with_columns(index_id, table_id, &[[0x66; 16]])
}

fn index_definition_with_columns(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
) -> (Vec<u8>, Vec<u8>) {
    index_definition_with_options(index_id, table_id, column_ids, false)
}

fn index_definition_with_options(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
    unique: bool,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 9u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&index_id);
    let mut columns = (column_ids.len() as u32).to_le_bytes().to_vec();
    columns.extend(column_ids.iter().flatten());
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
    let payload = encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

fn typed_bytea_index_key(
    index_id: [u8; 16],
    row_key: &[u8; 32],
    payload: &[u8],
) -> prdb_engine::index_key::DerivedIndexKey {
    prdb_engine::index_key::derive_index_key(
        index_id,
        row_key[16..].try_into().unwrap(),
        1,
        &[id(0x66)],
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        payload,
        &[id(0x66)],
    )
    .unwrap()
}

fn typed_two_bytea_index_key(
    index_id: [u8; 16],
    row_key: &[u8; 32],
    payload: &[u8],
    column_id: [u8; 16],
) -> prdb_engine::index_key::DerivedIndexKey {
    prdb_engine::index_key::derive_index_key(
        index_id,
        row_key[16..].try_into().unwrap(),
        1,
        &[id(0x66), id(0x67)],
        &[
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
        ],
        payload,
        &[column_id],
    )
    .unwrap()
}

fn owned_unique_index_definition(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
    constraint_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 9u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&index_id);
    let mut columns = (column_ids.len() as u32).to_le_bytes().to_vec();
    columns.extend(column_ids.iter().flatten());
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
    let payload = encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

fn bootstrap_database_record(database_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let mut key = 1u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&database_id);
    let mut builtins = 31u32.to_le_bytes().to_vec();
    for code in [1u16, 2, 3]
        .into_iter()
        .chain(101u16..=111)
        .chain(201u16..=217)
    {
        builtins.extend_from_slice(&code.to_le_bytes());
        builtins.extend_from_slice(&0u16.to_le_bytes());
        builtins.extend_from_slice(&builtin_id(code));
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
            payload: 2u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Bytes,
            payload: builtins,
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
    (key, payload)
}

fn initialize(path: &std::path::Path) -> Result<Database, Box<dyn std::error::Error>> {
    let database = Database::create(path)?;
    let mut transaction = database.begin_write()?;
    transaction.set_durability(redb::Durability::Immediate)?;
    transaction.set_two_phase_commit(true);
    schema::create_tables(&transaction)?;
    let (index_key, index_payload) = index_definition([0x44; 16], [0x10; 16]);
    let (database_key, database_payload) = bootstrap_database_record([0x33; 16]);
    let index_object = object_write([0x44; 16], 7);
    let table_object = object_write(id(16), 3);
    let table = table_write(id(16), id(20));
    let column_object = object_write(id(0x66), 4);
    let column = column_write(id(0x66), id(16), "object_102");
    transaction
        .open_table(CATALOG)?
        .insert(&database_key[..], &database_payload[..])?;
    transaction.open_table(CATALOG)?.insert(
        &index_object.key[..],
        index_object.payload.as_ref().unwrap().as_slice(),
    )?;
    transaction
        .open_table(CATALOG)?
        .insert(&index_key[..], &index_payload[..])?;
    for record in [table_object, table, column_object, column] {
        transaction
            .open_table(CATALOG)?
            .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
    }
    let owner_id = builtin_owner_id();
    let owner_principal = principal_write_with_name(owner_id, 2, 1, "database_admin");
    let owner_object = principal_object_write(owner_id, 2);
    transaction.open_table(CATALOG)?.insert(
        &owner_principal.key[..],
        owner_principal.payload.as_ref().unwrap().as_slice(),
    )?;
    transaction.open_table(CATALOG)?.insert(
        &owner_object.key[..],
        owner_object.payload.as_ref().unwrap().as_slice(),
    )?;
    for code in [1u16, 2] {
        let schema_id = builtin_id(code);
        let schema = schema_write(schema_id);
        let object = object_write(schema_id, 1);
        for record in [schema, object] {
            transaction
                .open_table(CATALOG)?
                .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
        }
    }
    for builtin_kind in 1u16..=11 {
        let type_id = builtin_id(100 + builtin_kind);
        let ty = builtin_type_write(type_id, builtin_kind);
        let object = object_write(type_id, 2);
        for record in [ty, object] {
            transaction
                .open_table(CATALOG)?
                .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
        }
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
    transaction
        .open_table(ENGINE_META)?
        .insert(&b"format"[..], &metadata[..])?;
    transaction.commit()?;
    Ok(database)
}

fn row(
    database: &Database,
    key: &[u8; 32],
) -> Result<Option<RowRecord>, Box<dyn std::error::Error>> {
    let transaction = database.begin_read()?;
    let value = transaction
        .open_table(ROWS)?
        .get(&key[..])?
        .map(|guard| guard.value().to_vec());
    value
        .map(|bytes| RowRecord::decode(&bytes).map_err(Into::into))
        .transpose()
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

fn index_key(row_key: &[u8; 32]) -> Vec<u8> {
    let mut key = vec![0x44; 16];
    key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    key.extend_from_slice(&row_key[16..]);
    key
}

fn audit_event(n: u8) -> AuditEvent {
    AuditEvent {
        event_id: [n; 16],
        actor_id: Some(id(9)),
        correlation_id: [n + 1; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[n + 2; 16]],
    }
}

fn identity_audit_event(n: u8, actor_id: [u8; 16], target_id: [u8; 16]) -> AuditEvent {
    AuditEvent {
        event_id: [n; 16],
        actor_id: Some(actor_id),
        correlation_id: [n.wrapping_add(1); 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![target_id],
    }
}

fn membership_write(member_id: [u8; 16], role_id: [u8; 16]) -> CatalogWrite {
    membership_write_with_grantor(member_id, role_id, builtin_owner_id())
}

fn membership_write_with_grantor(
    member_id: [u8; 16],
    role_id: [u8; 16],
    grantor_id: [u8; 16],
) -> CatalogWrite {
    let mut key = 11u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&member_id);
    key.push(1);
    key.extend_from_slice(&role_id);
    let payload = encode_envelope(
        &family_schema(11).unwrap(),
        &key,
        &[
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
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn grant_write(
    grantee_id: [u8; 16],
    object_id: [u8; 16],
    privilege: u16,
    target_scope: u16,
    target_id: [u8; 16],
    grantor_id: [u8; 16],
) -> CatalogWrite {
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
    let payload = encode_envelope(
        &family_schema(12).unwrap(),
        &key,
        &[
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
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn object_write(object_id: [u8; 16], kind: u16) -> CatalogWrite {
    object_write_with_parent(object_id, kind, None)
}

fn object_write_with_parent(
    object_id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
) -> CatalogWrite {
    object_write_with_parent_and_owner(object_id, kind, parent_id, builtin_owner_id())
}

fn object_write_with_owner(object_id: [u8; 16], kind: u16, owner_id: [u8; 16]) -> CatalogWrite {
    object_write_with_parent_and_owner(object_id, kind, None, owner_id)
}

fn object_write_with_parent_and_owner(
    object_id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
    owner_id: [u8; 16],
) -> CatalogWrite {
    let name = if object_id == builtin_owner_id() && kind == 9 {
        "database_admin".to_owned()
    } else if matches!(kind, 8 | 9) {
        principal_test_name(object_id)
    } else {
        format!("object_{}", object_id[0])
    };
    object_write_with_parent_and_owner_and_name(object_id, kind, parent_id, owner_id, &name)
}

fn object_write_with_name(object_id: [u8; 16], kind: u16, name: &str) -> CatalogWrite {
    object_write_with_parent_and_owner_and_name(object_id, kind, None, builtin_owner_id(), name)
}

fn object_write_with_parent_and_owner_and_name(
    object_id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
    owner_id: [u8; 16],
    name: &str,
) -> CatalogWrite {
    let mut key = 2u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
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
    ]);
    let payload = encode_envelope(&family_schema(2).unwrap(), &key, &fields).unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn principal_object_rename_write(
    object_id: [u8; 16],
    object_kind: u16,
    owner_id: [u8; 16],
    name: &str,
    definition_epoch: u64,
) -> CatalogWrite {
    let key = id_catalog_key(2, object_id);
    let payload = encode_envelope(
        &family_schema(2).unwrap(),
        &key,
        &[
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
                payload: owner_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U64,
                payload: definition_epoch.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn principal_object_write(principal_id: [u8; 16], principal_kind: u16) -> CatalogWrite {
    let object_kind = match principal_kind {
        1 => 8,
        2 => 9,
        _ => panic!("unsupported test principal kind"),
    };
    object_write(principal_id, object_kind)
}

fn schema_write(schema_id: [u8; 16]) -> CatalogWrite {
    let mut key = 3u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&schema_id);
    let payload = encode_envelope(
        &family_schema(3).unwrap(),
        &key,
        &[CatalogField {
            tag: 1,
            wire_type: WireType::Ids,
            payload: 0u32.to_le_bytes().to_vec(),
        }],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn builtin_type_write(type_id: [u8; 16], builtin_kind: u16) -> CatalogWrite {
    let parameter_rule = match builtin_kind {
        5 => 1,
        7 => 2,
        _ => 0,
    };
    let default_mask = if builtin_kind == 9 { 7 } else { 1 };
    builtin_type_write_with_profile(type_id, builtin_kind, parameter_rule, 1, 1, default_mask, 1)
}

fn builtin_type_write_with_profile(
    type_id: [u8; 16],
    builtin_kind: u16,
    parameter_rule: u16,
    scalar_encoding: u32,
    comparison_version: u32,
    default_mask: u32,
    cast_profile: u32,
) -> CatalogWrite {
    let key = id_catalog_key(4, type_id);
    let payload = encode_envelope(
        &family_schema(4).unwrap(),
        &key,
        &[
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
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn dependency_write(dependent_id: [u8; 16], referenced_id: [u8; 16], kind: u16) -> CatalogWrite {
    let mut key = 15u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&dependent_id);
    key.push(1);
    key.extend_from_slice(&referenced_id);
    key.push(1);
    key.extend_from_slice(&kind.to_be_bytes());
    let payload = encode_envelope(&family_schema(15).unwrap(), &key, &[]).unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn principal_write(principal_id: [u8; 16], kind: u16) -> CatalogWrite {
    principal_write_with_epoch(principal_id, kind, 1)
}

fn principal_write_with_name(
    principal_id: [u8; 16],
    kind: u16,
    epoch: u64,
    name: &str,
) -> CatalogWrite {
    principal_write_with_login(principal_id, kind, kind == 1, epoch, name)
}

fn principal_write_with_login(
    principal_id: [u8; 16],
    kind: u16,
    login_enabled: bool,
    epoch: u64,
    name: &str,
) -> CatalogWrite {
    let mut write = principal_write_with_epoch(principal_id, kind, epoch);
    let key = write.key.clone();
    write.payload = Some(
        encode_envelope(
            &family_schema(10).unwrap(),
            &key,
            &[
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
            ],
        )
        .unwrap(),
    );
    write
}

fn credential_verifier_write(
    token_id: [u8; 16],
    user_id: [u8; 16],
    secret: &[u8; 32],
    issued_at_us: i64,
    expires_at_us: i64,
    revoked_at_us: Option<i64>,
    authority_epoch: u64,
) -> CatalogWrite {
    let key = id_catalog_key(16, token_id);
    let mut fields = vec![
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
            payload: hash_token_secret(secret).to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::I64,
            payload: issued_at_us.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::I64,
            payload: expires_at_us.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Bool,
            payload: vec![u8::from(revoked_at_us.is_some())],
        },
    ];
    if let Some(revoked_at_us) = revoked_at_us {
        fields.push(CatalogField {
            tag: 8,
            wire_type: WireType::I64,
            payload: revoked_at_us.to_le_bytes().to_vec(),
        });
    }
    fields.extend([
        CatalogField {
            tag: 9,
            wire_type: WireType::Text,
            payload: b"test token".to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U64,
            payload: authority_epoch.to_le_bytes().to_vec(),
        },
    ]);
    CatalogWrite {
        key: key.clone(),
        expected_value: None,
        payload: Some(encode_envelope(&family_schema(16).unwrap(), &key, &fields).unwrap()),
    }
}

fn table_write(table_id: [u8; 16], schema_id: [u8; 16]) -> CatalogWrite {
    table_write_with_versions(table_id, schema_id, 1, 1)
}

fn table_write_with_versions(
    table_id: [u8; 16],
    schema_id: [u8; 16],
    schema_version: u64,
    write_epoch: u64,
) -> CatalogWrite {
    let mut key = 5u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&table_id);
    let payload = encode_envelope(
        &family_schema(5).unwrap(),
        &key,
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
                payload: write_epoch.to_le_bytes().to_vec(),
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
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn column_write(column_id: [u8; 16], table_id: [u8; 16], name: &str) -> CatalogWrite {
    column_write_with_defaults(column_id, table_id, name, None, None)
}

fn column_write_at_ordinal(
    column_id: [u8; 16],
    table_id: [u8; 16],
    ordinal: u32,
    name: &str,
) -> CatalogWrite {
    let key = id_catalog_key(6, column_id);
    let payload = encode_envelope(
        &family_schema(6).unwrap(),
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
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn column_write_not_null(column_id: [u8; 16], table_id: [u8; 16], name: &str) -> CatalogWrite {
    column_write_with_nullability_and_defaults(column_id, table_id, name, false, None, None)
}

fn column_write_with_defaults(
    column_id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
) -> CatalogWrite {
    column_write_with_nullability_and_defaults(
        column_id,
        table_id,
        name,
        true,
        default_expression_id,
        add_column_default_id,
    )
}

fn column_write_with_nullability_and_defaults(
    column_id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    nullable: bool,
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
) -> CatalogWrite {
    column_write_with_type_and_nullability(
        column_id,
        table_id,
        name,
        nullable,
        default_expression_id,
        add_column_default_id,
        builtin_id(108),
    )
}

fn column_write_with_type(
    column_id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    type_id: [u8; 16],
) -> CatalogWrite {
    column_write_with_type_and_nullability(column_id, table_id, name, true, None, None, type_id)
}

#[allow(clippy::too_many_arguments)]
fn column_write_with_type_and_nullability(
    column_id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    nullable: bool,
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
    type_id: [u8; 16],
) -> CatalogWrite {
    let mut key = 6u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&column_id);
    let mut fields = vec![
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
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn expression_write(
    expression_id: [u8; 16],
    purpose: u16,
    dependencies: &[[u8; 16]],
    expression: &prdb_engine::authorization::Expr,
) -> CatalogWrite {
    let key = id_catalog_key(7, expression_id);
    let mut dependency_list = (dependencies.len() as u32).to_le_bytes().to_vec();
    dependency_list.extend(dependencies.iter().flatten());
    let ir = prdb_engine::expression::encode_expression(expression).unwrap();
    let payload = encode_envelope(
        &family_schema(7).unwrap(),
        &key,
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
                payload: id(51).to_vec(),
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
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn constraint_write(
    constraint_id: [u8; 16],
    table_id: [u8; 16],
    kind: u16,
    source_columns: &[[u8; 16]],
    referenced_constraint_id: Option<[u8; 16]>,
    referenced_columns: &[[u8; 16]],
    backing_index_id: Option<[u8; 16]>,
) -> CatalogWrite {
    let key = id_catalog_key(8, constraint_id);
    let id_list = |ids: &[[u8; 16]]| {
        let mut encoded = (ids.len() as u32).to_le_bytes().to_vec();
        encoded.extend(ids.iter().flatten());
        encoded
    };
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
    CatalogWrite {
        payload: Some(encode_envelope(&family_schema(8).unwrap(), &key, &fields).unwrap()),
        key,
        expected_value: None,
    }
}

fn primary_index_write(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_id: [u8; 16],
    constraint_id: [u8; 16],
) -> CatalogWrite {
    let key = id_catalog_key(9, index_id);
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
    CatalogWrite {
        payload: Some(encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap()),
        key,
        expected_value: None,
    }
}

fn malformed_foreign_key_write() -> CatalogWrite {
    let constraint_id = id(81);
    let table_id = id(82);
    let mut key = 8u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&constraint_id);
    let payload = encode_envelope(
        &family_schema(8).unwrap(),
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
                payload: 5u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: {
                    let mut v = 1u32.to_le_bytes().to_vec();
                    v.extend_from_slice(&id(83));
                    v
                },
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
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
            CatalogField {
                tag: 11,
                wire_type: WireType::Id,
                payload: id(84).to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn check_constraint_without_table_write() -> CatalogWrite {
    let mut key = 8u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id(87));
    let payload = encode_envelope(
        &family_schema(8).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: id(88).to_vec(),
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
                payload: id(89).to_vec(),
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
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

#[test]
fn catalog_commit_rejects_malformed_constraint_semantics() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(86),
            id(9),
            9_999,
            &[malformed_foreign_key_write()],
            &[audit_event(86)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

#[test]
fn catalog_commit_rejects_nullable_primary_key_columns() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(16);
    let column_id = id(0x66);
    let index_id = id(0x44);
    let constraint_id = id(0x70);
    let (index_key, index_payload) =
        owned_unique_index_definition(index_id, table_id, &[column_id], constraint_id);
    let (old_index_key, _) = index_definition(index_id, table_id);
    assert_eq!(index_key, old_index_key);
    let (table_key, table_payload, old_index_payload) = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_key = id_catalog_key(5, table_id);
        let table_payload = catalog.get(table_key.as_slice())?.unwrap().value().to_vec();
        let index_payload = catalog.get(index_key.as_slice())?.unwrap().value().to_vec();
        (table_key, table_payload, index_payload)
    };
    let mut table_update = table_write_with_versions(table_id, id(20), 1, 2);
    table_update.key = table_key;
    table_update.expected_value = Some(table_payload);
    let index_update = CatalogWrite {
        key: index_key,
        expected_value: Some(old_index_payload),
        payload: Some(index_payload),
    };
    let constraint = constraint_write(
        constraint_id,
        table_id,
        2,
        &[column_id],
        None,
        &[],
        Some(index_id),
    );
    let object = object_write(constraint_id, 6);
    let dependency = dependency_write(constraint_id, index_id, 4);

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(91),
            id(9),
            9_999,
            &[table_update, index_update, constraint, object, dependency],
            &[audit_event(91)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

#[test]
fn catalog_commit_rejects_constraints_for_missing_tables() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(90),
            id(9),
            9_999,
            &[check_constraint_without_table_write()],
            &[audit_event(90)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

#[test]
fn catalog_commit_rejects_dangling_dependency_edges_and_records_abort()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let edge = dependency_write(id(101), id(102), 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(50),
            id(9),
            9_999,
            std::slice::from_ref(&edge),
            &[audit_event(50)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        lookup_outcome(&database, id(50), id(9), 9_998)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(read.open_table(CATALOG)?.get(&edge.key[..])?.is_none());
    assert!(
        read.open_table(prdb_engine::schema::AUDIT)?
            .get(&id(50)[..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn storage_manager_authorizes_constraint_ddl_by_table_alter()
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

#[test]
fn catalog_commit_checks_dependency_endpoints_in_the_final_catalog()
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

#[test]
fn catalog_commit_requires_owned_child_parent_edge_consistency()
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

fn foreign_key_catalog_writes(
    with_target_edge: bool,
    with_backing_edge: bool,
) -> Vec<CatalogWrite> {
    let schema_id = id(80);
    let target_table_id = id(81);
    let source_table_id = id(82);
    let target_column_id = id(83);
    let source_column_id = id(84);
    let target_constraint_id = id(85);
    let foreign_key_id = id(86);
    let backing_index_id = id(87);
    let mut writes = vec![
        schema_write(schema_id),
        table_write(target_table_id, schema_id),
        table_write(source_table_id, schema_id),
        column_write_not_null(target_column_id, target_table_id, "object_83"),
        column_write(source_column_id, source_table_id, "object_84"),
        constraint_write(
            target_constraint_id,
            target_table_id,
            2,
            &[target_column_id],
            None,
            &[],
            Some(backing_index_id),
        ),
        constraint_write(
            foreign_key_id,
            source_table_id,
            5,
            &[source_column_id],
            Some(target_constraint_id),
            &[target_column_id],
            None,
        ),
        primary_index_write(
            backing_index_id,
            target_table_id,
            target_column_id,
            target_constraint_id,
        ),
        object_write(schema_id, 1),
        object_write(target_table_id, 3),
        object_write(source_table_id, 3),
        object_write(target_column_id, 4),
        object_write(source_column_id, 4),
        object_write(target_constraint_id, 6),
        object_write(foreign_key_id, 6),
        object_write(backing_index_id, 7),
    ];
    if with_target_edge {
        writes.push(dependency_write(foreign_key_id, target_constraint_id, 3));
    }
    if with_backing_edge {
        writes.push(dependency_write(target_constraint_id, backing_index_id, 4));
    }
    writes
}

#[test]
fn catalog_commit_requires_foreign_key_target_dependency_edges()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(88),
            id(9),
            9_999,
            &foreign_key_catalog_writes(false, false),
            &[audit_event(88)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    let mut mismatched_types = foreign_key_catalog_writes(true, true);
    let mismatched_source_column =
        column_write_with_type(id(84), id(82), "object_84", builtin_id(109));
    let source_column_key = mismatched_source_column.key.clone();
    let source_column_write = mismatched_types
        .iter_mut()
        .find(|write| write.key == source_column_key)
        .unwrap();
    *source_column_write = mismatched_source_column;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(114),
            id(9),
            9_999,
            &mismatched_types,
            &[audit_event(114)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(89),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, true),
            &[audit_event(89)],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn adding_foreign_key_rejects_preexisting_orphan_rows() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_key = table_row_key(id(82), [1; 16]);
    commit_rows(
        &database,
        id(115),
        id(9),
        9_999,
        &[RowWrite {
            key: source_key,
            expected_version: None,
            payload: Some(bytea_tuple(b"missing-parent")),
        }],
    )?;

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(116),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, true),
            &[audit_event(116)],
        ),
        Err(CommitError::ForeignKeyViolation)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert!(row(&database, &source_key)?.is_some());
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::CATALOG)?
            .get(&id_catalog_key(8, id(86))[..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn adding_foreign_key_accepts_preexisting_matching_rows() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let ddl_fences = DdlFenceManager::new();
    let ddl_coordinator = DdlFenceCoordinator::new(ddl_fences.clone(), Duration::from_secs(1));
    let mut initial_catalog = foreign_key_catalog_writes(false, true);
    initial_catalog.retain(|write| {
        write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
    });
    assert_eq!(
        commit_catalog_and_audit_with_fences(
            &database,
            &ddl_coordinator,
            id(117),
            id(9),
            9_999,
            &initial_catalog,
            &[audit_event(117)],
        )?,
        1
    );

    let source_key = table_row_key(id(82), [1; 16]);
    let target_key = table_row_key(id(81), [2; 16]);
    let source_payload = bytea_tuple(b"existing-parent");
    let target_payload = bytea_tuple(b"existing-parent");
    let target_schema = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: false,
    }];
    let target_index = prdb_engine::index_key::derive_index_key(
        id(87),
        target_key[16..].try_into().unwrap(),
        1,
        &[id(83)],
        &target_schema,
        &target_payload,
        &[id(83)],
    )?;
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(118),
            id(9),
            9_999,
            &[
                RowWrite {
                    key: target_key,
                    expected_version: None,
                    payload: Some(target_payload),
                },
                RowWrite {
                    key: source_key,
                    expected_version: None,
                    payload: Some(source_payload),
                },
            ],
            &[IndexWrite {
                key: target_index.key,
                row_key: target_key,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: target_index.has_null,
            }],
        )?,
        2
    );

    let source_table_key = id_catalog_key(5, id(82));
    let previous_source_table = initial_catalog
        .iter()
        .find(|write| write.key == source_table_key)
        .unwrap()
        .payload
        .clone();
    let mut source_table = table_write_with_versions(id(82), id(80), 1, 2);
    source_table.expected_value = previous_source_table;
    let new_foreign_key_without_target_fence = [
        constraint_write(id(86), id(82), 5, &[id(84)], Some(id(85)), &[id(83)], None),
        object_write(id(86), 6),
        dependency_write(id(86), id(85), 3),
        source_table.clone(),
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(119),
            id(9),
            9_999,
            &new_foreign_key_without_target_fence,
            &[audit_event(119)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 2);

    let target_table_key = id_catalog_key(5, id(81));
    let previous_target_table = initial_catalog
        .iter()
        .find(|write| write.key == target_table_key)
        .unwrap()
        .payload
        .clone();
    let mut target_table = table_write_with_versions(id(81), id(80), 1, 2);
    target_table.expected_value = previous_target_table;
    let new_foreign_key = [
        new_foreign_key_without_target_fence[0].clone(),
        new_foreign_key_without_target_fence[1].clone(),
        new_foreign_key_without_target_fence[2].clone(),
        source_table,
        target_table,
    ];
    let target_writer = ddl_fences
        .acquire_shared(&[id(81)], Duration::ZERO)
        .unwrap();
    let no_wait_coordinator = DdlFenceCoordinator::new(ddl_fences.clone(), Duration::ZERO);
    assert_eq!(
        commit_catalog_and_audit_with_fences(
            &database,
            &no_wait_coordinator,
            id(120),
            id(9),
            9_999,
            &new_foreign_key,
            &[audit_event(120)],
        ),
        Err(CommitError::DdlFenceUnavailable)
    );
    drop(target_writer);
    assert_eq!(
        commit_catalog_and_audit_with_fences(
            &database,
            &ddl_coordinator,
            id(121),
            id(9),
            9_999,
            &new_foreign_key,
            &[audit_event(121)],
        )?,
        3
    );
    assert_eq!(commit_sequence(&database)?, 3);
    Ok(())
}

#[test]
fn storage_manager_routes_invocations_and_catalog_commits_through_shared_fences()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(
        storage.commit_catalog_and_audit(id(190), id(9), 9_999, &[], &[audit_event(190)],)?,
        1
    );

    let mut invocation = storage.begin_invocation(id(191), id(9), 9_999, 2, 64)?;
    let row_id = prdb_engine::overlay::RowId {
        table: id(10),
        row: id(11),
    };
    invocation.stage_put(row_id, b"coordinated")?;
    assert_eq!(invocation.commit()?, 2);
    assert_eq!(
        row(&database, &table_row_key(id(10), id(11)))?
            .unwrap()
            .payload,
        b"coordinated"
    );
    Ok(())
}

#[test]
fn storage_manager_admission_requires_a_login_enabled_catalog_user()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::AuthorizationError;
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_invocation_age: Duration::from_secs(30),
            max_token_lifecycle_requests_per_minute: 30,
        },
    )?;
    let enabled_user = id(229);
    let disabled_user = id(230);
    let writes = [
        object_write_with_name(enabled_user, 8, "admitted_user"),
        principal_write_with_name(enabled_user, 1, 1, "admitted_user"),
        object_write_with_name(disabled_user, 8, "disabled_user"),
        principal_write_with_login(disabled_user, 1, false, 1, "disabled_user"),
    ];
    storage.commit_catalog_and_audit(id(231), id(9), 9_999, &writes, &[audit_event(231)])?;

    let (mut admitted, roles) =
        storage.begin_authenticated_invocation(id(232), enabled_user, 9_999, 2, 64)?;
    assert!(roles.is_empty());
    admitted.abort()?;

    for (invocation_id, principal_id) in [(id(233), disabled_user), (id(234), id(235))] {
        assert!(matches!(
            storage.begin_authenticated_invocation(invocation_id, principal_id, 9_999, 2, 64,),
            Err(TransactionError::Authorization(AuthorizationError::Denied))
        ));
        assert_eq!(
            lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted
        );
    }

    let (mut admitted_again, _) =
        storage.begin_authenticated_invocation(id(236), enabled_user, 9_999, 2, 64)?;
    admitted_again.abort()?;
    Ok(())
}

#[test]
fn storage_manager_bounds_active_invocation_snapshots_across_clones()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 0,
                max_invocation_age: Duration::from_secs(30),
                max_token_lifecycle_requests_per_minute: 30,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroActiveInvocationLimit,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 1,
                max_invocation_age: Duration::ZERO,
                max_token_lifecycle_requests_per_minute: 30,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroInvocationAge,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 1,
                max_invocation_age: Duration::from_secs(30),
                max_token_lifecycle_requests_per_minute: 0,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroTokenLifecycleRequestLimit,
        ))
    );
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_invocation_age: Duration::from_secs(30),
            max_token_lifecycle_requests_per_minute: 30,
        },
    )?;
    let independent_handle = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(independent_handle.max_active_invocations(), 1);
    let mut first = storage.begin_invocation(id(192), id(9), 9_999, 2, 64)?;
    let cloned_storage = storage.clone();
    assert!(matches!(
        cloned_storage.begin_invocation(id(193), id(9), 9_999, 2, 64),
        Err(prdb_engine::transaction::TransactionError::AdmissionLimitExceeded)
    ));
    assert!(matches!(
        independent_handle.begin_invocation(id(195), id(9), 9_999, 2, 64),
        Err(prdb_engine::transaction::TransactionError::AdmissionLimitExceeded)
    ));

    first.abort()?;
    let next = cloned_storage.begin_invocation(id(194), id(9), 9_999, 2, 64)?;
    drop(next);
    drop(first);
    Ok(())
}

#[test]
fn storage_manager_expires_idle_invocations_and_releases_the_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_invocation_age: Duration::from_millis(1),
            max_token_lifecycle_requests_per_minute: 30,
        },
    )?;
    let mut expired = storage.begin_invocation(id(196), id(9), 9_999, 2, 64)?;
    std::thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        expired.check_deadline(),
        Err(prdb_engine::transaction::TransactionError::Expired)
    ));
    assert_eq!(
        lookup_outcome(&database, id(196), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let next = storage.begin_invocation(id(197), id(9), 9_999, 2, 64)?;
    drop(next);
    drop(expired);

    let expired_commit = storage.begin_invocation(id(198), id(9), 9_999, 2, 64)?;
    std::thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        expired_commit.commit(),
        Err(prdb_engine::transaction::TransactionError::Expired)
    ));
    assert_eq!(
        lookup_outcome(&database, id(198), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn storage_manager_resolves_only_retained_outcomes_for_the_authenticated_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let mut aborted = storage.begin_invocation(id(199), id(9), 9_999, 2, 64)?;
    aborted.abort()?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(199), id(9), 9_998)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let mut committed = storage.begin_invocation(id(200), id(9), 9_999, 2, 64)?;
    committed.stage_put(
        prdb_engine::overlay::RowId {
            table: id(16),
            row: id(201),
        },
        b"status lookup",
    )?;
    committed.commit()?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(200), id(9), 9_998)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        storage.lookup_invocation_outcome(id(200), id(10), 9_998)?,
        None
    );
    assert_eq!(
        storage.lookup_invocation_outcome(id(200), id(9), 9_999)?,
        None
    );
    assert_eq!(
        storage.lookup_invocation_outcome(id(202), id(9), 9_998)?,
        None
    );
    Ok(())
}

#[test]
fn storage_manager_reports_live_and_durable_invocation_status_to_the_owner()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::InvocationStatus;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let status_storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;

    let mut active = storage.begin_invocation(id(203), id(9), retention_deadline_us, 2, 64)?;
    assert!(matches!(
        storage.begin_invocation(id(203), id(9), retention_deadline_us, 2, 64),
        Err(prdb_engine::transaction::TransactionError::DuplicateInvocation)
    ));
    let live = status_storage
        .get_invocation_status(id(203), id(9), now_us)?
        .unwrap();
    assert!(matches!(live.status, InvocationStatus::Active { .. }));
    assert_eq!(live.observed_at_us, now_us);
    assert_eq!(
        status_storage.get_invocation_status(id(203), id(10), now_us)?,
        None
    );
    active.abort()?;
    drop(active);
    assert!(matches!(
        storage
            .get_invocation_status(id(203), id(9), now_us)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(InvocationOutcome {
            status: OutcomeStatus::Aborted,
            ..
        })
    ));

    let committed = storage.begin_invocation(id(204), id(9), retention_deadline_us, 2, 64)?;
    let writer = database.begin_write()?;
    let commit_wait_deadline = Instant::now() + Duration::from_secs(2);
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let commit_thread = scope.spawn(move || committed.commit());
        loop {
            let status = status_storage
                .get_invocation_status(id(204), id(9), now_us)?
                .unwrap();
            if matches!(status.status, InvocationStatus::Committing { .. }) {
                break;
            }
            assert!(Instant::now() < commit_wait_deadline);
            std::thread::yield_now();
        }
        writer.abort()?;
        commit_thread
            .join()
            .map_err(|_| std::io::Error::other("commit thread panicked"))??;
        Ok(())
    })?;
    assert!(matches!(
        storage
            .get_invocation_status(id(204), id(9), now_us)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(InvocationOutcome {
            status: OutcomeStatus::Committed,
            ..
        })
    ));
    drop(storage);
    drop(status_storage);
    drop(database);

    let reopened = Database::open(fixture.path())?;
    let restarted_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    assert!(matches!(
        restarted_storage
            .get_invocation_status(id(204), id(9), now_us)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(InvocationOutcome {
            status: OutcomeStatus::Committed,
            ..
        })
    ));
    assert_eq!(
        restarted_storage.get_invocation_status(id(204), id(10), now_us)?,
        None
    );
    Ok(())
}

#[test]
fn storage_manager_lists_only_owner_live_invocations_with_generation_bound_pages()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        InvocationStatus, InvocationStatusConsistency, InvocationStatusError,
        MAX_OWN_INVOCATION_PAGE,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;
    let mut first = storage.begin_invocation(id(220), id(9), retention_deadline_us, 2, 64)?;
    let mut hidden = storage.begin_invocation(id(221), id(10), retention_deadline_us, 2, 64)?;
    let mut second = storage.begin_invocation(id(222), id(9), retention_deadline_us, 2, 64)?;

    let page = storage.list_own_invocations(id(9), now_us, 1, None)?;
    assert_eq!(page.invocations.len(), 1);
    assert_eq!(page.invocations[0].invocation_id, id(220));
    assert!(matches!(
        page.invocations[0].status,
        InvocationStatus::Active { .. }
    ));
    assert_eq!(page.observed_at_us, now_us);
    assert_eq!(page.consistency, InvocationStatusConsistency::BestEffort);
    let cursor = page.continuation.unwrap();
    let mut unrelated = storage.begin_invocation(id(224), id(10), retention_deadline_us, 2, 64)?;
    assert!(matches!(
        storage.list_own_invocations(id(10), now_us, 1, Some(&cursor)),
        Err(InvocationStatusError::InvalidCursor)
    ));
    let next = storage.list_own_invocations(id(9), now_us, 1, Some(&cursor))?;
    assert_eq!(next.invocations.len(), 1);
    assert_eq!(next.invocations[0].invocation_id, id(222));
    assert_eq!(next.continuation, None);
    assert!(matches!(
        storage.list_own_invocations(id(9), now_us, 0, None),
        Err(InvocationStatusError::InvalidPageLimit)
    ));
    assert!(
        storage
            .list_own_invocations(id(9), now_us, MAX_OWN_INVOCATION_PAGE + 1, None)
            .is_err()
    );

    let stale = storage
        .list_own_invocations(id(9), now_us, 1, None)?
        .continuation
        .unwrap();
    let mut added = storage.begin_invocation(id(223), id(9), retention_deadline_us, 2, 64)?;
    assert!(matches!(
        storage.list_own_invocations(id(9), now_us, 1, Some(&stale)),
        Err(InvocationStatusError::StaleCursor)
    ));

    added.abort()?;
    unrelated.abort()?;
    second.abort()?;
    hidden.abort()?;
    first.abort()?;
    Ok(())
}

#[test]
fn storage_manager_rejects_invalid_recovered_images_before_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&key(195)[..], &b"not a row record"[..])?;
    write.commit()?;

    assert!(matches!(
        StorageManager::new(&database, Duration::from_millis(100)),
        Err(StorageManagerInitError::Startup(
            prdb_engine::startup::StartupError::InvalidRow
        ))
    ));
    Ok(())
}

#[test]
fn catalog_commit_requires_backing_index_dependency_edges() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(90),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, false),
            &[audit_event(90)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(91),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, true),
            &[audit_event(91)],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn catalog_commit_rejects_not_null_when_an_existing_row_is_null()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let schema_id = id(80);
    let table_id = id(82);
    let column_id = id(84);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(0xa1),
            id(9),
            9_999,
            &foreign_key_catalog_writes(true, true),
            &[audit_event(0xa1)],
        )?,
        1
    );
    let parent_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"present".to_vec()))],
    )?;
    let parent_key = table_row_key(id(81), id(0xa9));
    let parent_index_key = prdb_engine::index_key::derive_index_key(
        id(87),
        id(0xa9),
        1,
        &[id(83)],
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        }],
        &parent_tuple,
        &[id(83)],
    )?
    .key;
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(0xa2),
            id(9),
            9_999,
            &[RowWrite {
                key: parent_key,
                expected_version: None,
                payload: Some(parent_tuple),
            }],
            &[IndexWrite {
                key: parent_index_key,
                row_key: parent_key,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: false,
            }],
        )?,
        2
    );
    let null_tuple = encode_tuple(
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
            id(0xa8),
            id(9),
            9_999,
            &[RowWrite {
                key: table_row_key(table_id, id(0xa3)),
                expected_version: None,
                payload: Some(null_tuple),
            }],
        )?,
        3
    );

    let row_key = table_row_key(table_id, id(0xa3));
    let mut column = column_write_not_null(column_id, table_id, "object_84");
    column.expected_value = column_write(column_id, table_id, "object_84").payload;
    let mut table = table_write_with_versions(table_id, schema_id, 1, 2);
    table.expected_value = table_write(table_id, schema_id).payload;
    let constraint_id = id(0xa4);
    let changes = [
        constraint_write(constraint_id, table_id, 1, &[column_id], None, &[], None),
        object_write(constraint_id, 6),
        column,
        table,
    ];
    assert_eq!(
        storage.commit_catalog_and_audit(id(0xa5), id(9), 9_999, &changes, &[audit_event(0xa5)],),
        Err(CommitError::NotNullViolation)
    );
    assert_eq!(commit_sequence(&database)?, 3);
    assert_eq!(
        lookup_outcome(&database, id(0xa5), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(0xa0),
            id(9),
            9_999,
            &changes,
            &[audit_event(0xa0)],
        ),
        Err(CommitError::NotNullViolation)
    );
    assert_eq!(commit_sequence(&database)?, 3);

    let non_null_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"present".to_vec()))],
    )?;
    assert_eq!(
        commit_rows(
            &database,
            id(0xa6),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(3),
                payload: Some(non_null_tuple),
            }],
        )?,
        4
    );
    assert_eq!(
        storage.commit_catalog_and_audit(id(0xa7), id(9), 9_999, &changes, &[audit_event(0xa7)],)?,
        5
    );
    assert_eq!(commit_sequence(&database)?, 5);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn catalog_commit_requires_expression_reference_dependency_edges()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::Expr;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let column_id = id(120);
    let default_expression_id = id(121);
    let field_expression_id = id(122);
    let add_column_default_id = id(128);
    let policy_id = id(126);
    let policy_expression_id = id(127);
    let policy_key = id_catalog_key(13, policy_id);
    let policy_payload = encode_envelope(
        &family_schema(13).unwrap(),
        &policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: id(123).to_vec(),
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
    let base_writes = vec![
        column_write_with_defaults(
            column_id,
            id(123),
            "object_120",
            Some(default_expression_id),
            Some(add_column_default_id),
        ),
        expression_write(default_expression_id, 1, &[], &Expr::CurrentPrincipalId),
        expression_write(field_expression_id, 2, &[column_id], &Expr::Field(0)),
        expression_write(add_column_default_id, 1, &[], &Expr::Literal(None)),
        expression_write(policy_expression_id, 3, &[], &Expr::CurrentPrincipalId),
        CatalogWrite {
            key: policy_key,
            expected_value: None,
            payload: Some(policy_payload),
        },
        object_write(column_id, 4),
        object_write(default_expression_id, 5),
        object_write(field_expression_id, 5),
        object_write(add_column_default_id, 5),
        object_write(policy_id, 10),
        object_write(policy_expression_id, 5),
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(124),
            id(9),
            9_999,
            &base_writes,
            &[audit_event(124)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let mut complete_writes = base_writes;
    complete_writes.push(dependency_write(column_id, default_expression_id, 2));
    complete_writes.push(dependency_write(column_id, add_column_default_id, 2));
    complete_writes.push(dependency_write(field_expression_id, column_id, 2));
    complete_writes.push(dependency_write(policy_id, policy_expression_id, 2));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(125),
            id(9),
            9_999,
            &complete_writes,
            &[audit_event(125)],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn catalog_commit_rejects_unknown_dependency_edge_kinds() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let mut dependency = dependency_write(id(32), id(33), 5);
    dependency.key[37..39].copy_from_slice(&6u16.to_be_bytes());
    dependency.payload.as_mut().unwrap()[32 + 37..32 + 39].copy_from_slice(&6u16.to_be_bytes());
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(64),
            id(9),
            9_999,
            &[object_write(id(32), 3), object_write(id(33), 4), dependency,],
            &[audit_event(64)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

#[test]
fn catalog_commit_requires_object_owner_principals() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let owner_id = id(71);
    let object = object_write_with_owner(id(70), 3, owner_id);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(66),
            id(9),
            9_999,
            std::slice::from_ref(&object),
            &[audit_event(66)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let principal = principal_write(owner_id, 1);
    let principal_object = principal_object_write(owner_id, 1);
    let table = table_write(id(70), builtin_id(1));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(67),
            id(9),
            9_999,
            &[
                object.clone(),
                table,
                principal.clone(),
                principal_object.clone(),
            ],
            &[audit_event(67)],
        )?,
        1
    );

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(68),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: principal.key,
                    expected_value: principal.payload,
                    payload: None,
                },
                CatalogWrite {
                    key: principal_object.key,
                    expected_value: principal_object.payload,
                    payload: None,
                },
            ],
            &[audit_event(68)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn catalog_commit_requires_index_object_identity_and_kind() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let (missing_key, missing_payload) = index_definition(id(55), id(16));
    let missing_index = CatalogWrite {
        key: missing_key,
        expected_value: None,
        payload: Some(missing_payload),
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(60),
            id(9),
            9_999,
            std::slice::from_ref(&missing_index),
            &[audit_event(60)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let (index_key, index_payload) = index_definition(id(56), id(16));
    let index = CatalogWrite {
        key: index_key,
        expected_value: None,
        payload: Some(index_payload),
    };
    let index_object = object_write(id(56), 7);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(61),
            id(9),
            9_999,
            &[index, index_object.clone()],
            &[audit_event(61)],
        )?,
        1
    );

    let delete_object = CatalogWrite {
        key: index_object.key.clone(),
        expected_value: index_object.payload,
        payload: None,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(62),
            id(9),
            9_999,
            &[delete_object],
            &[audit_event(62)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let (wrong_key, wrong_payload) = index_definition(id(57), id(16));
    let wrong_index = CatalogWrite {
        key: wrong_key,
        expected_value: None,
        payload: Some(wrong_payload),
    };
    let wrong_object = object_write(id(57), 6);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(63),
            id(9),
            9_999,
            &[wrong_index, wrong_object],
            &[audit_event(63)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn catalog_commit_rejects_indexes_with_missing_or_mistabled_columns()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;

    let missing_column_id = id(90);
    let (missing_key, missing_payload) =
        index_definition_with_columns(id(89), id(16), &[missing_column_id]);
    let missing_index = CatalogWrite {
        key: missing_key,
        expected_value: None,
        payload: Some(missing_payload),
    };
    let missing_index_object = object_write(id(89), 7);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(90),
            id(9),
            9_999,
            &[missing_index, missing_index_object],
            &[audit_event(90)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let other_table_id = id(92);
    let other_column_id = id(93);
    let (mismatched_key, mismatched_payload) =
        index_definition_with_columns(id(94), id(16), &[other_column_id]);
    let mismatched_index = CatalogWrite {
        key: mismatched_key,
        expected_value: None,
        payload: Some(mismatched_payload),
    };
    let other_column = column_write(other_column_id, other_table_id, "object_93");
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(91),
            id(9),
            9_999,
            &[
                mismatched_index,
                object_write(id(94), 7),
                object_write(other_table_id, 3),
                table_write(other_table_id, id(95)),
                object_write(other_column_id, 4),
                other_column,
            ],
            &[audit_event(91)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let (valid_key, valid_payload) = index_definition_with_columns(id(96), id(16), &[id(0x66)]);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(92),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: valid_key,
                    expected_value: None,
                    payload: Some(valid_payload),
                },
                object_write(id(96), 7),
            ],
            &[audit_event(92)],
        )?,
        1,
    );
    Ok(())
}

#[test]
fn catalog_index_changes_reject_tables_with_existing_rows() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = key(77);
    let existing_index_key = index_key(&row_key);
    commit_rows_and_indexes(
        &database,
        id(76),
        id(9),
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(bytea_tuple(b"existing row")),
        }],
        &[IndexWrite {
            key: existing_index_key.clone(),
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: false,
        }],
    )?;

    let (new_index_key, new_index_payload) = index_definition(id(0x88), id(16));
    let new_index = CatalogWrite {
        key: new_index_key.clone(),
        expected_value: None,
        payload: Some(new_index_payload),
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(77),
            id(9),
            9_999,
            &[new_index, object_write(id(0x88), 7)],
            &[audit_event(77)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(77), id(9), 1)?.unwrap().status,
        OutcomeStatus::Aborted
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(new_index_key.as_slice())?.is_none());
    assert!(
        catalog
            .get(id_catalog_key(2, id(0x88)).as_slice())?
            .is_none()
    );
    assert!(
        read.open_table(schema::INDEXES)?
            .get(existing_index_key.as_slice())?
            .is_some()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[77; 16][..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn storage_manager_builds_and_drops_populated_indexes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = table_row_key(id(16), id(77));
    let payload = bytea_tuple(b"existing row");
    let previous_index = typed_bytea_index_key(id(0x44), &row_key, &payload);
    commit_rows_and_indexes(
        &database,
        id(76),
        id(9),
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(payload.clone()),
        }],
        &[IndexWrite {
            key: previous_index.key,
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: false,
        }],
    )?;

    let (new_index_key, new_index_payload) = index_definition(id(0x88), id(16));
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(77),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: new_index_key.clone(),
                    expected_value: None,
                    payload: Some(new_index_payload),
                },
                object_write(id(0x88), 7),
            ],
            &[audit_event(77)],
        )?,
        2
    );

    let expected = typed_bytea_index_key(id(0x88), &row_key, &payload);
    let read = database.begin_read()?;
    let mut reverse_key = row_key.to_vec();
    reverse_key.extend_from_slice(&expected.key[..expected.key.len() - 16]);
    let (definition_bytes, object_bytes) = {
        let indexes = read.open_table(schema::INDEXES)?;
        let locator = indexes.get(expected.key.as_slice())?.unwrap();
        assert_eq!(
            IndexLocator::decode_for_key(&expected.key, locator.value())?.row_version,
            1
        );
        assert!(
            read.open_table(schema::ROW_INDEXES)?
                .get(reverse_key.as_slice())?
                .is_some()
        );
        let catalog = read.open_table(CATALOG)?;
        let definition_bytes = catalog
            .get(new_index_key.as_slice())?
            .unwrap()
            .value()
            .to_vec();
        let object_key = id_catalog_key(2, id(0x88));
        let object_bytes = catalog
            .get(object_key.as_slice())?
            .unwrap()
            .value()
            .to_vec();
        assert!(
            read.open_table(schema::AUDIT)?
                .get(&[77; 16][..])?
                .is_some()
        );
        (definition_bytes, object_bytes)
    };
    assert_eq!(
        lookup_outcome(&database, id(77), id(9), 1)?.unwrap().status,
        OutcomeStatus::Committed
    );

    let mut object_delete = object_write(id(0x88), 7);
    object_delete.expected_value = Some(object_bytes);
    object_delete.payload = None;
    let drop_writes = [
        CatalogWrite {
            key: new_index_key.clone(),
            expected_value: Some(definition_bytes),
            payload: None,
        },
        object_delete,
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(203),
            id(9),
            9_999,
            &drop_writes,
            &[audit_event(203)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(202),
            id(9),
            9_999,
            &drop_writes,
            &[audit_event(202)],
        )?,
        3
    );
    assert!(
        read.open_table(schema::INDEXES)?
            .get(expected.key.as_slice())?
            .is_some()
    );
    assert!(
        database
            .begin_read()?
            .open_table(schema::INDEXES)?
            .get(expected.key.as_slice())?
            .is_none()
    );
    assert!(
        database
            .begin_read()?
            .open_table(schema::ROW_INDEXES)?
            .get(reverse_key.as_slice())?
            .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, id(202), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_rebuilds_populated_indexes_atomically() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let new_column_object = object_write(id(0x67), 4);
    let new_column = column_write_at_ordinal(id(0x67), id(16), 1, "object_103");
    {
        let setup = database.begin_write()?;
        for record in [new_column_object, new_column] {
            setup
                .open_table(CATALOG)?
                .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
        }
        setup.commit()?;
    }
    let row_key = table_row_key(id(16), id(73));
    let payload = two_bytea_tuple(b"old index key", b"new index key");
    let existing = typed_two_bytea_index_key(id(0x44), &row_key, &payload, id(0x66));
    commit_rows_and_indexes(
        &database,
        id(204),
        id(9),
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(payload.clone()),
        }],
        &[IndexWrite {
            key: existing.key.clone(),
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: false,
        }],
    )?;
    let old_snapshot = database.begin_read()?;
    let index_catalog_key = id_catalog_key(9, id(0x44));
    let old_definition = old_snapshot
        .open_table(CATALOG)?
        .get(index_catalog_key.as_slice())?
        .ok_or("missing existing index definition")?
        .value()
        .to_vec();
    let (same_index_key, unique_definition) =
        index_definition_with_options(id(0x44), id(16), &[id(0x67)], true);
    assert_eq!(same_index_key, index_catalog_key);
    let rebuilt = typed_two_bytea_index_key(id(0x44), &row_key, &payload, id(0x67));
    assert_ne!(existing.key, rebuilt.key);
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(208),
            id(9),
            9_999,
            &[CatalogWrite {
                key: index_catalog_key.clone(),
                expected_value: Some(old_definition.clone()),
                payload: Some(unique_definition.clone()),
            }],
            &[audit_event(208)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(205),
            id(9),
            9_999,
            &[CatalogWrite {
                key: index_catalog_key.clone(),
                expected_value: Some(old_definition.clone()),
                payload: Some(unique_definition.clone()),
            }],
            &[audit_event(205)],
        )?,
        2
    );

    let old_catalog = old_snapshot.open_table(CATALOG)?;
    assert_eq!(
        old_catalog
            .get(index_catalog_key.as_slice())?
            .unwrap()
            .value(),
        old_definition
    );
    let old_indexes = old_snapshot.open_table(schema::INDEXES)?;
    let old_locator = old_indexes.get(existing.key.as_slice())?.unwrap();
    assert_eq!(
        IndexLocator::decode_for_key(&existing.key, old_locator.value())?.row_version,
        1
    );
    let reverse_key = {
        let mut key = row_key.to_vec();
        key.extend_from_slice(&existing.key[..existing.key.len() - 16]);
        key
    };
    assert!(
        old_snapshot
            .open_table(schema::ROW_INDEXES)?
            .get(reverse_key.as_slice())?
            .is_some()
    );
    assert!(old_indexes.get(rebuilt.key.as_slice())?.is_none());

    let current = database.begin_read()?;
    assert_eq!(
        current
            .open_table(CATALOG)?
            .get(index_catalog_key.as_slice())?
            .unwrap()
            .value(),
        unique_definition
    );
    let locator = current
        .open_table(schema::INDEXES)?
        .get(rebuilt.key.as_slice())?
        .unwrap();
    assert_eq!(
        IndexLocator::decode_for_key(&rebuilt.key, locator.value())?.row_version,
        1
    );
    assert!(
        current
            .open_table(schema::INDEXES)?
            .get(existing.key.as_slice())?
            .is_none()
    );
    let rebuilt_reverse_key = {
        let mut key = row_key.to_vec();
        key.extend_from_slice(&rebuilt.key[..rebuilt.key.len() - 16]);
        key
    };
    assert!(
        current
            .open_table(schema::ROW_INDEXES)?
            .get(reverse_key.as_slice())?
            .is_none()
    );
    assert!(
        current
            .open_table(schema::ROW_INDEXES)?
            .get(rebuilt_reverse_key.as_slice())?
            .is_some()
    );
    assert_eq!(
        lookup_outcome(&database, id(205), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(current);
    drop(old_snapshot);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_rejects_unique_rebuild_with_duplicate_values()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let payload = bytea_tuple(b"duplicate rebuild");
    let row_keys = [table_row_key(id(16), id(74)), table_row_key(id(16), id(75))];
    let indexes = row_keys
        .iter()
        .map(|row_key| {
            let derived = typed_bytea_index_key(id(0x44), row_key, &payload);
            IndexWrite {
                key: derived.key,
                row_key: *row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }
        })
        .collect::<Vec<_>>();
    commit_rows_and_indexes(
        &database,
        id(206),
        id(9),
        9_999,
        &row_keys
            .iter()
            .map(|row_key| RowWrite {
                key: *row_key,
                expected_version: None,
                payload: Some(payload.clone()),
            })
            .collect::<Vec<_>>(),
        &indexes,
    )?;

    let index_catalog_key = id_catalog_key(9, id(0x44));
    let old_definition = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(index_catalog_key.as_slice())?
        .unwrap()
        .value()
        .to_vec();
    let (_, unique_definition) = index_definition_with_options(id(0x44), id(16), &[id(0x66)], true);
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(207),
            id(9),
            9_999,
            &[CatalogWrite {
                key: index_catalog_key.clone(),
                expected_value: Some(old_definition.clone()),
                payload: Some(unique_definition),
            }],
            &[audit_event(207)],
        ),
        Err(CommitError::UniqueViolation)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(207), id(9), 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert_eq!(
        read.open_table(CATALOG)?
            .get(index_catalog_key.as_slice())?
            .unwrap()
            .value(),
        old_definition
    );
    for index_write in &indexes {
        assert!(
            read.open_table(schema::INDEXES)?
                .get(index_write.key.as_slice())?
                .is_some()
        );
    }
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[207; 16][..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn storage_manager_checks_index_ddl_authority_for_current_actor()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let authorized_user = id(0x51);
    let denied_user = id(0x52);
    let admin_user = id(0x53);
    let table_owner = id(0x54);
    let mut records = vec![
        principal_write_with_name(authorized_user, 1, 1, "indexer"),
        object_write_with_name(authorized_user, 8, "indexer"),
        principal_write_with_name(denied_user, 1, 1, "no_index"),
        object_write_with_name(denied_user, 8, "no_index"),
        principal_write_with_name(admin_user, 1, 1, "database_admin_member"),
        object_write_with_name(admin_user, 8, "database_admin_member"),
        principal_write_with_name(table_owner, 1, 1, "table_owner"),
        object_write_with_name(table_owner, 8, "table_owner"),
        object_write_with_owner(id(16), 3, table_owner),
        grant_write(authorized_user, id(16), 9, 1, id(16), builtin_owner_id()),
        dependency_write(authorized_user, id(16), 5),
        membership_write(admin_user, builtin_owner_id()),
        dependency_write(admin_user, builtin_owner_id(), 5),
    ];
    {
        let setup = database.begin_write()?;
        for record in records.drain(..) {
            setup
                .open_table(CATALOG)?
                .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
        }
        setup.commit()?;
    }

    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let (allowed_index_key, allowed_index_definition) = index_definition(id(0x8b), id(16));
    let mut allowed_audit = audit_event(210);
    allowed_audit.actor_id = Some(authorized_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(210),
            authorized_user,
            9_999,
            &[
                CatalogWrite {
                    key: allowed_index_key.clone(),
                    expected_value: None,
                    payload: Some(allowed_index_definition),
                },
                object_write(id(0x8b), 7),
            ],
            &[allowed_audit],
        )?,
        1
    );

    let (admin_index_key, admin_index_definition) = index_definition(id(0x8d), id(16));
    let mut admin_audit = audit_event(212);
    admin_audit.actor_id = Some(admin_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(212),
            admin_user,
            9_999,
            &[
                CatalogWrite {
                    key: admin_index_key.clone(),
                    expected_value: None,
                    payload: Some(admin_index_definition),
                },
                object_write(id(0x8d), 7),
            ],
            &[admin_audit],
        )?,
        2
    );

    let (owned_index_key, owned_index_definition) = index_definition(id(0x8e), id(16));
    let mut owner_audit = audit_event(214);
    owner_audit.actor_id = Some(table_owner);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(214),
            table_owner,
            9_999,
            &[
                CatalogWrite {
                    key: owned_index_key.clone(),
                    expected_value: None,
                    payload: Some(owned_index_definition),
                },
                object_write(id(0x8e), 7),
            ],
            &[owner_audit],
        )?,
        3
    );

    let allowed_index_definition_key = id_catalog_key(9, id(0x8b));
    let allowed_index_object_key = id_catalog_key(2, id(0x8b));
    let (old_index_definition, old_index_object) = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        (
            catalog
                .get(allowed_index_definition_key.as_slice())?
                .unwrap()
                .value()
                .to_vec(),
            catalog
                .get(allowed_index_object_key.as_slice())?
                .unwrap()
                .value()
                .to_vec(),
        )
    };
    let (_, unique_index_definition) =
        index_definition_with_options(id(0x8b), id(16), &[id(0x66)], true);
    let owner_transfer = object_write_with_owner(id(0x8b), 7, denied_user);
    let mut transfer_audit = audit_event(213);
    transfer_audit.actor_id = Some(authorized_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(213),
            authorized_user,
            9_999,
            &[
                CatalogWrite {
                    key: allowed_index_definition_key.clone(),
                    expected_value: Some(old_index_definition.clone()),
                    payload: Some(unique_index_definition),
                },
                CatalogWrite {
                    key: allowed_index_object_key,
                    expected_value: Some(old_index_object),
                    payload: owner_transfer.payload,
                },
            ],
            &[transfer_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let (denied_index_key, denied_index_definition) = index_definition(id(0x8c), id(16));
    let mut denied_audit = audit_event(211);
    denied_audit.actor_id = Some(denied_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(211),
            denied_user,
            9_999,
            &[
                CatalogWrite {
                    key: denied_index_key.clone(),
                    expected_value: None,
                    payload: Some(denied_index_definition),
                },
                object_write(id(0x8c), 7),
            ],
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 3);
    assert_eq!(
        lookup_outcome(&database, id(210), authorized_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(212), admin_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(214), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(213), authorized_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, id(211), denied_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(allowed_index_key.as_slice())?
            .is_some()
    );
    assert!(
        read.open_table(CATALOG)?
            .get(denied_index_key.as_slice())?
            .is_none()
    );
    assert!(
        read.open_table(CATALOG)?
            .get(admin_index_key.as_slice())?
            .is_some()
    );
    assert!(
        read.open_table(CATALOG)?
            .get(owned_index_key.as_slice())?
            .is_some()
    );
    assert_eq!(
        read.open_table(CATALOG)?
            .get(allowed_index_definition_key.as_slice())?
            .unwrap()
            .value(),
        old_index_definition
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[211; 16][..])?
            .is_none()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[213; 16][..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn storage_manager_authorizes_membership_ddl_by_system_privilege_and_admin_role()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let role_manager = id(0x61);
    let denied_user = id(0x62);
    let admin_user = id(0x63);
    let first_member = id(0x64);
    let second_member = id(0x65);
    let app_role = id(0x67);
    let admin_parent_role = id(0x68);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(role_manager, 1, 1, "role_manager"),
        object_write_with_name(role_manager, 8, "role_manager"),
        principal_write_with_name(denied_user, 1, 1, "role_denied"),
        object_write_with_name(denied_user, 8, "role_denied"),
        principal_write_with_name(admin_user, 1, 1, "role_admin"),
        object_write_with_name(admin_user, 8, "role_admin"),
        principal_write_with_name(first_member, 1, 1, "first_member"),
        object_write_with_name(first_member, 8, "first_member"),
        principal_write_with_name(second_member, 1, 1, "second_member"),
        object_write_with_name(second_member, 8, "second_member"),
        principal_write_with_name(app_role, 2, 1, "app_role"),
        object_write_with_name(app_role, 9, "app_role"),
        principal_write_with_name(admin_parent_role, 2, 1, "admin_parent_role"),
        object_write_with_name(admin_parent_role, 9, "admin_parent_role"),
        grant_write(role_manager, database_id, 13, 1, database_id, admin_role),
        dependency_write(role_manager, database_id, 5),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        credential_verifier_write(id(0x6a), admin_user, &[0x6a; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(0x6b), second_member, &[0x6b; 32], 1, i64::MAX, None, 1),
        membership_write_with_grantor(admin_parent_role, admin_role, admin_role),
        dependency_write(admin_parent_role, admin_role, 5),
    ];
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let preflight = database.begin_read()?;
    let preflight_catalog = preflight.open_table(CATALOG)?;
    for record in &records {
        assert!(record.expected_value.is_none());
        assert!(
            preflight_catalog.get(record.key.as_slice())?.is_none(),
            "fixture catalog key already exists: {:?}",
            record.key
        );
    }
    drop(preflight_catalog);
    drop(preflight);
    let mut setup_audit = audit_event(79);
    setup_audit.actor_id = Some(admin_role);
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(id(79), admin_role, 9_999, &records, &[setup_audit])
        .map_err(|error| format!("membership authorization fixture setup: {error:?}"))?;
    let bump_member_epoch =
        |member_id, epoch, name: &str| -> Result<_, Box<dyn std::error::Error>> {
            let mut write = principal_write_with_name(member_id, 1, epoch, name);
            let read = database.begin_read()?;
            write.expected_value = Some(
                read.open_table(CATALOG)?
                    .get(id_catalog_key(10, member_id).as_slice())?
                    .ok_or("missing member principal")?
                    .value()
                    .to_vec(),
            );
            Ok(write)
        };
    let remove_catalog_record = |mut write: CatalogWrite| -> Result<_, Box<dyn std::error::Error>> {
        let read = database.begin_read()?;
        write.expected_value = Some(
            read.open_table(CATALOG)?
                .get(write.key.as_slice())?
                .ok_or("missing catalog record to remove")?
                .value()
                .to_vec(),
        );
        write.payload = None;
        Ok(write)
    };
    let mut allowed_audit = audit_event(80);
    allowed_audit.actor_id = Some(role_manager);
    allowed_audit.action = AuditAction::AuthorityChange;
    let manager_membership = membership_write_with_grantor(first_member, app_role, role_manager);
    assert_eq!(
        storage
            .commit_authorized_membership_ddl(
                id(80),
                role_manager,
                9_999,
                &[
                    manager_membership.clone(),
                    dependency_write(first_member, app_role, 5),
                    bump_member_epoch(first_member, 2, "first_member")?,
                ],
                &[allowed_audit],
            )
            .map_err(|error| format!("MANAGE_ROLES membership change: {error:?}"))?,
        2
    );

    let mut removal_audit = audit_event(84);
    removal_audit.actor_id = Some(role_manager);
    removal_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(84),
            role_manager,
            9_999,
            &[
                remove_catalog_record(manager_membership.clone())?,
                remove_catalog_record(dependency_write(first_member, app_role, 5))?,
                bump_member_epoch(first_member, 3, "first_member")?,
            ],
            &[removal_audit],
        )?,
        3
    );

    let denied_membership = membership_write_with_grantor(second_member, app_role, denied_user);
    let mut denied_audit = audit_event(81);
    denied_audit.actor_id = Some(denied_user);
    denied_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(81),
            denied_user,
            9_999,
            &[
                denied_membership.clone(),
                dependency_write(second_member, app_role, 5),
                bump_member_epoch(second_member, 2, "second_member")?,
            ],
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let inherited_admin_membership =
        membership_write_with_grantor(second_member, admin_parent_role, role_manager);
    let mut inherited_admin_audit = audit_event(87);
    inherited_admin_audit.actor_id = Some(role_manager);
    inherited_admin_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(87),
            role_manager,
            9_999,
            &[
                inherited_admin_membership.clone(),
                dependency_write(second_member, admin_parent_role, 5),
                bump_member_epoch(second_member, 2, "second_member")?,
            ],
            &[inherited_admin_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let admin_role_membership =
        membership_write_with_grantor(second_member, admin_role, role_manager);
    let admin_role_membership_by_admin =
        membership_write_with_grantor(second_member, admin_role, admin_user);
    let mut restricted_audit = audit_event(82);
    restricted_audit.actor_id = Some(role_manager);
    restricted_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(82),
            role_manager,
            9_999,
            &[
                admin_role_membership.clone(),
                dependency_write(second_member, admin_role, 5),
                bump_member_epoch(second_member, 2, "second_member")?,
            ],
            &[restricted_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let mut admin_audit = audit_event(83);
    admin_audit.actor_id = Some(admin_user);
    admin_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(83),
            admin_user,
            9_999,
            &[
                admin_role_membership_by_admin.clone(),
                dependency_write(second_member, admin_role, 5),
                bump_member_epoch(second_member, 2, "second_member")?,
            ],
            &[admin_audit],
        )?,
        4
    );
    let mut reduce_admin_audit = audit_event(85);
    reduce_admin_audit.actor_id = Some(admin_user);
    reduce_admin_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(85),
            admin_user,
            9_999,
            &[
                remove_catalog_record(membership_write_with_grantor(
                    admin_user, admin_role, admin_role,
                ))?,
                remove_catalog_record(dependency_write(admin_user, admin_role, 5))?,
                bump_member_epoch(admin_user, 2, "role_admin")?,
            ],
            &[reduce_admin_audit],
        )?,
        5
    );

    let mut last_admin_audit = audit_event(86);
    last_admin_audit.actor_id = Some(second_member);
    last_admin_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(86),
            second_member,
            9_999,
            &[
                remove_catalog_record(admin_role_membership_by_admin.clone())?,
                remove_catalog_record(dependency_write(second_member, admin_role, 5))?,
                bump_member_epoch(second_member, 3, "second_member")?,
            ],
            &[last_admin_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 5);
    assert_eq!(
        lookup_outcome(&database, id(80), role_manager, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(84), role_manager, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    for (invocation_id, principal_id) in [
        (id(81), denied_user),
        (id(82), role_manager),
        (id(87), role_manager),
    ] {
        assert_eq!(
            lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted
        );
    }
    assert_eq!(
        lookup_outcome(&database, id(83), admin_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(85), admin_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(86), second_member, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(manager_membership.key.as_slice())?.is_none());
    assert!(catalog.get(denied_membership.key.as_slice())?.is_none());
    assert!(
        catalog
            .get(inherited_admin_membership.key.as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(admin_role_membership_by_admin.key.as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(
                membership_write_with_grantor(admin_user, admin_role, admin_role)
                    .key
                    .as_slice()
            )?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(10, second_member).as_slice())?
            .is_some()
    );
    Ok(())
}

#[test]
fn membership_ddl_requires_an_unexpired_credential_for_a_final_admin()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0xa1);
    let added_admin = id(0xa2);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let records = [
        principal_write_with_name(admin_user, 1, 1, "expired_admin"),
        object_write_with_name(admin_user, 8, "expired_admin"),
        principal_write_with_name(added_admin, 1, 1, "expired_added_admin"),
        object_write_with_name(added_admin, 8, "expired_added_admin"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        credential_verifier_write(id(0xa3), admin_user, &[0xa3; 32], 1, 2, None, 1),
        credential_verifier_write(id(0xa4), added_admin, &[0xa4; 32], 1, 2, None, 1),
    ];
    let mut setup_audit = identity_audit_event(0xa0, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xa0), admin_role, 9_999, &records, &[setup_audit])?;

    let mut bump = principal_write_with_name(added_admin, 1, 2, "expired_added_admin");
    bump.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(10, added_admin).as_slice())?
            .ok_or("added admin principal missing")?
            .value()
            .to_vec(),
    );
    let membership = membership_write_with_grantor(added_admin, admin_role, admin_user);
    let mut audit = identity_audit_event(0xa5, admin_user, added_admin);
    audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_membership_ddl(
            id(0xa5),
            admin_user,
            9_999,
            &[
                membership,
                dependency_write(added_admin, admin_role, 5),
                bump,
            ],
            &[audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(0xa5), admin_user, 1)?
            .ok_or("missing aborted outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn storage_manager_restricts_system_privilege_ddl_to_database_admins()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0x71);
    let role_manager = id(0x72);
    let target_user = id(0x73);
    let admin_role = builtin_owner_id();
    let database_id = id(0x33);
    let records = [
        principal_write_with_name(admin_user, 1, 1, "system_admin"),
        object_write_with_name(admin_user, 8, "system_admin"),
        principal_write_with_name(role_manager, 1, 1, "system_role_manager"),
        object_write_with_name(role_manager, 8, "system_role_manager"),
        principal_write_with_name(target_user, 1, 1, "system_target"),
        object_write_with_name(target_user, 8, "system_target"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        grant_write(role_manager, database_id, 13, 1, database_id, admin_role),
        dependency_write(role_manager, database_id, 5),
    ];
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut setup_audit = audit_event(90);
    setup_audit.actor_id = Some(admin_role);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(90), admin_role, 9_999, &records, &[setup_audit])?;

    let bump_target_epoch = |epoch| -> Result<_, Box<dyn std::error::Error>> {
        let mut write = principal_write_with_name(target_user, 1, epoch, "system_target");
        let read = database.begin_read()?;
        write.expected_value = Some(
            read.open_table(CATALOG)?
                .get(id_catalog_key(10, target_user).as_slice())?
                .ok_or("missing system grant target")?
                .value()
                .to_vec(),
        );
        Ok(write)
    };
    let remove_catalog_record = |mut write: CatalogWrite| -> Result<_, Box<dyn std::error::Error>> {
        let read = database.begin_read()?;
        write.expected_value = Some(
            read.open_table(CATALOG)?
                .get(write.key.as_slice())?
                .ok_or("missing system privilege record")?
                .value()
                .to_vec(),
        );
        write.payload = None;
        Ok(write)
    };

    let manage_users_grant = grant_write(target_user, database_id, 12, 1, database_id, admin_user);
    let mut grant_audit = audit_event(91);
    grant_audit.actor_id = Some(admin_user);
    grant_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_system_privilege_ddl(
            id(91),
            admin_user,
            9_999,
            &[
                manage_users_grant.clone(),
                dependency_write(target_user, database_id, 5),
                bump_target_epoch(2)?,
            ],
            &[grant_audit],
        )?,
        2
    );

    let create_schema_grant =
        grant_write(target_user, database_id, 14, 1, database_id, role_manager);
    let mut denied_audit = audit_event(92);
    denied_audit.actor_id = Some(role_manager);
    denied_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_system_privilege_ddl(
            id(92),
            role_manager,
            9_999,
            &[
                create_schema_grant,
                dependency_write(target_user, database_id, 5),
                bump_target_epoch(3)?,
            ],
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let mut revoke_audit = audit_event(93);
    revoke_audit.actor_id = Some(admin_user);
    revoke_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_system_privilege_ddl(
            id(93),
            admin_user,
            9_999,
            &[
                remove_catalog_record(manage_users_grant.clone())?,
                remove_catalog_record(dependency_write(target_user, database_id, 5))?,
                bump_target_epoch(3)?,
            ],
            &[revoke_audit],
        )?,
        3
    );
    assert_eq!(commit_sequence(&database)?, 3);
    for (invocation_id, principal_id, status) in [
        (id(91), admin_user, OutcomeStatus::Committed),
        (id(92), role_manager, OutcomeStatus::Aborted),
        (id(93), admin_user, OutcomeStatus::Committed),
    ] {
        assert_eq!(
            lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            status
        );
    }
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(manage_users_grant.key.as_slice())?.is_none());
    assert!(
        catalog
            .get(dependency_write(target_user, database_id, 5).key.as_slice())?
            .is_none()
    );
    Ok(())
}

#[test]
fn identity_ddl_refuses_to_reuse_a_tombstoned_id() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0xc1);
    let retired_id = id(0xc2);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let setup = [
        principal_write_with_name(admin_user, 1, 1, "tombstone_admin"),
        object_write_with_name(admin_user, 8, "tombstone_admin"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0xc0, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xc0), admin_role, 9_999, &setup, &[setup_audit])?;
    storage.commit_catalog_and_audit(
        id(0xc3),
        admin_user,
        9_999,
        &[identity_tombstone_write(retired_id)],
        &[identity_audit_event(0xc3, admin_user, retired_id)],
    )?;

    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0xc4),
            admin_user,
            9_999,
            &[
                principal_write_with_name(retired_id, 1, 1, "retired_identity"),
                object_write_with_name(retired_id, 8, "retired_identity"),
            ],
            &[identity_audit_event(0xc4, admin_user, retired_id)],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(0xc5),
            admin_user,
            9_999,
            &[identity_tombstone_write(admin_user)],
            &[identity_audit_event(0xc5, admin_user, admin_user)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 2);
    assert!(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(10, retired_id).as_slice())?
            .is_none()
    );
    Ok(())
}

#[test]
fn identity_ddl_drops_unreferenced_user_and_revokes_tokens_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0xd1);
    let dropped_user = id(0xd2);
    let admin_token = id(0xd3);
    let dropped_token = id(0xd4);
    let dropped_role = id(0xd7);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut seed_audit = identity_audit_event(0xd0, admin_role, admin_user);
    seed_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xd0),
        admin_role,
        9_999,
        &[
            principal_write_with_name(admin_user, 1, 1, "drop_admin"),
            object_write_with_name(admin_user, 8, "drop_admin"),
            principal_write_with_name(dropped_user, 1, 1, "drop_target"),
            object_write_with_parent_and_owner_and_name(
                dropped_user,
                8,
                None,
                admin_user,
                "drop_target",
            ),
            principal_write_with_name(dropped_role, 2, 1, "drop_role"),
            object_write_with_parent_and_owner_and_name(
                dropped_role,
                9,
                None,
                admin_user,
                "drop_role",
            ),
            membership_write_with_grantor(admin_user, admin_role, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(admin_token, admin_user, &[0xd3; 32], 1, i64::MAX, None, 1),
            credential_verifier_write(
                dropped_token,
                dropped_user,
                &[0xd4; 32],
                1,
                i64::MAX,
                None,
                1,
            ),
        ],
        &[seed_audit],
    )?;
    let read_catalog = || -> Result<_, Box<dyn std::error::Error>> {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        Ok((
            catalog
                .get(id_catalog_key(2, dropped_user).as_slice())?
                .map(|v| v.value().to_vec()),
            catalog
                .get(id_catalog_key(10, dropped_user).as_slice())?
                .map(|v| v.value().to_vec()),
            catalog
                .get(id_catalog_key(2, dropped_role).as_slice())?
                .map(|v| v.value().to_vec()),
            catalog
                .get(id_catalog_key(10, dropped_role).as_slice())?
                .map(|v| v.value().to_vec()),
            catalog
                .get(id_catalog_key(16, dropped_token).as_slice())?
                .map(|v| v.value().to_vec()),
        ))
    };
    let (object, principal, role_object, role_principal, token) = read_catalog()?;
    let revoked_token = catalog_expect(
        credential_verifier_write(
            dropped_token,
            dropped_user,
            &[0xd4; 32],
            1,
            i64::MAX,
            Some(2),
            2,
        ),
        token.ok_or("missing target token")?,
    );
    let drop_writes = [
        catalog_delete(
            CatalogWrite {
                key: id_catalog_key(2, dropped_user),
                expected_value: None,
                payload: None,
            },
            object.ok_or("missing user object")?,
        ),
        catalog_delete(
            CatalogWrite {
                key: id_catalog_key(10, dropped_user),
                expected_value: None,
                payload: None,
            },
            principal.ok_or("missing user principal")?,
        ),
        identity_tombstone_write(dropped_user),
        catalog_delete(
            CatalogWrite {
                key: id_catalog_key(2, dropped_role),
                expected_value: None,
                payload: None,
            },
            role_object.ok_or("missing role object")?,
        ),
        catalog_delete(
            CatalogWrite {
                key: id_catalog_key(10, dropped_role),
                expected_value: None,
                payload: None,
            },
            role_principal.ok_or("missing role principal")?,
        ),
        identity_tombstone_write(dropped_role),
        revoked_token,
    ];
    let mut incomplete_drop_audit = identity_audit_event(0xd6, admin_user, dropped_user);
    incomplete_drop_audit.target_ids = vec![dropped_user];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0xd6),
            admin_user,
            9_999,
            &drop_writes[..6],
            &[incomplete_drop_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    let mut drop_audit = identity_audit_event(0xd5, admin_user, dropped_user);
    drop_audit.target_ids = vec![dropped_user, dropped_token, dropped_role];
    let drop_result = storage.commit_authorized_identity_ddl(
        id(0xd5),
        admin_user,
        9_999,
        &drop_writes,
        &[drop_audit],
    );
    assert_eq!(drop_result, Ok(2));
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(2, dropped_user).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(10, dropped_user).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(17, dropped_user).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(id_catalog_key(2, dropped_role).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(10, dropped_role).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(17, dropped_role).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(id_catalog_key(16, dropped_token).as_slice())?
            .is_some()
    );
    assert_eq!(commit_sequence(&database)?, 2);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn identity_ddl_restricts_user_drop_when_a_grant_references_the_user()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0xe1);
    let dropped_user = id(0xe2);
    let admin_token = id(0xe3);
    let dropped_token = id(0xe4);
    let admin_role = builtin_owner_id();
    let database_id = id(0x33);
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut seed_audit = identity_audit_event(0xe0, admin_role, admin_user);
    seed_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xe0),
        admin_role,
        9_999,
        &[
            principal_write_with_name(admin_user, 1, 1, "restrict_admin"),
            object_write_with_name(admin_user, 8, "restrict_admin"),
            principal_write_with_name(dropped_user, 1, 1, "restrict_target"),
            object_write_with_parent_and_owner_and_name(
                dropped_user,
                8,
                None,
                admin_user,
                "restrict_target",
            ),
            membership_write_with_grantor(admin_user, admin_role, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(admin_token, admin_user, &[0xe3; 32], 1, i64::MAX, None, 1),
            credential_verifier_write(
                dropped_token,
                dropped_user,
                &[0xe4; 32],
                1,
                i64::MAX,
                None,
                1,
            ),
            grant_write(dropped_user, database_id, 12, 1, database_id, admin_role),
            dependency_write(dropped_user, database_id, 5),
        ],
        &[seed_audit],
    )?;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let object = catalog
        .get(id_catalog_key(2, dropped_user).as_slice())?
        .ok_or("missing user object")?
        .value()
        .to_vec();
    let principal = catalog
        .get(id_catalog_key(10, dropped_user).as_slice())?
        .ok_or("missing user principal")?
        .value()
        .to_vec();
    let token = catalog
        .get(id_catalog_key(16, dropped_token).as_slice())?
        .ok_or("missing target token")?
        .value()
        .to_vec();
    let grant = grant_write(dropped_user, database_id, 12, 1, database_id, admin_role);
    let mut drop_audit = identity_audit_event(0xe5, admin_user, dropped_user);
    drop_audit.target_ids = vec![dropped_user, dropped_token];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0xe5),
            admin_user,
            9_999,
            &[
                catalog_delete(
                    CatalogWrite {
                        key: id_catalog_key(2, dropped_user),
                        expected_value: None,
                        payload: None,
                    },
                    object,
                ),
                catalog_delete(
                    CatalogWrite {
                        key: id_catalog_key(10, dropped_user),
                        expected_value: None,
                        payload: None,
                    },
                    principal,
                ),
                identity_tombstone_write(dropped_user),
                catalog_expect(
                    credential_verifier_write(
                        dropped_token,
                        dropped_user,
                        &[0xe4; 32],
                        1,
                        i64::MAX,
                        Some(2),
                        2,
                    ),
                    token,
                ),
            ],
            &[drop_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert!(catalog.get(grant.key.as_slice())?.is_some());
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn storage_manager_authorizes_identity_ddl_and_preserves_a_login_admin()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0x85);
    let user_manager = id(0x86);
    let created_user = id(0x87);
    let created_role = id(0x88);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let seed_records = [
        principal_write_with_name(admin_user, 1, 1, "identity_admin"),
        object_write_with_name(admin_user, 8, "identity_admin"),
        principal_write_with_name(user_manager, 1, 1, "identity_user_manager"),
        object_write_with_name(user_manager, 8, "identity_user_manager"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        credential_verifier_write(id(0x8d), admin_user, &[0x8d; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(0x8e), admin_user, &[0x8e; 32], 1, 2, None, 1),
        grant_write(user_manager, database_id, 12, 1, database_id, admin_role),
        dependency_write(user_manager, database_id, 5),
    ];
    let mut seed_audit = identity_audit_event(0x80, admin_role, admin_user);
    seed_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x80), admin_role, 9_999, &seed_records, &[seed_audit])?;

    let user_create = [
        principal_write_with_name(created_user, 1, 1, "created_user"),
        object_write_with_parent_and_owner_and_name(
            created_user,
            8,
            None,
            user_manager,
            "created_user",
        ),
    ];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x81),
            user_manager,
            9_999,
            &user_create,
            &[identity_audit_event(0x81, user_manager, created_user)],
        )?,
        2
    );

    let role_create = [
        principal_write_with_name(created_role, 2, 1, "created_role"),
        object_write_with_parent_and_owner_and_name(
            created_role,
            9,
            None,
            user_manager,
            "created_role",
        ),
    ];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x82),
            user_manager,
            9_999,
            &role_create,
            &[identity_audit_event(0x82, user_manager, created_role)],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let current_value = |key: &[u8]| -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(database
            .begin_read()?
            .open_table(CATALOG)?
            .get(key)?
            .ok_or("missing identity record")?
            .value()
            .to_vec())
    };
    let admin_token_secret = [0x8d; 32];
    let mut revoke_admin_token = credential_verifier_write(
        id(0x8d),
        admin_user,
        &admin_token_secret,
        1,
        i64::MAX,
        Some(2),
        2,
    );
    revoke_admin_token.expected_value = Some(current_value(&revoke_admin_token.key)?);
    let mut revoke_admin_audit = identity_audit_event(0x95, user_manager, admin_user);
    revoke_admin_audit.action = AuditAction::TokenLifecycle;
    revoke_admin_audit.target_ids = vec![admin_user, id(0x8d)];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x95),
            user_manager,
            9_999,
            &[revoke_admin_token],
            &[revoke_admin_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert!(!storage.list_tokens(user_manager, admin_user)?[0].revoked);

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_micros(),
    )?;
    let replacement_event = AuditEvent {
        event_id: id(0x96),
        actor_id: Some(user_manager),
        correlation_id: id(0x97),
        occurred_at_us: now,
        action: AuditAction::TokenLifecycle,
        result: AuditResult::Succeeded,
        target_ids: vec![admin_user],
    };
    let _replacement = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0x96),
        authenticated_actor_id: user_manager,
        user_id: admin_user,
        label: "replacement admin token",
        expires_at_us: now + 3_600_000_000,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: replacement_event,
    })?;
    let revoke_old_event = AuditEvent {
        event_id: id(0x98),
        actor_id: Some(user_manager),
        correlation_id: id(0x99),
        occurred_at_us: now,
        action: AuditAction::TokenLifecycle,
        result: AuditResult::Succeeded,
        target_ids: vec![id(0x8d)],
    };
    storage.revoke_token(
        id(0x98),
        user_manager,
        id(0x8d),
        now + 7_200_000_000,
        revoke_old_event,
    )?;

    let mut rename_user = principal_write_with_name(created_user, 1, 2, "renamed_user");
    rename_user.expected_value = Some(current_value(&rename_user.key)?);
    let mut rename_object =
        principal_object_rename_write(created_user, 8, user_manager, "renamed_user", 2);
    rename_object.expected_value = Some(current_value(&rename_object.key)?);
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x83),
            user_manager,
            9_999,
            &[rename_user, rename_object],
            &[identity_audit_event(0x83, user_manager, created_user)],
        )?,
        5
    );

    let user_token_secret = [0x47; 32];
    let active_user_token = credential_verifier_write(
        id(0x8b),
        created_user,
        &user_token_secret,
        1,
        i64::MAX,
        None,
        1,
    );
    let active_user_token_payload = active_user_token.payload.clone().unwrap();
    let mut token_audit = identity_audit_event(0x8b, user_manager, created_user);
    token_audit.target_ids = vec![created_user, id(0x8b)];
    storage.commit_catalog_and_audit(
        id(0x8b),
        user_manager,
        9_999,
        &[active_user_token],
        &[token_audit],
    )?;

    let mut disable_user = principal_write_with_login(created_user, 1, false, 3, "renamed_user");
    disable_user.expected_value = Some(current_value(&disable_user.key)?);
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x84),
            user_manager,
            9_999,
            std::slice::from_ref(&disable_user),
            &[identity_audit_event(0x84, user_manager, created_user)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let mut revoked_user_token = credential_verifier_write(
        id(0x8b),
        created_user,
        &user_token_secret,
        1,
        i64::MAX,
        Some(2),
        2,
    );
    revoked_user_token.expected_value = Some(active_user_token_payload);
    let mut disable_and_revoke = disable_user;
    disable_and_revoke.expected_value = Some(current_value(&disable_and_revoke.key)?);
    let mut revoke_audit = identity_audit_event(0x8c, user_manager, created_user);
    revoke_audit.target_ids = vec![created_user, id(0x8b)];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x8c),
            user_manager,
            9_999,
            &[disable_and_revoke, revoked_user_token],
            &[revoke_audit],
        )?,
        7
    );

    let mut disable_last_admin =
        principal_write_with_login(admin_user, 1, false, 2, "identity_admin");
    disable_last_admin.expected_value = Some(current_value(&disable_last_admin.key)?);
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x89),
            user_manager,
            9_999,
            &[disable_last_admin],
            &[identity_audit_event(0x89, user_manager, admin_user)],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let admin_role_create = [
        principal_write_with_name(created_role, 2, 1, "created_role"),
        object_write_with_parent_and_owner_and_name(
            created_role,
            9,
            None,
            admin_user,
            "created_role",
        ),
    ];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0x8a),
            admin_user,
            9_999,
            &admin_role_create,
            &[identity_audit_event(0x8a, admin_user, created_role)],
        )?,
        8
    );
    assert_eq!(commit_sequence(&database)?, 8);
    Ok(())
}

#[test]
fn identity_ddl_cannot_disable_the_last_credentialed_admin()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_to_disable = id(0xb1);
    let expired_admin = id(0xb2);
    let user_manager = id(0xb3);
    let active_token_id = id(0xb4);
    let expired_token_id = id(0xb5);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let active_secret = [0xb4; 32];
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let records = [
        principal_write_with_name(admin_to_disable, 1, 1, "admin_to_disable"),
        object_write_with_name(admin_to_disable, 8, "admin_to_disable"),
        principal_write_with_name(expired_admin, 1, 1, "expired_admin"),
        object_write_with_name(expired_admin, 8, "expired_admin"),
        principal_write_with_name(user_manager, 1, 1, "credential_manager"),
        object_write_with_name(user_manager, 8, "credential_manager"),
        membership_write_with_grantor(admin_to_disable, admin_role, admin_role),
        membership_write_with_grantor(expired_admin, admin_role, admin_role),
        dependency_write(admin_to_disable, admin_role, 5),
        dependency_write(expired_admin, admin_role, 5),
        credential_verifier_write(
            active_token_id,
            admin_to_disable,
            &active_secret,
            1,
            i64::MAX,
            None,
            1,
        ),
        credential_verifier_write(expired_token_id, expired_admin, &[0xb5; 32], 1, 2, None, 1),
        grant_write(user_manager, database_id, 12, 1, database_id, admin_role),
        dependency_write(user_manager, database_id, 5),
    ];
    let mut setup_audit = identity_audit_event(0xb0, admin_role, admin_to_disable);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb0), admin_role, 9_999, &records, &[setup_audit])?;

    let mut disable = principal_write_with_login(admin_to_disable, 1, false, 2, "admin_to_disable");
    let mut revoke = credential_verifier_write(
        active_token_id,
        admin_to_disable,
        &active_secret,
        1,
        i64::MAX,
        Some(2),
        2,
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        disable.expected_value = Some(
            catalog
                .get(disable.key.as_slice())?
                .ok_or("admin principal missing")?
                .value()
                .to_vec(),
        );
        revoke.expected_value = Some(
            catalog
                .get(revoke.key.as_slice())?
                .ok_or("admin token missing")?
                .value()
                .to_vec(),
        );
    }
    let mut audit = identity_audit_event(0xb6, user_manager, admin_to_disable);
    audit.target_ids = vec![admin_to_disable, active_token_id];
    assert_eq!(
        storage.commit_authorized_identity_ddl(
            id(0xb6),
            user_manager,
            9_999,
            &[disable, revoke],
            &[audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(0xb6), user_manager, 1)?
            .ok_or("missing aborted outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn storage_manager_authorizes_object_grants_for_owners_and_manage_grants_holders()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_owner = id(0x74);
    let grant_manager = id(0x75);
    let denied_user = id(0x76);
    let owner_grantee = id(0x77);
    let manager_grantee = id(0x78);
    let denied_grantee = id(0x79);
    let admin_role = builtin_owner_id();
    let records = vec![
        principal_write_with_name(table_owner, 1, 1, "table_owner"),
        object_write_with_name(table_owner, 8, "table_owner"),
        principal_write_with_name(grant_manager, 1, 1, "grant_manager"),
        object_write_with_name(grant_manager, 8, "grant_manager"),
        principal_write_with_name(denied_user, 1, 1, "grant_denied"),
        object_write_with_name(denied_user, 8, "grant_denied"),
        principal_write_with_name(owner_grantee, 1, 1, "owner_grantee"),
        object_write_with_name(owner_grantee, 8, "owner_grantee"),
        principal_write_with_name(manager_grantee, 1, 1, "manager_grantee"),
        object_write_with_name(manager_grantee, 8, "manager_grantee"),
        principal_write_with_name(denied_grantee, 1, 1, "denied_grantee"),
        object_write_with_name(denied_grantee, 8, "denied_grantee"),
        grant_write(grant_manager, id(16), 11, 1, id(16), admin_role),
        dependency_write(grant_manager, id(16), 5),
    ];
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let old_table_object = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(id_catalog_key(2, id(16)).as_slice())?
        .ok_or("missing table object")?
        .value()
        .to_vec();
    let mut owner_table = object_write_with_owner(id(16), 3, table_owner);
    owner_table.expected_value = Some(old_table_object);
    let mut setup_writes = records;
    setup_writes.push(owner_table);
    let mut setup_audit = audit_event(100);
    setup_audit.actor_id = Some(admin_role);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(100), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let bump_grantee_epoch =
        |grantee_id, epoch, name: &str| -> Result<_, Box<dyn std::error::Error>> {
            let mut write = principal_write_with_name(grantee_id, 1, epoch, name);
            let read = database.begin_read()?;
            write.expected_value = Some(
                read.open_table(CATALOG)?
                    .get(id_catalog_key(10, grantee_id).as_slice())?
                    .ok_or("missing grant grantee")?
                    .value()
                    .to_vec(),
            );
            Ok(write)
        };
    let remove_catalog_record = |mut write: CatalogWrite| -> Result<_, Box<dyn std::error::Error>> {
        let read = database.begin_read()?;
        write.expected_value = Some(
            read.open_table(CATALOG)?
                .get(write.key.as_slice())?
                .ok_or("missing grant record")?
                .value()
                .to_vec(),
        );
        write.payload = None;
        Ok(write)
    };

    let owner_select = grant_write(owner_grantee, id(16), 2, 1, id(16), table_owner);
    let mut owner_audit = audit_event(101);
    owner_audit.actor_id = Some(table_owner);
    owner_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_object_grant_ddl(
            id(101),
            table_owner,
            9_999,
            &[
                owner_select.clone(),
                dependency_write(owner_grantee, id(16), 5),
                bump_grantee_epoch(owner_grantee, 2, "owner_grantee")?,
            ],
            &[owner_audit],
        )?,
        2
    );

    let manager_update = grant_write(manager_grantee, id(16), 4, 1, id(16), grant_manager);
    let mut manager_audit = audit_event(102);
    manager_audit.actor_id = Some(grant_manager);
    manager_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_object_grant_ddl(
            id(102),
            grant_manager,
            9_999,
            &[
                manager_update.clone(),
                dependency_write(manager_grantee, id(16), 5),
                bump_grantee_epoch(manager_grantee, 2, "manager_grantee")?,
            ],
            &[manager_audit],
        )?,
        3
    );

    let denied_insert = grant_write(denied_grantee, id(16), 3, 1, id(16), denied_user);
    let mut denied_audit = audit_event(103);
    denied_audit.actor_id = Some(denied_user);
    denied_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_object_grant_ddl(
            id(103),
            denied_user,
            9_999,
            &[
                denied_insert.clone(),
                bump_grantee_epoch(denied_grantee, 2, "denied_grantee")?,
            ],
            &[denied_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let mut revoke_audit = audit_event(104);
    revoke_audit.actor_id = Some(table_owner);
    revoke_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage.commit_authorized_object_grant_ddl(
            id(104),
            table_owner,
            9_999,
            &[
                remove_catalog_record(owner_select.clone())?,
                remove_catalog_record(dependency_write(owner_grantee, id(16), 5))?,
                bump_grantee_epoch(owner_grantee, 3, "owner_grantee")?,
            ],
            &[revoke_audit],
        )?,
        4
    );
    assert_eq!(commit_sequence(&database)?, 4);
    for (invocation_id, principal_id, status) in [
        (id(101), table_owner, OutcomeStatus::Committed),
        (id(102), grant_manager, OutcomeStatus::Committed),
        (id(103), denied_user, OutcomeStatus::Aborted),
        (id(104), table_owner, OutcomeStatus::Committed),
    ] {
        assert_eq!(
            lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            status
        );
    }
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(owner_select.key.as_slice())?.is_none());
    assert!(catalog.get(manager_update.key.as_slice())?.is_some());
    assert!(catalog.get(denied_insert.key.as_slice())?.is_none());
    assert!(
        catalog
            .get(dependency_write(owner_grantee, id(16), 5).key.as_slice())?
            .is_none()
    );
    Ok(())
}

#[test]
fn storage_manager_authorizes_policy_ddl_for_owners_and_inherited_manage_policies()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::Expr;
    use prdb_engine::catalog::{PolicyTarget, encode_policy_target};
    use prdb_engine::tuple::ScalarValue;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_owner = id(0x30);
    let policy_manager = id(0x31);
    let denied_user = id(0x32);
    let manager_role = id(0x33);
    let schema_id = id(0x34);
    let table_id = id(0x35);
    let policy_id = id(0x36);
    let expression_id = id(0x37);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let schema_object = object_write_with_parent_and_owner_and_name(
        schema_id,
        1,
        None,
        admin_role,
        "policy_schema",
    );
    let table_object = object_write_with_parent_and_owner_and_name(
        table_id,
        3,
        Some(schema_id),
        table_owner,
        "policy_table",
    );
    let policy_key = id_catalog_key(13, policy_id);
    let table_key = id_catalog_key(5, table_id);
    let policy_payload_for =
        |target_policy_id: [u8; 16], target_expression_id: [u8; 16], epoch: u64| {
            let key = id_catalog_key(13, target_policy_id);
            encode_envelope(
                &family_schema(13).unwrap(),
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
                        payload: target_expression_id.to_vec(),
                    },
                    CatalogField {
                        tag: 7,
                        wire_type: WireType::U64,
                        payload: epoch.to_le_bytes().to_vec(),
                    },
                ],
            )
            .unwrap()
        };
    let policy_payload = |epoch| policy_payload_for(policy_id, expression_id, epoch);
    let table_payload = |epoch: u64| {
        table_write_with_versions(table_id, schema_id, 1, epoch)
            .payload
            .unwrap()
    };
    let policy_target_payload = encode_policy_target(&PolicyTarget {
        policy_id,
        kind: 2,
        target_id: manager_role,
    })?;
    let policy_target = CatalogWrite {
        key: policy_target_payload[32..].to_vec(),
        expected_value: None,
        payload: Some(policy_target_payload),
    };
    let mut setup_audit = audit_event(0x80);
    setup_audit.actor_id = Some(admin_role);
    let setup_writes = vec![
        principal_write_with_name(table_owner, 1, 1, "policy_table_owner"),
        object_write_with_name(table_owner, 8, "policy_table_owner"),
        principal_write_with_name(policy_manager, 1, 1, "policy_manager"),
        object_write_with_name(policy_manager, 8, "policy_manager"),
        principal_write_with_name(denied_user, 1, 1, "policy_denied"),
        object_write_with_name(denied_user, 8, "policy_denied"),
        principal_write_with_name(manager_role, 2, 1, "policy_managers"),
        object_write_with_name(manager_role, 9, "policy_managers"),
        membership_write(policy_manager, manager_role),
        dependency_write(policy_manager, manager_role, 5),
        grant_write(manager_role, schema_id, 10, 1, schema_id, admin_role),
        dependency_write(manager_role, schema_id, 5),
        schema_object,
        schema_write(schema_id),
        table_object,
        table_write_with_versions(table_id, schema_id, 1, 1),
        dependency_write(table_id, schema_id, 1),
        CatalogWrite {
            key: policy_key.clone(),
            expected_value: None,
            payload: Some(policy_payload(1)),
        },
        object_write_with_parent_and_owner_and_name(
            policy_id,
            10,
            None,
            table_owner,
            "visible_rows",
        ),
        expression_write(
            expression_id,
            3,
            &[],
            &Expr::Literal(Some(ScalarValue::Boolean(true))),
        ),
        object_write_with_parent_and_owner_and_name(
            expression_id,
            5,
            None,
            table_owner,
            "visible_rows_expression",
        ),
        dependency_write(policy_id, expression_id, 2),
        dependency_write(policy_id, manager_role, 5),
        policy_target,
    ];
    assert_eq!(
        storage
            .commit_catalog_and_audit(id(0x80), admin_role, 9_999, &setup_writes, &[setup_audit],)
            .map_err(|error| format!("base catalog setup: {error:?}"))?,
        1
    );

    let change_policy = |policy_epoch: u64, table_epoch: u64| {
        vec![
            CatalogWrite {
                key: policy_key.clone(),
                expected_value: Some(policy_payload(policy_epoch - 1)),
                payload: Some(policy_payload(policy_epoch)),
            },
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_payload(table_epoch - 1)),
                payload: Some(table_payload(table_epoch)),
            },
        ]
    };
    let policy_audit = |event_id, actor_id| {
        let mut event = audit_event(event_id);
        event.actor_id = Some(actor_id);
        event.action = AuditAction::Ddl;
        event.target_ids = vec![policy_id];
        event
    };
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x81),
            table_owner,
            9_999,
            &change_policy(2, 2),
            &[policy_audit(0x81, table_owner)],
        )?,
        2
    );
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x82),
            policy_manager,
            9_999,
            &change_policy(3, 3),
            &[policy_audit(0x82, policy_manager)],
        )?,
        3
    );

    let added_policy_id = id(0x38);
    let added_expression_id = id(0x39);
    let added_policy_key = id_catalog_key(13, added_policy_id);
    let added_policy_payload =
        |epoch| policy_payload_for(added_policy_id, added_expression_id, epoch);
    let added_policy_object = object_write_with_parent_and_owner_and_name(
        added_policy_id,
        10,
        None,
        table_owner,
        "additional_rows",
    );
    let added_expression = expression_write(
        added_expression_id,
        3,
        &[],
        &Expr::Literal(Some(ScalarValue::Boolean(true))),
    );
    let added_expression_object = object_write_with_parent_and_owner_and_name(
        added_expression_id,
        5,
        None,
        table_owner,
        "additional_rows_expression",
    );
    let added_target_payload = encode_policy_target(&PolicyTarget {
        policy_id: added_policy_id,
        kind: 2,
        target_id: manager_role,
    })?;
    let added_target = CatalogWrite {
        key: added_target_payload[32..].to_vec(),
        expected_value: None,
        payload: Some(added_target_payload.clone()),
    };
    let added_expression_edge = dependency_write(added_policy_id, added_expression_id, 2);
    let added_recipient_edge = dependency_write(added_policy_id, manager_role, 5);
    let mut add_table_epoch = table_write_with_versions(table_id, schema_id, 1, 4);
    add_table_epoch.expected_value = Some(table_payload(3));
    let mut create_audit = policy_audit(0x84, table_owner);
    create_audit.target_ids = vec![added_policy_id];
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x84),
            table_owner,
            9_999,
            &[
                CatalogWrite {
                    key: added_policy_key.clone(),
                    expected_value: None,
                    payload: Some(added_policy_payload(1)),
                },
                added_policy_object.clone(),
                added_expression.clone(),
                added_expression_object.clone(),
                added_target.clone(),
                added_expression_edge.clone(),
                added_recipient_edge.clone(),
                add_table_epoch,
            ],
            &[create_audit.clone()],
        )?,
        4
    );

    let mut deleted_policy_object = added_policy_object.clone();
    deleted_policy_object.expected_value = added_policy_object.payload.clone();
    deleted_policy_object.payload = None;
    let mut deleted_expression = added_expression.clone();
    deleted_expression.expected_value = added_expression.payload.clone();
    deleted_expression.payload = None;
    let mut deleted_expression_object = added_expression_object.clone();
    deleted_expression_object.expected_value = added_expression_object.payload.clone();
    deleted_expression_object.payload = None;
    let mut deleted_target = added_target.clone();
    deleted_target.expected_value = Some(added_target_payload);
    deleted_target.payload = None;
    let mut deleted_expression_edge = added_expression_edge.clone();
    deleted_expression_edge.expected_value = added_expression_edge.payload.clone();
    deleted_expression_edge.payload = None;
    let mut deleted_recipient_edge = added_recipient_edge.clone();
    deleted_recipient_edge.expected_value = added_recipient_edge.payload.clone();
    deleted_recipient_edge.payload = None;
    let mut delete_table_epoch = table_write_with_versions(table_id, schema_id, 1, 5);
    delete_table_epoch.expected_value = Some(table_payload(4));
    let deleted_policy = CatalogWrite {
        key: added_policy_key.clone(),
        expected_value: Some(added_policy_payload(1)),
        payload: None,
    };
    let mut delete_audit = policy_audit(0x85, table_owner);
    delete_audit.target_ids = vec![added_policy_id];
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x85),
            table_owner,
            9_999,
            &[
                deleted_policy.clone(),
                deleted_policy_object,
                deleted_expression,
                deleted_expression_object,
                deleted_target,
                deleted_expression_edge,
                deleted_recipient_edge,
                delete_table_epoch,
            ],
            &[delete_audit],
        )?,
        5
    );

    let unassigned_policy_id = id(0x40);
    let unassigned_expression_id = id(0x41);
    let unassigned_policy_key = id_catalog_key(13, unassigned_policy_id);
    let unassigned_policy_payload =
        policy_payload_for(unassigned_policy_id, unassigned_expression_id, 1);
    let unassigned_policy_object = object_write_with_parent_and_owner_and_name(
        unassigned_policy_id,
        10,
        None,
        table_owner,
        "unassigned_rows",
    );
    let unassigned_expression = expression_write(
        unassigned_expression_id,
        3,
        &[],
        &Expr::Literal(Some(ScalarValue::Boolean(true))),
    );
    let unassigned_expression_object = object_write_with_parent_and_owner_and_name(
        unassigned_expression_id,
        5,
        None,
        table_owner,
        "unassigned_rows_expression",
    );
    let mut unassigned_table_epoch = table_write_with_versions(table_id, schema_id, 1, 6);
    unassigned_table_epoch.expected_value = Some(table_payload(5));
    let mut unassigned_audit = policy_audit(0x86, table_owner);
    unassigned_audit.target_ids = vec![unassigned_policy_id];
    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x86),
            table_owner,
            9_999,
            &[
                CatalogWrite {
                    key: unassigned_policy_key.clone(),
                    expected_value: None,
                    payload: Some(unassigned_policy_payload),
                },
                unassigned_policy_object,
                unassigned_expression,
                unassigned_expression_object,
                dependency_write(unassigned_policy_id, unassigned_expression_id, 2),
                unassigned_table_epoch,
            ],
            &[unassigned_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    assert_eq!(
        storage.commit_authorized_policy_ddl(
            id(0x83),
            denied_user,
            9_999,
            &change_policy(4, 6),
            &[policy_audit(0x83, denied_user)],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    assert_eq!(commit_sequence(&database)?, 5);
    assert_eq!(
        lookup_outcome(&database, id(0x81), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0x82), policy_manager, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0x84), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0x85), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        lookup_outcome(&database, id(0x86), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, id(0x83), denied_user, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert_eq!(
        catalog.get(policy_key.as_slice())?.unwrap().value(),
        policy_payload(3).as_slice()
    );
    assert_eq!(
        catalog.get(table_key.as_slice())?.unwrap().value(),
        table_payload(5).as_slice()
    );
    assert!(catalog.get(added_policy_key.as_slice())?.is_none());
    Ok(())
}

#[test]
fn storage_manager_separates_foreign_key_alter_and_references_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let alter_and_reference_user = id(0x90);
    let reference_only_user = id(0x91);
    let alter_only_user = id(0x92);
    let target_table_id = id(81);
    let source_table_id = id(82);
    let target_column_id = id(83);
    let target_constraint_id = id(85);
    let foreign_key_id = id(86);
    let schema_id = id(80);
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let foreign_key_key = id_catalog_key(8, foreign_key_id);
    let foreign_key_object_key = id_catalog_key(2, foreign_key_id);
    let target_edge = dependency_write(foreign_key_id, target_constraint_id, 3);

    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != foreign_key_key
                && write.key != foreign_key_object_key
                && write.key != target_edge.key
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(alter_and_reference_user, 1, 1, "alter_reference_user"),
        object_write_with_name(alter_and_reference_user, 8, "alter_reference_user"),
        principal_write_with_name(reference_only_user, 1, 1, "reference_only_user"),
        object_write_with_name(reference_only_user, 8, "reference_only_user"),
        principal_write_with_name(alter_only_user, 1, 1, "alter_only_user"),
        object_write_with_name(alter_only_user, 8, "alter_only_user"),
    ]);
    let mut setup_audit = audit_event(0x70);
    setup_audit.actor_id = Some(admin_role);
    assert_eq!(
        storage
            .commit_catalog_and_audit(id(0x70), admin_role, 9_999, &setup_writes, &[setup_audit],)
            .map_err(|error| format!("base catalog setup: {error:?}"))?,
        1
    );
    let bump_principal_epoch = |principal_id, name: &str| {
        let mut write = principal_write_with_name(principal_id, 1, 2, name);
        write.expected_value = Some(
            principal_write_with_name(principal_id, 1, 1, name)
                .payload
                .unwrap(),
        );
        write
    };
    let authority_setup = [
        grant_write(
            alter_and_reference_user,
            source_table_id,
            8,
            1,
            source_table_id,
            admin_role,
        ),
        dependency_write(alter_and_reference_user, source_table_id, 5),
        grant_write(
            alter_and_reference_user,
            target_table_id,
            6,
            2,
            target_column_id,
            admin_role,
        ),
        dependency_write(alter_and_reference_user, target_column_id, 5),
        grant_write(
            reference_only_user,
            target_table_id,
            6,
            1,
            target_table_id,
            admin_role,
        ),
        dependency_write(reference_only_user, target_table_id, 5),
        grant_write(
            alter_only_user,
            source_table_id,
            8,
            1,
            source_table_id,
            admin_role,
        ),
        dependency_write(alter_only_user, source_table_id, 5),
        bump_principal_epoch(alter_and_reference_user, "alter_reference_user"),
        bump_principal_epoch(reference_only_user, "reference_only_user"),
        bump_principal_epoch(alter_only_user, "alter_only_user"),
    ];
    let mut authority_setup_audit = audit_event(0x75);
    authority_setup_audit.actor_id = Some(admin_role);
    assert_eq!(
        storage
            .commit_catalog_and_audit(
                id(0x75),
                admin_role,
                9_999,
                &authority_setup,
                &[authority_setup_audit],
            )
            .map_err(|error| format!("authority setup: {error:?}"))?,
        2
    );

    let foreign_key_definition = foreign_key_catalog_writes(true, true)
        .into_iter()
        .find(|write| write.key == foreign_key_key)
        .ok_or("missing foreign-key test definition")?;
    let foreign_key_object = object_write(foreign_key_id, 6);
    let add_edge = target_edge.clone();
    let mut source_table_add = table_write_with_versions(source_table_id, schema_id, 1, 2);
    source_table_add.expected_value = table_write(source_table_id, schema_id).payload;
    let mut target_table_add = table_write_with_versions(target_table_id, schema_id, 1, 2);
    target_table_add.expected_value = table_write(target_table_id, schema_id).payload;
    let mut alter_only_audit = audit_event(0x71);
    alter_only_audit.actor_id = Some(alter_only_user);
    assert_eq!(
        storage.commit_authorized_foreign_key_ddl(
            id(0x71),
            alter_only_user,
            9_999,
            &[
                foreign_key_definition.clone(),
                foreign_key_object.clone(),
                add_edge.clone(),
                source_table_add.clone(),
                target_table_add.clone(),
            ],
            &[alter_only_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    let mut alter_reference_audit = audit_event(0x72);
    alter_reference_audit.actor_id = Some(alter_and_reference_user);
    assert_eq!(
        storage.commit_authorized_foreign_key_ddl(
            id(0x72),
            alter_and_reference_user,
            9_999,
            &[
                foreign_key_definition.clone(),
                foreign_key_object.clone(),
                add_edge.clone(),
                source_table_add,
                target_table_add,
            ],
            &[alter_reference_audit],
        )?,
        3
    );

    let mut removed_foreign_key = foreign_key_definition.clone();
    removed_foreign_key.expected_value = foreign_key_definition.payload.clone();
    removed_foreign_key.payload = None;
    let mut removed_object = foreign_key_object.clone();
    removed_object.expected_value = foreign_key_object.payload.clone();
    removed_object.payload = None;
    let mut removed_edge = add_edge.clone();
    removed_edge.expected_value = add_edge.payload.clone();
    removed_edge.payload = None;
    let mut source_table_drop = table_write_with_versions(source_table_id, schema_id, 1, 3);
    source_table_drop.expected_value =
        table_write_with_versions(source_table_id, schema_id, 1, 2).payload;
    let mut target_table_drop = table_write_with_versions(target_table_id, schema_id, 1, 3);
    target_table_drop.expected_value =
        table_write_with_versions(target_table_id, schema_id, 1, 2).payload;
    let mut alter_only_drop_audit = audit_event(0x73);
    alter_only_drop_audit.actor_id = Some(alter_only_user);
    assert_eq!(
        storage.commit_authorized_foreign_key_ddl(
            id(0x73),
            alter_only_user,
            9_999,
            &[
                removed_foreign_key,
                removed_object,
                removed_edge,
                source_table_drop,
                target_table_drop,
            ],
            &[alter_only_drop_audit],
        )?,
        4
    );

    let mut reference_only_audit = audit_event(0x74);
    reference_only_audit.actor_id = Some(reference_only_user);
    let mut source_table_reference_only =
        table_write_with_versions(source_table_id, schema_id, 1, 4);
    source_table_reference_only.expected_value =
        table_write_with_versions(source_table_id, schema_id, 1, 3).payload;
    let mut target_table_reference_only =
        table_write_with_versions(target_table_id, schema_id, 1, 4);
    target_table_reference_only.expected_value =
        table_write_with_versions(target_table_id, schema_id, 1, 3).payload;
    assert_eq!(
        storage.commit_authorized_foreign_key_ddl(
            id(0x74),
            reference_only_user,
            9_999,
            &[
                foreign_key_definition,
                foreign_key_object,
                add_edge.clone(),
                source_table_reference_only,
                target_table_reference_only,
            ],
            &[reference_only_audit],
        ),
        Err(CommitError::DdlAuthorizationDenied)
    );

    assert_eq!(commit_sequence(&database)?, 4);
    for (invocation_id, principal_id, status) in [
        (id(0x71), alter_only_user, OutcomeStatus::Aborted),
        (id(0x72), alter_and_reference_user, OutcomeStatus::Committed),
        (id(0x73), alter_only_user, OutcomeStatus::Committed),
        (id(0x74), reference_only_user, OutcomeStatus::Aborted),
    ] {
        assert_eq!(
            lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            status
        );
    }
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(foreign_key_key.as_slice())?.is_none());
    assert!(catalog.get(foreign_key_object_key.as_slice())?.is_none());
    assert!(catalog.get(add_edge.key.as_slice())?.is_none());
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[0x71; 16][..])?
            .is_none()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[0x74; 16][..])?
            .is_none()
    );
    Ok(())
}

#[test]
fn storage_manager_rejects_unique_index_backfill_with_duplicate_values()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let payload = bytea_tuple(b"duplicate");
    let row_keys = [table_row_key(id(16), id(71)), table_row_key(id(16), id(72))];
    let old_indexes = row_keys
        .iter()
        .map(|row_key| {
            let derived = typed_bytea_index_key(id(0x44), row_key, &payload);
            IndexWrite {
                key: derived.key,
                row_key: *row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }
        })
        .collect::<Vec<_>>();
    commit_rows_and_indexes(
        &database,
        id(78),
        id(9),
        9_999,
        &row_keys
            .iter()
            .map(|row_key| RowWrite {
                key: *row_key,
                expected_version: None,
                payload: Some(payload.clone()),
            })
            .collect::<Vec<_>>(),
        &old_indexes,
    )?;

    let (unique_key, unique_payload) =
        index_definition_with_options(id(0x89), id(16), &[id(0x66)], true);
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(79),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: unique_key.clone(),
                    expected_value: None,
                    payload: Some(unique_payload),
                },
                object_write(id(0x89), 7),
            ],
            &[audit_event(79)],
        ),
        Err(CommitError::UniqueViolation)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(79), id(9), 1)?.unwrap().status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(unique_key.as_slice())?
            .is_none()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[79; 16][..])?
            .is_none()
    );
    assert!(
        read.open_table(schema::INDEXES)?.iter()?.all(|entry| !entry
            .unwrap()
            .0
            .value()
            .starts_with(&id(0x89)))
    );
    Ok(())
}

#[test]
fn catalog_index_backfill_limit_fails_without_publishing_catalog()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let payload = bytea_tuple(b"bounded");
    let rows = (1u64..=65_537)
        .map(|row_id| {
            let mut row_suffix = [0; 16];
            row_suffix[8..].copy_from_slice(&row_id.to_be_bytes());
            RowWrite {
                key: table_row_key(id(16), row_suffix),
                expected_version: None,
                payload: Some(payload.clone()),
            }
        })
        .collect::<Vec<_>>();
    commit_rows(&database, id(80), id(9), 9_999, &rows)?;

    let (index_key, index_payload) = index_definition(id(0x8a), id(16));
    let ddl_fences = DdlFenceManager::new();
    let coordinator = DdlFenceCoordinator::new(ddl_fences, Duration::from_secs(1));
    assert_eq!(
        commit_catalog_and_audit_with_fences(
            &database,
            &coordinator,
            id(81),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: index_key.clone(),
                    expected_value: None,
                    payload: Some(index_payload),
                },
                object_write(id(0x8a), 7),
            ],
            &[audit_event(81)],
        ),
        Err(CommitError::IndexDdlLimitExceeded)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, id(81), id(9), 1)?.unwrap().status,
        OutcomeStatus::Aborted
    );
    assert!(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(index_key.as_slice())?
            .is_none()
    );
    Ok(())
}

#[test]
fn catalog_commit_rejects_removing_a_mapped_builtin_type() -> Result<(), Box<dyn std::error::Error>>
{
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

#[test]
fn catalog_commit_rejects_unassigned_builtin_type_profiles()
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

#[test]
fn catalog_commit_rejects_duplicate_builtin_type_kinds() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn catalog_commit_rejects_removing_the_bootstrap_descriptor()
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

#[test]
fn catalog_commit_rejects_renaming_the_mapped_admin_role() -> Result<(), Box<dyn std::error::Error>>
{
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

#[test]
fn catalog_commit_requires_typed_rows_for_every_object() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn catalog_commit_rejects_malformed_credential_verifier_profiles()
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

#[test]
fn token_admission_checks_expiry_secret_revocation_and_commit_guards()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::transaction::TransactionError;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(70);
    let active_secret = [0x31; 32];
    let expired_secret = [0x32; 32];
    let revoked_secret = [0x33; 32];
    let active = credential_verifier_write(
        id(71),
        user_id,
        &active_secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    let expired = credential_verifier_write(
        id(72),
        user_id,
        &expired_secret,
        now - 2_000_000,
        now - 1_000_000,
        None,
        1,
    );
    let revoked = credential_verifier_write(
        id(73),
        user_id,
        &revoked_secret,
        now - 2_000_000,
        now + 60_000_000,
        Some(now - 1_000_000),
        1,
    );
    let active_payload = active.payload.clone().unwrap();
    commit_catalog_and_audit(
        &database,
        id(74),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "credential_user"),
            object_write_with_name(user_id, 8, "credential_user"),
            active,
            expired,
            revoked,
        ],
        &[audit_event(74)],
    )
    .map_err(|error| format!("seed credential records: {error:?}"))?;

    let storage = StorageManager::new(&database, Duration::from_millis(10))
        .map_err(|error| format!("validate seeded credential records: {error:?}"))?;
    let (admitted, roles) = storage.begin_token_authenticated_invocation(
        id(75),
        id(71),
        &active_secret,
        now + 3_600_000_000,
        2,
        64,
    )?;
    assert!(roles.is_empty());

    for (invocation_id, token_id, secret) in [
        (id(76), id(71), [0xFF; 32]),
        (id(77), id(72), expired_secret),
        (id(78), id(73), revoked_secret),
    ] {
        assert!(matches!(
            storage.begin_token_authenticated_invocation(
                invocation_id,
                token_id,
                &secret,
                now + 3_600_000_000,
                2,
                64,
            ),
            Err(TransactionError::AuthenticationFailed)
        ));
        assert_eq!(
            lookup_outcome(&database, invocation_id, user_id, now)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted
        );
    }

    let mut revoked_active = credential_verifier_write(
        id(71),
        user_id,
        &active_secret,
        now - 1_000_000,
        now + 60_000_000,
        Some(now),
        2,
    );
    revoked_active.expected_value = Some(active_payload);
    storage.commit_catalog_and_audit(
        id(79),
        id(9),
        now + 3_600_000_000,
        &[revoked_active],
        &[audit_event(79)],
    )?;
    assert!(matches!(
        admitted.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert_eq!(
        lookup_outcome(&database, id(75), user_id, now)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(80),
            id(71),
            &active_secret,
            now + 3_600_000_000,
            2,
            64,
        ),
        Err(TransactionError::AuthenticationFailed)
    ));
    assert_eq!(
        lookup_outcome(&database, id(80), user_id, now)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert!(prdb_engine::startup::validate_persisted_records(&database).is_ok());
    Ok(())
}

#[test]
fn storage_manager_issues_and_revokes_tokens_for_manage_users()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{TokenIssueRequest, TokenLifecycleError};
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0x90);
    let user_manager = id(0x91);
    let target_user = id(0x92);
    let denied_user = id(0x93);
    let manage_users_role = id(0x9a);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let token_config = StorageManagerConfig {
        ddl_fence_timeout: Duration::from_millis(100),
        max_active_invocations: 128,
        max_invocation_age: Duration::from_secs(15 * 60),
        max_token_lifecycle_requests_per_minute: 5,
    };
    let storage = StorageManager::with_config(&database, token_config)?;
    let second_storage_handle = StorageManager::with_config(&database, token_config)?;
    let seed_records = [
        principal_write_with_name(admin_user, 1, 1, "token_admin"),
        object_write_with_name(admin_user, 8, "token_admin"),
        principal_write_with_name(user_manager, 1, 1, "token_manager"),
        object_write_with_name(user_manager, 8, "token_manager"),
        principal_write_with_name(manage_users_role, 2, 1, "token_managers"),
        object_write_with_name(manage_users_role, 9, "token_managers"),
        principal_write_with_name(target_user, 1, 1, "token_target"),
        object_write_with_name(target_user, 8, "token_target"),
        principal_write_with_name(denied_user, 1, 1, "token_denied"),
        object_write_with_name(denied_user, 8, "token_denied"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        membership_write_with_grantor(user_manager, manage_users_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        dependency_write(user_manager, manage_users_role, 5),
        credential_verifier_write(id(0x9b), admin_user, &[0x9b; 32], 1, i64::MAX, None, 1),
        grant_write(
            manage_users_role,
            database_id,
            12,
            1,
            database_id,
            admin_role,
        ),
        dependency_write(manage_users_role, database_id, 5),
    ];
    let mut seed_audit = identity_audit_event(0x90, admin_role, admin_user);
    seed_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x90), admin_role, 9_999, &seed_records, &[seed_audit])?;

    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let lifecycle_event = |event_id: u8, actor_id, target_ids| AuditEvent {
        event_id: id(event_id),
        actor_id: Some(actor_id),
        correlation_id: id(event_id.wrapping_add(1)),
        occurred_at_us: now,
        action: AuditAction::TokenLifecycle,
        result: AuditResult::Succeeded,
        target_ids,
    };
    let expires_at_us = now + 3_600_000_000;
    let mut issued = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0x94),
        authenticated_actor_id: user_manager,
        user_id: target_user,
        label: "integration client",
        expires_at_us,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: lifecycle_event(0x94, user_manager, vec![target_user]),
    })?;
    let token_id = issued.token_id;
    assert_eq!(issued.commit_sequence, 2);
    let secret = issued.take_secret().ok_or("issued secret missing")?;
    assert!(issued.take_secret().is_none());
    let own_inventory = storage.list_tokens(target_user, target_user)?;
    assert_eq!(own_inventory.len(), 1);
    assert_eq!(own_inventory[0].token_id, token_id);
    assert_eq!(own_inventory[0].label, "integration client");
    assert!(own_inventory[0].issued_at_us >= now);
    assert_eq!(own_inventory[0].expires_at_us, expires_at_us);
    assert!(!own_inventory[0].revoked);
    let managed_inventory = storage.list_tokens(user_manager, target_user)?;
    assert_eq!(managed_inventory, own_inventory);
    assert_eq!(
        storage.list_tokens(denied_user, target_user).unwrap_err(),
        TokenLifecycleError::TokenUnavailable
    );
    assert_eq!(
        storage.list_tokens(user_manager, id(0x94)).unwrap_err(),
        TokenLifecycleError::TokenUnavailable
    );

    let read = database.begin_read()?;
    let token_key = id_catalog_key(16, token_id);
    let token_bytes = read
        .open_table(CATALOG)?
        .get(token_key.as_slice())?
        .ok_or("issued verifier missing")?
        .value()
        .to_vec();
    let verifier_record = decode_registered_envelope(&token_bytes)?;
    let verifier = verifier_record
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("verifier field missing")?;
    assert_eq!(verifier.payload, hash_token_secret(&secret));
    assert_ne!(verifier.payload, secret);
    drop(read);

    let (admitted, roles) = storage.begin_token_authenticated_invocation(
        id(0x95),
        token_id,
        &secret,
        now + 7_200_000_000,
        2,
        64,
    )?;
    assert!(roles.is_empty());
    assert_eq!(
        storage.revoke_token(
            id(0x96),
            user_manager,
            token_id,
            now + 7_200_000_000,
            lifecycle_event(0x96, user_manager, vec![token_id]),
        )?,
        3
    );
    assert!(matches!(
        admitted.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0x97),
            token_id,
            &secret,
            now + 7_200_000_000,
            2,
            64,
        ),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));
    let revoked_inventory = storage.list_tokens(user_manager, target_user)?;
    assert_eq!(revoked_inventory.len(), 1);
    assert!(revoked_inventory[0].revoked);
    assert!(revoked_inventory[0].revoked_at_us.is_some());

    let denied_issue = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0x98),
        authenticated_actor_id: denied_user,
        user_id: target_user,
        label: "denied client",
        expires_at_us,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: lifecycle_event(0x98, denied_user, vec![target_user]),
    });
    assert!(matches!(
        denied_issue,
        Err(TokenLifecycleError::Commit(
            CommitError::DdlAuthorizationDenied
        ))
    ));
    assert_eq!(
        second_storage_handle
            .list_tokens(user_manager, target_user)
            .unwrap_err(),
        TokenLifecycleError::RateLimited
    );
    assert!(prdb_engine::startup::validate_persisted_records(&database).is_ok());
    Ok(())
}

#[test]
fn catalog_commit_requires_principal_object_identity_and_kind()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let principal = principal_write(id(70), 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(70),
            id(9),
            9_999,
            std::slice::from_ref(&principal),
            &[audit_event(70)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let object = object_write(id(70), 8);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(71),
            id(9),
            9_999,
            &[principal, object.clone()],
            &[audit_event(71)],
        )?,
        1
    );

    let wrong_kind = object_write(id(70), 9);
    let wrong_kind_update = CatalogWrite {
        key: wrong_kind.key,
        expected_value: object.payload.clone(),
        payload: wrong_kind.payload,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(72),
            id(9),
            9_999,
            &[wrong_kind_update],
            &[audit_event(72)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn catalog_commit_requires_matching_column_and_principal_names()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let principal_id = id(76);
    let mismatched_principal =
        principal_write_with_name(principal_id, 1, 1, "different_principal_name");
    let principal_object = principal_object_write(principal_id, 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(76),
            id(9),
            9_999,
            &[mismatched_principal, principal_object],
            &[audit_event(76)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let schema_id = id(77);
    let table_id = id(78);
    let column_id = id(79);
    let make_column_batch = |column_name: &str| {
        vec![
            schema_write(schema_id),
            table_write(table_id, schema_id),
            column_write(column_id, table_id, column_name),
            object_write(schema_id, 1),
            object_write(table_id, 3),
            object_write(column_id, 4),
        ]
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(77),
            id(9),
            9_999,
            &make_column_batch("different_column_name"),
            &[audit_event(77)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(78),
            id(9),
            9_999,
            &make_column_batch("object_79"),
            &[audit_event(78)],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn catalog_commit_rejects_duplicate_user_and_role_names() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user_id = id(80);
    let principal = principal_write_with_name(user_id, 1, 1, "database_admin");
    let object = object_write_with_name(user_id, 8, "database_admin");

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(80),
            id(9),
            9_999,
            &[principal, object],
            &[audit_event(80)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

#[test]
fn catalog_commit_checks_principal_name_uniqueness_in_the_final_view()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user_id = id(81);
    let mut user = principal_write_with_name(user_id, 1, 1, "alice");
    let mut user_object = object_write_with_name(user_id, 8, "alice");
    commit_catalog_and_audit(
        &database,
        id(81),
        id(9),
        9_999,
        &[user.clone(), user_object.clone()],
        &[audit_event(81)],
    )?;

    let mut renamed_user = principal_write_with_name(user_id, 1, 2, "bob");
    renamed_user.expected_value = user.payload.take();
    let mut renamed_object = object_write_with_name(user_id, 8, "bob");
    renamed_object.expected_value = user_object.payload.take();
    let role_id = id(82);
    let role = principal_write_with_login(role_id, 2, false, 1, "alice");
    let role_object = object_write_with_name(role_id, 9, "alice");
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(82),
            id(9),
            9_999,
            &[renamed_user, renamed_object, role, role_object],
            &[audit_event(82)],
        )?,
        2
    );
    assert_eq!(commit_sequence(&database)?, 2);
    Ok(())
}

#[test]
fn catalog_commit_requires_schema_object_identity_and_kind()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let schema = schema_write(id(73));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(73),
            id(9),
            9_999,
            std::slice::from_ref(&schema),
            &[audit_event(73)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let object = object_write(id(73), 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(74),
            id(9),
            9_999,
            &[schema.clone(), object.clone()],
            &[audit_event(74)],
        )?,
        1
    );

    let wrong_kind = object_write(id(73), 2);
    let wrong_kind_update = CatalogWrite {
        key: wrong_kind.key,
        expected_value: object.payload.clone(),
        payload: wrong_kind.payload,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(75),
            id(9),
            9_999,
            &[wrong_kind_update],
            &[audit_event(75)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

fn principal_write_with_epoch(principal_id: [u8; 16], kind: u16, epoch: u64) -> CatalogWrite {
    let mut key = 10u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&principal_id);
    let payload = encode_envelope(
        &family_schema(10).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: kind.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::Text,
                payload: principal_test_name(principal_id).into_bytes(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Bool,
                payload: vec![u8::from(kind == 1)],
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U64,
                payload: epoch.to_le_bytes().to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn principal_epoch_update(principal_id: [u8; 16], kind: u16, epoch: u64) -> CatalogWrite {
    let mut write = principal_write_with_epoch(principal_id, kind, epoch);
    write.expected_value = Some(principal_write(principal_id, kind).payload.unwrap());
    write
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

#[test]
fn policy_target_limit_checks_final_set_and_allows_replacements()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::catalog::{PolicyTarget, encode_policy_target};
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(77);
    let policy_id = id(78);
    let key = |family: u16, object_id: [u8; 16]| {
        let mut key = family.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&object_id);
        key
    };
    let field = |tag, wire_type, payload| CatalogField {
        tag,
        wire_type,
        payload,
    };
    let table_key = key(5, table_id);
    let table_payload = |epoch: u64| {
        encode_envelope(
            &family_schema(5).unwrap(),
            &table_key,
            &[
                field(1, WireType::Id, id(79).to_vec()),
                field(2, WireType::U64, 1u64.to_le_bytes().to_vec()),
                field(3, WireType::U64, epoch.to_le_bytes().to_vec()),
                field(4, WireType::Id, id(80).to_vec()),
                field(5, WireType::U16, 1u16.to_le_bytes().to_vec()),
            ],
        )
        .unwrap()
    };
    let policy_key = key(13, policy_id);
    let expression_id = id(81);
    let policy_payload = |epoch: u64| {
        encode_envelope(
            &family_schema(13).unwrap(),
            &policy_key,
            &[
                field(1, WireType::Id, table_id.to_vec()),
                field(2, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(3, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(4, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(5, WireType::Id, expression_id.to_vec()),
                field(7, WireType::U64, epoch.to_le_bytes().to_vec()),
            ],
        )
        .unwrap()
    };
    let target = |kind: u16, target_id: [u8; 16]| {
        let payload = encode_policy_target(&PolicyTarget {
            policy_id,
            kind,
            target_id,
        })
        .unwrap();
        CatalogWrite {
            key: payload[32..].to_vec(),
            expected_value: None,
            payload: Some(payload),
        }
    };
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(prdb_engine::schema::CATALOG)?;
            catalog.insert(table_key.as_slice(), table_payload(1).as_slice())?;
            catalog.insert(policy_key.as_slice(), policy_payload(1).as_slice())?;
            for object in [object_write(table_id, 3), object_write(policy_id, 10)] {
                catalog.insert(
                    object.key.as_slice(),
                    object.payload.as_ref().unwrap().as_slice(),
                )?;
            }
            let expression = expression_write(
                expression_id,
                3,
                &[],
                &prdb_engine::authorization::Expr::CurrentPrincipalId,
            );
            let expression_object = object_write(expression_id, 5);
            let expression_edge = dependency_write(policy_id, expression_id, 2);
            let recipient_edge = dependency_write(policy_id, builtin_owner_id(), 5);
            for record in [
                expression,
                expression_object,
                expression_edge,
                recipient_edge,
            ] {
                catalog.insert(
                    record.key.as_slice(),
                    record.payload.as_ref().unwrap().as_slice(),
                )?;
            }
            let recipient = target(2, builtin_owner_id());
            catalog.insert(
                recipient.key.as_slice(),
                recipient.payload.as_ref().unwrap().as_slice(),
            )?;
            for n in 1..=4095u128 {
                let principal_id = n.to_be_bytes();
                for record in [
                    principal_write(principal_id, 1),
                    principal_object_write(principal_id, 1),
                    dependency_write(policy_id, principal_id, 5),
                    target(1, principal_id),
                ] {
                    catalog.insert(
                        record.key.as_slice(),
                        record.payload.as_ref().unwrap().as_slice(),
                    )?;
                }
            }
        }
        write.commit()?;
    }
    let new_principal_id = 4096u128.to_be_bytes();
    let addition = target(1, new_principal_id);
    let new_principal = principal_write(new_principal_id, 1);
    let new_principal_object = principal_object_write(new_principal_id, 1);
    let new_principal_edge = dependency_write(policy_id, new_principal_id, 5);
    let old = target(1, 1u128.to_be_bytes());
    let updates = [
        CatalogWrite {
            key: policy_key.clone(),
            expected_value: Some(policy_payload(1)),
            payload: Some(policy_payload(2)),
        },
        CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_payload(1)),
            payload: Some(table_payload(2)),
        },
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(1),
            id(9),
            9_999,
            &[
                addition.clone(),
                new_principal.clone(),
                new_principal_object.clone(),
                new_principal_edge.clone(),
                updates[0].clone(),
                updates[1].clone(),
            ],
            &[audit_event(10)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    let removal = CatalogWrite {
        key: old.key.clone(),
        expected_value: old.payload,
        payload: None,
    };
    let old_edge = dependency_write(policy_id, 1u128.to_be_bytes(), 5);
    let edge_removal = CatalogWrite {
        key: old_edge.key,
        expected_value: old_edge.payload,
        payload: None,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(2),
            id(9),
            9_999,
            &[
                addition.clone(),
                new_principal,
                new_principal_object,
                new_principal_edge,
                removal,
                edge_removal,
                updates[0].clone(),
                updates[1].clone()
            ],
            &[audit_event(20)]
        )?,
        1
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(prdb_engine::schema::CATALOG)?;
    assert!(catalog.get(old.key.as_slice())?.is_none());
    assert!(catalog.get(addition.key.as_slice())?.is_some());
    Ok(())
}

#[test]
fn policy_column_targets_require_matching_cell_policy_columns()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::catalog::{PolicyTarget, encode_policy_target};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(77);
    let policy_id = id(78);
    let expression_id = id(81);
    let first_column_id = id(82);
    let second_column_id = id(84);
    let missing_column_id = id(85);
    let other_table_id = id(86);
    let other_column_id = id(87);
    let schema_id = id(79);
    let table_key = id_catalog_key(5, table_id);
    let table_payload = |epoch: u64| {
        encode_envelope(
            &family_schema(5).unwrap(),
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
                    payload: epoch.to_le_bytes().to_vec(),
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
            ],
        )
        .unwrap()
    };
    let policy_key = id_catalog_key(13, policy_id);
    let policy_payload = |kind: u16, epoch: u64| {
        encode_envelope(
            &family_schema(13).unwrap(),
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
                    payload: expression_id.to_vec(),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::U64,
                    payload: epoch.to_le_bytes().to_vec(),
                },
            ],
        )
        .unwrap()
    };
    let policy_target = |kind: u16, target_id: [u8; 16]| {
        let payload = encode_policy_target(&PolicyTarget {
            policy_id,
            kind,
            target_id,
        })
        .unwrap();
        CatalogWrite {
            key: payload[32..].to_vec(),
            expected_value: None,
            payload: Some(payload),
        }
    };
    let target_write = |kind: u16, target_id: [u8; 16]| policy_target(kind, target_id);
    let dependency =
        |dependent_id, referenced_id, kind| dependency_write(dependent_id, referenced_id, kind);
    let table_record = CatalogWrite {
        key: table_key.clone(),
        expected_value: None,
        payload: Some(table_payload(1)),
    };
    let policy_record = CatalogWrite {
        key: policy_key.clone(),
        expected_value: None,
        payload: Some(policy_payload(2, 1)),
    };
    let expression = expression_write(
        expression_id,
        3,
        &[],
        &prdb_engine::authorization::Expr::CurrentPrincipalId,
    );
    let first_column = column_write(first_column_id, table_id, "object_82");
    let second_column = column_write(second_column_id, table_id, "object_84");
    let other_table = table_write(other_table_id, schema_id);
    let other_column = column_write(other_column_id, other_table_id, "object_87");
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(prdb_engine::schema::CATALOG)?;
            for record in [
                table_record.clone(),
                policy_record.clone(),
                object_write(table_id, 3),
                object_write(other_table_id, 3),
                object_write(policy_id, 10),
                other_table.clone(),
                object_write(expression_id, 5),
                expression.clone(),
                dependency(policy_id, expression_id, 2),
                dependency(policy_id, builtin_owner_id(), 5),
                object_write(first_column_id, 4),
                first_column.clone(),
                object_write(second_column_id, 4),
                second_column.clone(),
                object_write(other_column_id, 4),
                other_column.clone(),
                target_write(2, builtin_owner_id()),
                target_write(3, first_column_id),
            ] {
                catalog.insert(
                    record.key.as_slice(),
                    record.payload.as_ref().unwrap().as_slice(),
                )?;
            }
        }
        write.commit()?;
    }

    let policy_update = |kind, epoch| CatalogWrite {
        key: policy_key.clone(),
        expected_value: Some(policy_payload(2, 1)),
        payload: Some(policy_payload(kind, epoch)),
    };
    let table_update = CatalogWrite {
        key: table_key.clone(),
        expected_value: Some(table_payload(1)),
        payload: Some(table_payload(2)),
    };
    let missing_target = target_write(3, missing_column_id);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(1),
            id(9),
            9_999,
            &[missing_target, policy_update(2, 2), table_update.clone(),],
            &[audit_event(10)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(2),
            id(9),
            9_999,
            &[policy_update(1, 2), table_update.clone(),],
            &[audit_event(20)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(3),
            id(9),
            9_999,
            &[
                target_write(3, other_column_id),
                policy_update(2, 2),
                table_update.clone(),
            ],
            &[audit_event(30)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(4),
            id(9),
            9_999,
            &[
                target_write(3, second_column_id),
                policy_update(2, 2),
                table_update,
            ],
            &[audit_event(40)],
        )?,
        1,
    );
    Ok(())
}

#[test]
fn membership_limit_applies_to_final_graph_independent_of_write_order()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let node = |n: u128| n.to_be_bytes();
    let member = node(1);
    let old_role = node(100);
    let new_role = node(164);
    {
        let transaction = database.begin_write()?;
        {
            let mut catalog = transaction.open_table(prdb_engine::schema::CATALOG)?;
            for n in (1..=64).chain(100..=164) {
                let principal = principal_write(node(n), 2);
                let object = principal_object_write(node(n), 2);
                catalog.insert(
                    principal.key.as_slice(),
                    principal.payload.as_ref().unwrap().as_slice(),
                )?;
                catalog.insert(
                    object.key.as_slice(),
                    object.payload.as_ref().unwrap().as_slice(),
                )?;
            }
            for member_number in 1..=64 {
                for role_number in 100..=163 {
                    let edge = membership_write(node(member_number), node(role_number));
                    catalog.insert(
                        edge.key.as_slice(),
                        edge.payload.as_ref().unwrap().as_slice(),
                    )?;
                    let authority = dependency_write(node(member_number), node(role_number), 5);
                    catalog.insert(
                        authority.key.as_slice(),
                        authority.payload.as_ref().unwrap().as_slice(),
                    )?;
                }
            }
        }
        transaction.commit()?;
    }
    let old = membership_write(member, old_role);
    let new = membership_write(member, new_role);
    let old_authority = dependency_write(member, old_role, 5);
    let new_authority = dependency_write(member, new_role, 5);
    let removal = CatalogWrite {
        key: old.key.clone(),
        expected_value: old.payload.clone(),
        payload: None,
    };
    let authority_removal = CatalogWrite {
        key: old_authority.key.clone(),
        expected_value: old_authority.payload.clone(),
        payload: None,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(1),
            id(9),
            9_999,
            &[
                new.clone(),
                removal.clone(),
                authority_removal.clone(),
                new_authority.clone(),
                principal_epoch_update(member, 2, 2)
            ],
            &[audit_event(10)]
        )?,
        1
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(prdb_engine::schema::CATALOG)?;
    assert!(catalog.get(old.key.as_slice())?.is_none());
    assert!(catalog.get(new.key.as_slice())?.is_some());
    assert_eq!(catalog.len()?, 293 + 8192);
    drop(catalog);
    drop(read);
    let mut advance = principal_write_with_epoch(member, 2, 3);
    advance.expected_value = principal_write_with_epoch(member, 2, 2).payload;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(2),
            id(9),
            9_999,
            &[old.clone(), advance.clone()],
            &[audit_event(20)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    let removal_new = CatalogWrite {
        key: new.key.clone(),
        expected_value: new.payload.clone(),
        payload: None,
    };
    let new_authority_removal = CatalogWrite {
        key: new_authority.key,
        expected_value: new_authority.payload,
        payload: None,
    };
    let old_authority_restore = CatalogWrite {
        key: old_authority.key,
        expected_value: None,
        payload: old_authority.payload,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(3),
            id(9),
            9_999,
            &[
                removal_new,
                old,
                new_authority_removal,
                old_authority_restore,
                advance
            ],
            &[audit_event(30)]
        )?,
        2
    );
    Ok(())
}

#[test]
fn membership_catalog_commits_reject_cycles_and_record_aborted_outcomes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(40),
            id(9),
            9_999,
            &[
                principal_write(id(1), 2),
                principal_write(id(2), 2),
                principal_object_write(id(1), 2),
                principal_object_write(id(2), 2),
                membership_write(id(1), id(2)),
                dependency_write(id(1), id(2), 5),
            ],
            &[audit_event(40)],
        )?,
        1
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(44),
            id(9),
            9_999,
            &[
                principal_write(id(4), 2),
                principal_object_write(id(4), 2),
                membership_write(id(1), id(4)),
            ],
            &[audit_event(44)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(42),
            id(9),
            9_999,
            &[
                principal_write(id(3), 1),
                principal_object_write(id(3), 1),
                membership_write(id(1), id(3)),
            ],
            &[audit_event(42)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(43),
            id(9),
            9_999,
            &[membership_write(id(4), id(2))],
            &[audit_event(43)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(41),
            id(9),
            9_999,
            &[membership_write(id(2), id(1))],
            &[audit_event(41)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::CATALOG)?
            .get(&membership_write(id(2), id(1)).key[..])?
            .is_none()
    );
    assert!(
        read.open_table(prdb_engine::schema::AUDIT)?
            .get(&[41; 16][..])?
            .is_none()
    );
    assert!(
        read.open_table(prdb_engine::schema::CATALOG)?
            .get(&principal_write(id(3), 1).key[..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(41)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(43)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(42)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn catalog_commit_requires_exact_membership_authority_edges()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let identities = [
        principal_write(id(21), 1),
        principal_write(id(22), 2),
        principal_object_write(id(21), 1),
        principal_object_write(id(22), 2),
    ];
    let membership = membership_write(id(21), id(22));
    let mut missing_edge = identities.to_vec();
    missing_edge.push(membership.clone());
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(70),
            id(9),
            9_999,
            &missing_edge,
            &[audit_event(70)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let authority = dependency_write(id(21), id(22), 5);
    let mut complete = identities.to_vec();
    complete.extend([membership, authority]);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(71),
            id(9),
            9_999,
            &complete,
            &[audit_event(71)],
        )?,
        1
    );
    let extra = dependency_write(id(22), id(21), 5);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(72),
            id(9),
            9_999,
            &[extra],
            &[audit_event(72)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

#[test]
fn database_scoped_authority_grants_use_the_bootstrap_database_id()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user_id = id(73);
    let identities = [
        principal_write(user_id, 1),
        principal_object_write(user_id, 1),
    ];
    let database_id = id(0x33);
    let grant = grant_write(user_id, database_id, 12, 1, database_id, builtin_owner_id());
    let mut missing_edge = identities.to_vec();
    missing_edge.push(grant.clone());
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(74),
            id(9),
            9_999,
            &missing_edge,
            &[audit_event(74)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let mut complete = identities.to_vec();
    complete.push(grant);
    complete.push(dependency_write(user_id, database_id, 5));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(75),
            id(9),
            9_999,
            &complete,
            &[audit_event(75)],
        )?,
        1
    );
    Ok(())
}

#[test]
fn concurrent_membership_commits_cannot_publish_a_cycle() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    commit_catalog_and_audit(
        &database,
        id(50),
        id(9),
        9_999,
        &[
            principal_write(id(1), 2),
            principal_write(id(2), 2),
            principal_object_write(id(1), 2),
            principal_object_write(id(2), 2),
        ],
        &[audit_event(50)],
    )?;
    let barrier = Arc::new(Barrier::new(3));
    let (left, right) = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_database = &database;
        let left = scope.spawn(move || {
            left_barrier.wait();
            commit_catalog_and_audit(
                left_database,
                id(51),
                id(9),
                9_999,
                &[
                    membership_write(id(1), id(2)),
                    dependency_write(id(1), id(2), 5),
                    principal_epoch_update(id(1), 2, 2),
                ],
                &[audit_event(51)],
            )
        });
        let right_barrier = barrier.clone();
        let right_database = &database;
        let right = scope.spawn(move || {
            right_barrier.wait();
            commit_catalog_and_audit(
                right_database,
                id(52),
                id(9),
                9_999,
                &[
                    membership_write(id(2), id(1)),
                    dependency_write(id(2), id(1), 5),
                    principal_epoch_update(id(2), 2, 2),
                ],
                &[audit_event(52)],
            )
        });
        barrier.wait();
        (left.join().unwrap(), right.join().unwrap())
    });
    let outcomes = [left, right];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| **result == Err(CommitError::InvalidCatalog))
            .count(),
        1
    );
    assert_eq!(commit_sequence(&database)?, 2);
    let read = database.begin_read()?;
    let catalog = read.open_table(prdb_engine::schema::CATALOG)?;
    assert_eq!(
        [
            catalog
                .get(&membership_write(id(1), id(2)).key[..])?
                .is_some(),
            catalog
                .get(&membership_write(id(2), id(1)).key[..])?
                .is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count(),
        1
    );
    let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
    let statuses = [id(51), id(52)].map(|invocation_id| {
        InvocationOutcome::decode(outcomes.get(&invocation_id[..]).unwrap().unwrap().value())
            .unwrap()
            .status
    });
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == OutcomeStatus::Committed)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == OutcomeStatus::Aborted)
            .count(),
        1
    );
    Ok(())
}

#[test]
fn catalog_audit_metadata_and_outcome_commit_together() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let (catalog_key, catalog_payload) = dependency_record([1; 16], [2; 16], 1);
    let write = CatalogWrite {
        key: catalog_key.clone(),
        expected_value: None,
        payload: Some(catalog_payload.clone()),
    };
    let catalog_writes = [
        object_write_with_parent([1; 16], 4, Some([2; 16])),
        object_write([2; 16], 3),
        column_write([1; 16], [2; 16], "object_1"),
        table_write([2; 16], builtin_id(1)),
        write.clone(),
    ];

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(1),
            id(9),
            9_999,
            &catalog_writes,
            &[audit_event(10)],
        )?,
        1
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(prdb_engine::schema::CATALOG)?;
        assert_eq!(
            catalog.get(&catalog_key[..])?.unwrap().value(),
            catalog_payload
        );
        let audit = read.open_table(prdb_engine::schema::AUDIT)?;
        let event_bytes = audit.get(&[10; 16][..])?.unwrap().value().to_vec();
        assert_eq!(AuditEvent::decode(&event_bytes)?, audit_event(10));
        let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
        assert_eq!(
            InvocationOutcome::decode(outcomes.get(&id(1)[..])?.unwrap().value())?.status,
            OutcomeStatus::Committed
        );
    }

    assert_eq!(
        commit_catalog_and_audit(&database, id(2), id(9), 9_999, &[write], &[audit_event(20)],),
        Err(CommitError::Conflict)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::AUDIT)?
            .get(&[20; 16][..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(2)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn one_redb_commit_can_publish_rows_indexes_catalog_audit_and_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = key(7);
    let index_key = index_key(&row_key);
    let (catalog_key, catalog_payload) = dependency_record([6; 16], [7; 16], 1);

    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(4),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: row_key,
                    expected_version: None,
                    payload: Some(b"row".to_vec()),
                }],
                indexes: &[IndexWrite {
                    key: index_key.clone(),
                    row_key,
                    expected_version: None,
                    present: true,
                    unique: false,
                    null_exempt: false,
                }],
                foreign_keys: &[],
                catalog_guards: &[],
                catalog: &[
                    object_write_with_parent([6; 16], 4, Some([7; 16])),
                    object_write([7; 16], 3),
                    column_write([6; 16], [7; 16], "object_6"),
                    table_write([7; 16], builtin_id(1)),
                    CatalogWrite {
                        key: catalog_key.clone(),
                        expected_value: None,
                        payload: Some(catalog_payload.clone()),
                    },
                ],
                audit: &[audit_event(30)],
            },
        )?,
        1
    );

    let read = database.begin_read()?;
    assert_eq!(row(&database, &row_key)?.unwrap().payload, b"row");
    let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
    let locator = indexes.get(&index_key[..])?.unwrap().value().to_vec();
    assert_eq!(
        IndexLocator::decode_for_key(&index_key, &locator)?.row_version,
        1
    );
    assert!(
        read.open_table(prdb_engine::schema::CATALOG)?
            .get(&catalog_key[..])?
            .is_some()
    );
    assert!(
        read.open_table(prdb_engine::schema::AUDIT)?
            .get(&[30; 16][..])?
            .is_some()
    );
    assert_eq!(commit_sequence(&database)?, 1);
    let conflict = commit_storage_changes(
        &database,
        CommitRequest {
            invocation_id: id(5),
            principal_id: id(9),
            retention_deadline_us: 9_999,
            rows: &[RowWrite {
                key: row_key,
                expected_version: Some(1),
                payload: Some(b"must-rollback".to_vec()),
            }],
            indexes: &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: Some(1),
                present: true,
                unique: false,
                null_exempt: false,
            }],
            foreign_keys: &[],
            catalog_guards: &[],
            catalog: &[CatalogWrite {
                key: catalog_key.clone(),
                expected_value: None,
                payload: Some(catalog_payload),
            }],
            audit: &[audit_event(31)],
        },
    );
    assert_eq!(conflict, Err(CommitError::Conflict));
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(row(&database, &row_key)?.unwrap().payload, b"row");
    let verify = database.begin_read()?;
    let index_value = verify
        .open_table(prdb_engine::schema::INDEXES)?
        .get(&index_key[..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        IndexLocator::decode_for_key(&index_key, &index_value)?.row_version,
        1
    );
    assert!(
        verify
            .open_table(prdb_engine::schema::CATALOG)?
            .get(&catalog_key[..])?
            .is_some()
    );
    assert!(
        verify
            .open_table(prdb_engine::schema::AUDIT)?
            .get(&[31; 16][..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            verify
                .open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(5)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn row_updates_refresh_retained_index_locator_versions() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let mut row_key = [0x10; 32];
    row_key[16..].fill(0x31);
    let index_key = index_key(&row_key);
    commit_rows_and_indexes(
        &database,
        id(40),
        id(9),
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(bytea_tuple(b"first")),
        }],
        &[IndexWrite {
            key: index_key.clone(),
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: false,
        }],
    )?;

    assert_eq!(
        commit_rows(
            &database,
            id(41),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(1),
                payload: Some(bytea_tuple(b"second")),
            }],
        )?,
        2
    );
    let read = database.begin_read()?;
    let locator = read
        .open_table(prdb_engine::schema::INDEXES)?
        .get(&index_key[..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        IndexLocator::decode_for_key(&index_key, &locator)?.row_version,
        2
    );
    drop(read);

    assert_eq!(
        commit_rows(
            &database,
            id(42),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(2),
                payload: None,
            }],
        )?,
        3
    );
    let mut reverse_key = row_key.to_vec();
    reverse_key.extend_from_slice(&index_key[..index_key.len() - 16]);
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::INDEXES)?
            .get(&index_key[..])?
            .is_none()
    );
    assert!(
        read.open_table(prdb_engine::schema::ROW_INDEXES)?
            .get(&reverse_key[..])?
            .is_none()
    );
    assert!(row(&database, &row_key)?.is_none());
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn row_and_index_locators_publish_and_remove_atomically() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = key(7);
    let index_key = index_key(&row_key);

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(1),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(b"v1".to_vec())
            }],
            &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }],
        )?,
        1
    );
    {
        let read = database.begin_read()?;
        let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
        let locator = indexes.get(&index_key[..])?.unwrap().value().to_vec();
        assert_eq!(
            IndexLocator::decode_for_key(&index_key, &locator)?.row_version,
            1
        );
    }

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(2),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(1),
                payload: Some(b"v2".to_vec())
            }],
            &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: Some(1),
                present: true,
                unique: false,
                null_exempt: false,
            }],
        )?,
        2
    );
    {
        let read = database.begin_read()?;
        let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
        let locator = indexes.get(&index_key[..])?.unwrap().value().to_vec();
        assert_eq!(
            IndexLocator::decode_for_key(&index_key, &locator)?.row_version,
            2
        );
    }

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(8),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(2),
                payload: Some(b"must-rollback".to_vec())
            }],
            &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: Some(1),
                present: true,
                unique: false,
                null_exempt: false,
            }],
        ),
        Err(CommitError::Conflict)
    );
    assert_eq!(commit_sequence(&database)?, 2);
    assert_eq!(row(&database, &row_key)?.unwrap().payload, b"v2");

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(3),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(2),
                payload: None
            }],
            &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: Some(2),
                present: false,
                unique: false,
                null_exempt: false,
            }],
        )?,
        3
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::INDEXES)?
            .get(&index_key[..])?
            .is_none()
    );
    assert!(row(&database, &row_key)?.is_none());
    Ok(())
}

#[test]
fn serialized_unique_index_check_rejects_duplicates_and_exempts_null_tuples()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let first_row = key(31);
    let first_index = index_key(&first_row);
    commit_rows_and_indexes(
        &database,
        id(31),
        id(9),
        9_999,
        &[RowWrite {
            key: first_row,
            expected_version: None,
            payload: Some(b"first".to_vec()),
        }],
        &[IndexWrite {
            key: first_index,
            row_key: first_row,
            expected_version: None,
            present: true,
            unique: true,
            null_exempt: false,
        }],
    )?;

    let second_row = key(32);
    let second_index = index_key(&second_row);
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(32),
            id(9),
            9_999,
            &[RowWrite {
                key: second_row,
                expected_version: None,
                payload: Some(b"duplicate".to_vec()),
            }],
            &[IndexWrite {
                key: second_index.clone(),
                row_key: second_row,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: false,
            }],
        ),
        Err(CommitError::UniqueViolation)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert!(row(&database, &second_row)?.is_none());

    let null_index = prdb_engine::index_key::compose_key(
        [0x44; 16],
        &[0xff],
        second_row[16..].try_into().unwrap(),
    );
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(33),
            id(9),
            9_999,
            &[RowWrite {
                key: second_row,
                expected_version: None,
                payload: Some(b"null-key".to_vec()),
            }],
            &[IndexWrite {
                key: null_index,
                row_key: second_row,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: true,
            }],
        )?,
        2
    );
    assert_eq!(row(&database, &second_row)?.unwrap().payload, b"null-key");

    let third_row = key(33);
    let third_null_index = prdb_engine::index_key::compose_key(
        [0x44; 16],
        &[0xff],
        third_row[16..].try_into().unwrap(),
    );
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(34),
            id(9),
            9_999,
            &[RowWrite {
                key: third_row,
                expected_version: None,
                payload: Some(b"another-null-key".to_vec()),
            }],
            &[IndexWrite {
                key: third_null_index,
                row_key: third_row,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: true,
            }],
        )?,
        3
    );
    assert_eq!(
        row(&database, &third_row)?.unwrap().payload,
        b"another-null-key"
    );
    Ok(())
}

#[test]
fn foreign_key_commit_checks_child_and_parent_changes_against_final_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x55; 16];
    let target_table_id = [0x66; 16];
    let source_key = table_row_key(source_table_id, [1; 16]);
    let null_source_key = table_row_key(source_table_id, [3; 16]);
    let target_key = table_row_key(target_table_id, [2; 16]);
    let column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    };
    let foreign_key = ForeignKeyCheck {
        source_table_id,
        source_schema_version: 1,
        source_schema: vec![column.clone()],
        source_ordinals: vec![0],
        target_table_id,
        target_schema_version: 1,
        target_schema: vec![column.clone()],
        target_ordinals: vec![0],
    };
    let parent = encode_tuple(
        std::slice::from_ref(&column),
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
    )?;
    commit_rows(
        &database,
        id(101),
        id(9),
        9_999,
        &[RowWrite {
            key: target_key,
            expected_version: None,
            payload: Some(parent.clone()),
        }],
    )?;

    let missing_parent = encode_tuple(
        std::slice::from_ref(&column),
        1,
        &[Some(ScalarValue::Bytea(b"missing".to_vec()))],
    )?;
    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(102),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: source_key,
                    expected_version: None,
                    payload: Some(missing_parent),
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        ),
        Err(CommitError::ForeignKeyViolation)
    );
    assert!(row(&database, &source_key)?.is_none());
    assert_eq!(commit_sequence(&database)?, 1);

    let null_child = encode_tuple(std::slice::from_ref(&column), 1, &[None])?;
    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(109),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: null_source_key,
                    expected_version: None,
                    payload: Some(null_child),
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        )?,
        2
    );

    let child = encode_tuple(
        std::slice::from_ref(&column),
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
    )?;
    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(103),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: source_key,
                    expected_version: None,
                    payload: Some(child),
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        )?,
        3
    );

    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(104),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: target_key,
                    expected_version: Some(1),
                    payload: None,
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        ),
        Err(CommitError::ForeignKeyViolation)
    );
    assert!(row(&database, &target_key)?.is_some());

    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(105),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[
                    RowWrite {
                        key: source_key,
                        expected_version: Some(3),
                        payload: None,
                    },
                    RowWrite {
                        key: null_source_key,
                        expected_version: Some(2),
                        payload: None,
                    },
                    RowWrite {
                        key: target_key,
                        expected_version: Some(1),
                        payload: None,
                    },
                ],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        )?,
        4
    );
    assert!(row(&database, &source_key)?.is_none());
    assert!(row(&database, &null_source_key)?.is_none());
    assert!(row(&database, &target_key)?.is_none());
    Ok(())
}

#[test]
fn foreign_key_row_visit_limit_aborts_the_whole_commit() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x55; 16];
    let target_table_id = [0x66; 16];
    let column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    };
    let parent = bytea_tuple(b"parent");
    let mut initial_rows = Vec::with_capacity(256);
    let mut updated_child_key = [0; 32];
    for row_number in 0..128u8 {
        let parent_key = table_row_key(target_table_id, [row_number; 16]);
        let child_key = table_row_key(source_table_id, [row_number; 16]);
        if row_number == 0 {
            updated_child_key = child_key;
        }
        initial_rows.push(RowWrite {
            key: parent_key,
            expected_version: None,
            payload: Some(parent.clone()),
        });
        initial_rows.push(RowWrite {
            key: child_key,
            expected_version: None,
            payload: Some(parent.clone()),
        });
    }
    commit_rows(&database, id(110), id(9), 9_999, &initial_rows)?;

    let foreign_key = ForeignKeyCheck {
        source_table_id,
        source_schema_version: 1,
        source_schema: vec![column.clone()],
        source_ordinals: vec![0],
        target_table_id,
        target_schema_version: 1,
        target_schema: vec![column],
        target_ordinals: vec![0],
    };
    let foreign_keys = vec![foreign_key; 256];
    let result = commit_storage_changes(
        &database,
        CommitRequest {
            invocation_id: id(111),
            principal_id: id(9),
            retention_deadline_us: 9_999,
            rows: &[RowWrite {
                key: updated_child_key,
                expected_version: Some(1),
                payload: Some(parent),
            }],
            indexes: &[],
            foreign_keys: &foreign_keys,
            catalog_guards: &[],
            catalog: &[],
            audit: &[],
        },
    );
    assert_eq!(result, Err(CommitError::ForeignKeyValidationLimitExceeded));
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(row(&database, &updated_child_key)?.unwrap().row_version, 1);
    let read = database.begin_read()?;
    let encoded_outcome = read
        .open_table(prdb_engine::schema::OUTCOMES)?
        .get(&id(111)[..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        InvocationOutcome::decode(&encoded_outcome)?.status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn foreign_key_comparison_key_budget_aborts_large_keys_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x55; 16];
    let target_table_id = [0x66; 16];
    let column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: false,
    };
    let mut initial_rows = Vec::with_capacity(256);
    let mut updated_child_key = [0; 32];
    let mut updated_child_payload = Vec::new();
    for row_number in 0..128u8 {
        let mut value = vec![row_number.wrapping_add(1); 70 * 1024];
        value[..8].copy_from_slice(&(row_number as u64).to_be_bytes());
        let payload = encode_tuple(
            std::slice::from_ref(&column),
            1,
            &[Some(ScalarValue::Bytea(value))],
        )?;
        let parent_key = table_row_key(target_table_id, [row_number; 16]);
        let child_key = table_row_key(source_table_id, [row_number; 16]);
        if row_number == 0 {
            updated_child_key = child_key;
            updated_child_payload = payload.clone();
        }
        initial_rows.push(RowWrite {
            key: parent_key,
            expected_version: None,
            payload: Some(payload.clone()),
        });
        initial_rows.push(RowWrite {
            key: child_key,
            expected_version: None,
            payload: Some(payload),
        });
    }
    commit_rows(&database, id(112), id(9), 9_999, &initial_rows)?;

    let foreign_key = ForeignKeyCheck {
        source_table_id,
        source_schema_version: 1,
        source_schema: vec![column.clone()],
        source_ordinals: vec![0],
        target_table_id,
        target_schema_version: 1,
        target_schema: vec![column],
        target_ordinals: vec![0],
    };
    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(113),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: updated_child_key,
                    expected_version: Some(1),
                    payload: Some(updated_child_payload),
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        ),
        Err(CommitError::ForeignKeyValidationLimitExceeded)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(row(&database, &updated_child_key)?.unwrap().row_version, 1);
    Ok(())
}

#[test]
fn concurrent_foreign_key_insert_and_parent_delete_cannot_both_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x55; 16];
    let target_table_id = [0x66; 16];
    let source_key = table_row_key(source_table_id, [1; 16]);
    let target_key = table_row_key(target_table_id, [2; 16]);
    let column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    };
    let foreign_key = ForeignKeyCheck {
        source_table_id,
        source_schema_version: 1,
        source_schema: vec![column.clone()],
        source_ordinals: vec![0],
        target_table_id,
        target_schema_version: 1,
        target_schema: vec![column.clone()],
        target_ordinals: vec![0],
    };
    let value = encode_tuple(
        std::slice::from_ref(&column),
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
    )?;
    commit_rows(
        &database,
        id(106),
        id(9),
        9_999,
        &[RowWrite {
            key: target_key,
            expected_version: None,
            payload: Some(value.clone()),
        }],
    )?;
    let source_write = RowWrite {
        key: source_key,
        expected_version: None,
        payload: Some(value),
    };
    let target_write = RowWrite {
        key: target_key,
        expected_version: Some(1),
        payload: None,
    };
    let barrier = Arc::new(Barrier::new(3));
    let (child_result, parent_result) = std::thread::scope(|scope| {
        let child_barrier = barrier.clone();
        let child_fk = foreign_key.clone();
        let child_database = &database;
        let child = scope.spawn(move || {
            child_barrier.wait();
            commit_storage_changes(
                child_database,
                CommitRequest {
                    invocation_id: id(107),
                    principal_id: id(9),
                    retention_deadline_us: 9_999,
                    rows: std::slice::from_ref(&source_write),
                    indexes: &[],
                    foreign_keys: std::slice::from_ref(&child_fk),
                    catalog_guards: &[],
                    catalog: &[],
                    audit: &[],
                },
            )
        });
        let parent_barrier = barrier.clone();
        let parent_database = &database;
        let parent = scope.spawn(move || {
            parent_barrier.wait();
            commit_storage_changes(
                parent_database,
                CommitRequest {
                    invocation_id: id(108),
                    principal_id: id(9),
                    retention_deadline_us: 9_999,
                    rows: std::slice::from_ref(&target_write),
                    indexes: &[],
                    foreign_keys: std::slice::from_ref(&foreign_key),
                    catalog_guards: &[],
                    catalog: &[],
                    audit: &[],
                },
            )
        });
        barrier.wait();
        (child.join().unwrap(), parent.join().unwrap())
    });
    assert_ne!(child_result.is_ok(), parent_result.is_ok());
    assert!(matches!(
        child_result.as_ref().err().or(parent_result.as_ref().err()),
        Some(CommitError::ForeignKeyViolation)
    ));
    assert_eq!(commit_sequence(&database)?, 2);
    let read = database.begin_read()?;
    let child_exists = read.open_table(ROWS)?.get(&source_key[..])?.is_some();
    let parent_exists = read.open_table(ROWS)?.get(&target_key[..])?.is_some();
    assert_eq!(child_exists, parent_exists);
    Ok(())
}

#[test]
fn multirow_commit_assigns_one_sequence_and_conflict_aborts_the_whole_set()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;

    assert_eq!(
        commit_rows(
            &database,
            id(1),
            id(9),
            9_999,
            &[
                RowWrite {
                    key: key(1),
                    expected_version: None,
                    payload: Some(b"a1".to_vec()),
                },
                RowWrite {
                    key: key(2),
                    expected_version: None,
                    payload: Some(b"b1".to_vec()),
                },
            ],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);

    assert_eq!(
        commit_rows(
            &database,
            id(2),
            id(9),
            9_999,
            &[
                RowWrite {
                    key: key(1),
                    expected_version: Some(1),
                    payload: Some(b"a2".to_vec()),
                },
                RowWrite {
                    key: key(2),
                    expected_version: Some(1),
                    payload: Some(b"b2".to_vec()),
                },
            ],
        )?,
        2
    );
    assert_eq!(row(&database, &key(1))?.unwrap().row_version, 2);
    assert_eq!(row(&database, &key(2))?.unwrap().row_version, 2);

    let conflict = commit_rows(
        &database,
        id(3),
        id(9),
        9_999,
        &[
            RowWrite {
                key: key(1),
                expected_version: Some(1),
                payload: Some(b"stale".to_vec()),
            },
            RowWrite {
                key: key(2),
                expected_version: Some(2),
                payload: Some(b"must-rollback".to_vec()),
            },
        ],
    );
    assert_eq!(conflict, Err(CommitError::Conflict));
    assert_eq!(commit_sequence(&database)?, 2);
    assert_eq!(row(&database, &key(1))?.unwrap().payload, b"a2");
    assert_eq!(row(&database, &key(2))?.unwrap().payload, b"b2");
    {
        let read = database.begin_read()?;
        let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
        let outcome = outcomes.get(&id(2)[..])?.unwrap().value().to_vec();
        assert_eq!(
            InvocationOutcome::decode(&outcome)?,
            InvocationOutcome {
                status: OutcomeStatus::Committed,
                invocation_id: id(2),
                principal_id: id(9),
                commit_sequence: 2,
                retention_deadline_us: 9_999,
            }
        );
        let aborted = outcomes.get(&id(3)[..])?.unwrap().value().to_vec();
        assert_eq!(
            InvocationOutcome::decode(&aborted)?,
            InvocationOutcome {
                status: OutcomeStatus::Aborted,
                invocation_id: id(3),
                principal_id: id(9),
                commit_sequence: 0,
                retention_deadline_us: 9_999,
            }
        );
    }
    drop(database);
    let reopened = Database::open(fixture.path())?;
    assert_eq!(
        lookup_outcome(&reopened, id(2), id(9), 9_998)?,
        Some(InvocationOutcome {
            status: OutcomeStatus::Committed,
            invocation_id: id(2),
            principal_id: id(9),
            commit_sequence: 2,
            retention_deadline_us: 9_999,
        })
    );
    assert_eq!(lookup_outcome(&reopened, id(2), id(10), 9_998)?, None);
    assert_eq!(lookup_outcome(&reopened, id(2), id(9), 9_999)?, None);
    assert_eq!(
        lookup_outcome(&reopened, id(3), id(9), 9_998)?,
        Some(InvocationOutcome {
            status: OutcomeStatus::Aborted,
            invocation_id: id(3),
            principal_id: id(9),
            commit_sequence: 0,
            retention_deadline_us: 9_999,
        })
    );
    Ok(())
}
