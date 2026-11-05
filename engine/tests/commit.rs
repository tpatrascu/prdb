use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::commit::{
    CatalogWrite, CommitError, CommitRequest, IndexWrite, RowWrite, commit_catalog_and_audit,
    commit_rows, commit_rows_and_indexes, commit_storage_changes,
};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus, lookup_outcome};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ENGINE_META, ROWS};
use redb::{Database, ReadableDatabase, ReadableTableMetadata};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

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

fn id(n: u8) -> [u8; 16] {
    [n; 16]
}

fn id_catalog_key(family: u16, object_id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

fn builtin_owner_id() -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = 3;
    id
}

fn index_definition(index_id: [u8; 16], table_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let mut key = 9u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&index_id);
    let mut columns = 1u32.to_le_bytes().to_vec();
    columns.extend_from_slice(&[0x66; 16]);
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
    let payload = encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

fn bootstrap_database_record(database_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let mut key = 1u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&database_id);
    let mut builtins = 30u32.to_le_bytes().to_vec();
    for code in [1u16, 2, 3]
        .into_iter()
        .chain(101u16..=111)
        .chain(201u16..=216)
    {
        builtins.extend_from_slice(&code.to_le_bytes());
        builtins.extend_from_slice(&0u16.to_le_bytes());
        let mut id = [0x80; 16];
        id[15] = code as u8;
        builtins.extend_from_slice(&id);
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
    let owner_id = builtin_owner_id();
    let owner_principal = principal_write(owner_id, 2);
    let owner_object = principal_object_write(owner_id, 2);
    transaction.open_table(CATALOG)?.insert(
        &owner_principal.key[..],
        owner_principal.payload.as_ref().unwrap().as_slice(),
    )?;
    transaction.open_table(CATALOG)?.insert(
        &owner_object.key[..],
        owner_object.payload.as_ref().unwrap().as_slice(),
    )?;
    let metadata = EngineMetadata {
        database_id: [0x33; 16],
        catalog_version: 1,
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

fn membership_write(member_id: [u8; 16], role_id: [u8; 16]) -> CatalogWrite {
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
                payload: id(9).to_vec(),
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
            payload: format!("object_{}", object_id[0]).into_bytes(),
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
                    payload: vec![u8::from(kind == 1)],
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

fn table_write(table_id: [u8; 16], schema_id: [u8; 16]) -> CatalogWrite {
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
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

fn column_write(column_id: [u8; 16], table_id: [u8; 16], name: &str) -> CatalogWrite {
    let mut key = 6u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&column_id);
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
                payload: id(50).to_vec(),
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
fn catalog_commit_checks_dependency_endpoints_in_the_final_catalog()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_object = object_write(id(101), 3);
    let column_object = object_write_with_parent(id(102), 4, Some(id(101)));
    let edge = dependency_write(id(102), id(101), 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(51),
            id(9),
            9_999,
            &[edge.clone(), table_object.clone(), column_object],
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
    let child = object_write_with_parent(child_id, 4, Some(parent_id));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(60),
            id(9),
            9_999,
            &[parent.clone(), child.clone()],
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
            &[parent.clone(), child.clone(), edge.clone()],
            &[audit_event(61)],
        )?,
        1
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
    let reparented_child = object_write_with_parent(child_id, 4, Some(next_parent_id));
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
                CatalogWrite {
                    key: edge.key,
                    expected_value: edge.payload,
                    payload: None,
                },
                next_parent,
                next_edge,
            ],
            &[audit_event(65)],
        )?,
        2
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
        column_write(target_column_id, target_table_id, "object_83"),
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
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(67),
            id(9),
            9_999,
            &[object.clone(), principal.clone(), principal_object.clone()],
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
    let (missing_key, missing_payload) = index_definition(id(55), id(10));
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

    let (index_key, index_payload) = index_definition(id(56), id(10));
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

    let (wrong_key, wrong_payload) = index_definition(id(57), id(10));
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
                payload: format!("object_{}", principal_id[0]).into_bytes(),
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
    let policy_payload = |epoch: u64| {
        encode_envelope(
            &family_schema(13).unwrap(),
            &policy_key,
            &[
                field(1, WireType::Id, table_id.to_vec()),
                field(2, WireType::U16, 2u16.to_le_bytes().to_vec()),
                field(3, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(4, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(5, WireType::Id, id(81).to_vec()),
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
            let recipient = target(1, id(9));
            catalog.insert(
                recipient.key.as_slice(),
                recipient.payload.as_ref().unwrap().as_slice(),
            )?;
            for n in 1..=4095u128 {
                let column = target(3, n.to_be_bytes());
                catalog.insert(
                    column.key.as_slice(),
                    column.payload.as_ref().unwrap().as_slice(),
                )?;
            }
        }
        write.commit()?;
    }
    let addition = target(3, 4096u128.to_be_bytes());
    let old = target(3, 1u128.to_be_bytes());
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
            &[addition.clone(), updates[0].clone(), updates[1].clone()],
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
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(2),
            id(9),
            9_999,
            &[
                addition.clone(),
                removal,
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
                }
            }
        }
        transaction.commit()?;
    }
    let old = membership_write(member, old_role);
    let new = membership_write(member, new_role);
    let removal = CatalogWrite {
        key: old.key.clone(),
        expected_value: old.payload.clone(),
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
    assert_eq!(catalog.len()?, 263 + 4096);
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
        key: new.key,
        expected_value: new.payload,
        payload: None,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(3),
            id(9),
            9_999,
            &[removal_new, old, advance],
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
    let (catalog_key, catalog_payload) = dependency_record([1; 16], [2; 16], 2);
    let write = CatalogWrite {
        key: catalog_key.clone(),
        expected_value: None,
        payload: Some(catalog_payload.clone()),
    };
    let catalog_writes = [
        object_write([1; 16], 4),
        object_write([2; 16], 3),
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
    let (catalog_key, catalog_payload) = dependency_record([6; 16], [7; 16], 2);

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
                catalog_guards: &[],
                catalog: &[
                    object_write([6; 16], 4),
                    object_write([7; 16], 3),
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
            payload: Some(b"first".to_vec()),
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
                payload: Some(b"second".to_vec()),
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
