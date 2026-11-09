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
use prdb_engine::schema::{self, AUDIT, CATALOG, ENGINE_META, ROWS};
use prdb_engine::storage_manager::{
    AdministrativeCancellationError, StorageManager, StorageManagerConfig, StorageManagerInitError,
    TokenIssueRequest,
};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageBackend};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

static NEXT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct FailNextSyncBackend {
    inner: redb::backends::FileBackend,
    fail_next_sync: Arc<AtomicBool>,
}

impl StorageBackend for FailNextSyncBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        if self.fail_next_sync.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected commit sync failure"));
        }
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.inner.write(offset, data)
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.inner.close()
    }
}

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

fn requested_scope_fixture() -> Vec<u8> {
    let mut bytes = b"PSC1\0\x01\x02\x02".to_vec();
    bytes.extend([1; 16]);
    bytes
}

fn admin_dispatch_json(
    storage: &StorageManager,
    staging: &prdb_engine::supervisor::StagingStore,
    session: &mut prdb_engine::client_protocol::ClientConnectionSession,
    request_id: u64,
    payload: &str,
    retention_period_us: i64,
) -> Result<String, Box<dyn std::error::Error>> {
    let dispatch = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        storage,
        staging,
        session,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id,
            payload: payload.as_bytes().to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us,
            max_rows: 1,
            max_bytes: 1,
        },
    );
    match dispatch.map_err(|error| format!("ADMIN request {payload:?} failed: {error:?}"))? {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            Ok(String::from_utf8(response.payload)?)
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            Err("ADMIN DDL unexpectedly started a worker invocation".into())
        }
    }
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

#[test]
fn storage_manager_creates_schema_atomically_for_privileged_actor()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, SchemaLifecycleError, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let setup_admin = id(0xe1);
    let denied = id(0xe2);
    let actor = id(0xe8);
    let successor_owner = id(0xea);
    let successor_role = id(0xf7);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(setup_admin, 1, 1, "schema_admin"),
        object_write_with_name(setup_admin, 8, "schema_admin"),
        membership_write(setup_admin, admin_role),
        dependency_write(setup_admin, admin_role, 5),
        principal_write_with_name(denied, 1, 1, "schema_denied"),
        object_write_with_name(denied, 8, "schema_denied"),
        principal_write_with_name(actor, 1, 1, "schema_creator"),
        object_write_with_name(actor, 8, "schema_creator"),
        principal_write_with_name(successor_owner, 1, 1, "schema_successor"),
        object_write_with_name(successor_owner, 8, "schema_successor"),
        principal_write_with_name(successor_role, 2, 1, "schema_successor_role"),
        object_write_with_name(successor_role, 9, "schema_successor_role"),
        membership_write(successor_owner, successor_role),
        dependency_write(successor_owner, successor_role, 5),
        grant_write(actor, id(0x33), 14, 1, id(0x33), admin_role),
        dependency_write(actor, id(0x33), 5),
    ];
    let mut setup_audit = identity_audit_event(0xe3, admin_role, setup_admin);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xe3), admin_role, 9_999, &setup, &[setup_audit])?;

    let denied_id = id(0xe4);
    let mut denied_audit = identity_audit_event(0xe4, denied, id(0xe5));
    denied_audit.correlation_id = denied_id;
    let denied_result = storage.create_schema(
        denied_id,
        denied,
        "private_schema",
        denied,
        9_999,
        denied_audit,
    );
    assert!(
        matches!(denied_result, Err(SchemaLifecycleError::Unauthorized)),
        "{denied_result:?}"
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, denied_id, denied, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let invocation_id = id(0xe6);
    let mut audit = identity_audit_event(0xe6, actor, id(0xe7));
    audit.correlation_id = invocation_id;
    let created = storage.create_schema(invocation_id, actor, "app_data", actor, 9_999, audit)?;
    assert_eq!(created.commit_sequence, 2);
    let schema_key = id_catalog_key(3, created.schema_id);
    let object_key = id_catalog_key(2, created.schema_id);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(schema_key.as_slice())?.is_some());
    let object = catalog
        .get(object_key.as_slice())?
        .ok_or("missing schema object")?;
    let record = decode_registered_envelope(object.value())?;
    assert_eq!(record.family_code, 2);
    assert_eq!(
        record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .unwrap()
            .payload,
        b"app_data"
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    let transfer_invocation = id(0xc1);
    let mut transfer_audit = identity_audit_event(0xc2, actor, created.schema_id);
    transfer_audit.correlation_id = transfer_invocation;
    let transfer_result = storage.set_schema_owner(
        transfer_invocation,
        actor,
        created.schema_id,
        successor_role,
        9_999,
        transfer_audit,
    );
    assert!(
        transfer_result.is_ok(),
        "schema ownership transfer failed: {transfer_result:?}"
    );
    assert_eq!(transfer_result?, 3);
    {
        let read = database.begin_read()?;
        let object = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(object_key.as_slice())?
                .ok_or("schema object disappeared after ownership transfer")?
                .value(),
        )?;
        assert_eq!(
            object
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .unwrap()
                .payload,
            successor_role
        );
        assert_eq!(
            u64::from_le_bytes(
                object
                    .fields
                    .iter()
                    .find(|field| field.tag == 5)
                    .unwrap()
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            2
        );
    }
    let owned_table_invocation = id(0xc5);
    let mut owned_table_audit = identity_audit_event(0xc6, successor_owner, id(0xc7));
    owned_table_audit.correlation_id = owned_table_invocation;
    let owned_table = storage.create_table(CreateTableRequest {
        invocation_id: owned_table_invocation,
        authenticated_actor_id: successor_owner,
        schema_id: created.schema_id,
        name: "successor_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: owned_table_audit,
    })?;
    let child_grant_prefix = {
        let mut prefix = 12u16.to_be_bytes().to_vec();
        prefix.push(1);
        prefix.extend_from_slice(&successor_role);
        prefix
    };
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        assert!(
            catalog
                .range(child_grant_prefix.as_slice()..)?
                .next()
                .transpose()?
                .is_none_or(|(key, _)| !key.value().starts_with(&child_grant_prefix))
        );
    }
    let owned_table_drop_invocation = id(0xc8);
    let mut owned_table_drop_audit = identity_audit_event(0xc9, successor_owner, id(0xca));
    owned_table_drop_audit.correlation_id = owned_table_drop_invocation;
    storage.drop_table(
        owned_table_drop_invocation,
        successor_owner,
        owned_table.table_id,
        9_999,
        owned_table_drop_audit,
    )?;
    let old_owner_drop = id(0xc3);
    let mut old_owner_drop_audit = identity_audit_event(0xc4, actor, created.schema_id);
    old_owner_drop_audit.correlation_id = old_owner_drop;
    assert!(matches!(
        storage.drop_schema(
            old_owner_drop,
            actor,
            created.schema_id,
            9_999,
            old_owner_drop_audit,
        ),
        Err(SchemaLifecycleError::Unauthorized)
    ));
    let denied_drop_invocation = id(0xf8);
    let mut denied_drop_audit = identity_audit_event(0xf9, denied, id(0xfa));
    denied_drop_audit.correlation_id = denied_drop_invocation;
    assert!(matches!(
        storage.drop_schema(
            denied_drop_invocation,
            denied,
            created.schema_id,
            9_999,
            denied_drop_audit,
        ),
        Err(SchemaLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(&database, denied_drop_invocation, denied, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let duplicate_id = id(0xe9);
    let mut duplicate_audit = identity_audit_event(0xea, actor, id(0xeb));
    duplicate_audit.correlation_id = duplicate_id;
    assert!(matches!(
        storage.create_schema(
            duplicate_id,
            actor,
            "app_data",
            actor,
            9_999,
            duplicate_audit
        ),
        Err(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))
    ));
    assert_eq!(
        lookup_outcome(&database, duplicate_id, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let rename_target_invocation = id(0xcb);
    let mut rename_target_audit = identity_audit_event(0xcc, actor, id(0xcd));
    rename_target_audit.correlation_id = rename_target_invocation;
    let rename_target = storage.create_schema(
        rename_target_invocation,
        actor,
        "rename_target",
        actor,
        9_999,
        rename_target_audit,
    )?;
    let duplicate_rename_invocation = id(0xce);
    let mut duplicate_rename_audit = identity_audit_event(0xcf, successor_owner, created.schema_id);
    duplicate_rename_audit.correlation_id = duplicate_rename_invocation;
    assert!(matches!(
        storage.rename_schema(
            duplicate_rename_invocation,
            successor_owner,
            created.schema_id,
            "rename_target",
            9_999,
            duplicate_rename_audit,
        ),
        Err(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))
    ));
    assert_eq!(
        lookup_outcome(&database, duplicate_rename_invocation, successor_owner, 1)?
            .ok_or("missing duplicate schema-name outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let rename_invocation = id(0xd0);
    let mut rename_audit = identity_audit_event(0xd1, successor_owner, created.schema_id);
    rename_audit.correlation_id = rename_invocation;
    let renamed_schema = storage.rename_schema(
        rename_invocation,
        successor_owner,
        created.schema_id,
        "app_archive",
        9_999,
        rename_audit,
    );
    assert!(
        renamed_schema.is_ok(),
        "schema rename failed: {renamed_schema:?}"
    );
    assert_eq!(renamed_schema?, 7);
    let renamed_object_bytes = {
        let read = database.begin_read()?;
        read.open_table(CATALOG)?
            .get(object_key.as_slice())?
            .ok_or("renamed schema object missing")?
            .value()
            .to_vec()
    };
    let renamed_object = decode_registered_envelope(&renamed_object_bytes)?;
    assert_eq!(
        renamed_object
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .unwrap()
            .payload,
        b"app_archive"
    );
    assert_eq!(
        u64::from_le_bytes(
            renamed_object
                .fields
                .iter()
                .find(|field| field.tag == 5)
                .unwrap()
                .payload
                .as_slice()
                .try_into()?
        ),
        3
    );
    let old_owner_rename_invocation = id(0xd2);
    let mut old_owner_rename_audit = identity_audit_event(0xd3, actor, created.schema_id);
    old_owner_rename_audit.correlation_id = old_owner_rename_invocation;
    assert!(matches!(
        storage.rename_schema(
            old_owner_rename_invocation,
            actor,
            created.schema_id,
            "old_owner_name",
            9_999,
            old_owner_rename_audit,
        ),
        Err(SchemaLifecycleError::Unauthorized)
    ));
    let drop_invocation = id(0xec);
    let mut drop_audit = identity_audit_event(0xed, successor_owner, id(0xee));
    drop_audit.correlation_id = drop_invocation;
    let dropped = storage.drop_schema(
        drop_invocation,
        successor_owner,
        created.schema_id,
        9_999,
        drop_audit,
    )?;
    assert_eq!(dropped.schema_id, created.schema_id);
    assert_eq!(dropped.commit_sequence, 8);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(schema_key.as_slice())?.is_none());
    assert!(catalog.get(object_key.as_slice())?.is_none());
    assert_eq!(
        lookup_outcome(&database, drop_invocation, successor_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    let drop_target_invocation = id(0xd4);
    let mut drop_target_audit = identity_audit_event(0xd5, actor, rename_target.schema_id);
    drop_target_audit.correlation_id = drop_target_invocation;
    storage.drop_schema(
        drop_target_invocation,
        actor,
        rename_target.schema_id,
        9_999,
        drop_target_audit,
    )?;

    let populated_schema_invocation = id(0xef);
    let mut populated_schema_audit = identity_audit_event(0xf0, actor, id(0xf1));
    populated_schema_audit.correlation_id = populated_schema_invocation;
    let populated_schema = storage.create_schema(
        populated_schema_invocation,
        actor,
        "populated_schema",
        setup_admin,
        9_999,
        populated_schema_audit,
    )?;
    let create_table_invocation = id(0xf2);
    let mut create_table_audit = identity_audit_event(0xf3, setup_admin, id(0xf4));
    create_table_audit.correlation_id = create_table_invocation;
    storage.create_table(CreateTableRequest {
        invocation_id: create_table_invocation,
        authenticated_actor_id: setup_admin,
        schema_id: populated_schema.schema_id,
        name: "present_child".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "label".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_table_audit,
    })?;
    let rejected_drop_invocation = id(0xf5);
    let mut rejected_drop_audit = identity_audit_event(0xf6, setup_admin, id(0xf7));
    rejected_drop_audit.correlation_id = rejected_drop_invocation;
    assert!(matches!(
        storage.drop_schema(
            rejected_drop_invocation,
            setup_admin,
            populated_schema.schema_id,
            9_999,
            rejected_drop_audit,
        ),
        Err(SchemaLifecycleError::Commit(CommitError::SchemaNotEmpty))
    ));
    assert_eq!(
        lookup_outcome(&database, rejected_drop_invocation, setup_admin, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(id_catalog_key(3, populated_schema.schema_id).as_slice())?
            .is_some()
    );
    Ok(())
}

#[test]
fn schema_drop_racing_table_creation_never_leaves_an_orphan()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, SchemaLifecycleError, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xa1);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "schema_race_admin"),
        object_write_with_name(actor, 8, "schema_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0xa2, admin_role, id(0xa2));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xa2), admin_role, 9_999, &setup, &[setup_audit])?;
    let mut create_schema_audit = identity_audit_event(0xa3, actor, id(0xa3));
    create_schema_audit.correlation_id = id(0xa3);
    let schema = storage.create_schema(
        id(0xa3),
        actor,
        "schema_race",
        actor,
        9_999,
        create_schema_audit,
    )?;

    let barrier = Arc::new(Barrier::new(3));
    let (table_result, drop_result) = std::thread::scope(|scope| {
        let create_barrier = barrier.clone();
        let create_storage = &storage;
        let create = scope.spawn(move || {
            create_barrier.wait();
            let invocation_id = id(0xa4);
            let mut audit = identity_audit_event(0xa5, actor, id(0xa6));
            audit.correlation_id = invocation_id;
            create_storage.create_table(CreateTableRequest {
                invocation_id,
                authenticated_actor_id: actor,
                schema_id: schema.schema_id,
                name: "racing_child".to_owned(),
                columns: vec![TableColumnDefinition {
                    name: "value".to_owned(),
                    scalar_type: ScalarType::Text,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                }],
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        let drop_barrier = barrier.clone();
        let drop_storage = &storage;
        let drop_schema = scope.spawn(move || {
            drop_barrier.wait();
            let invocation_id = id(0xa7);
            let mut audit = identity_audit_event(0xa8, actor, id(0xa9));
            audit.correlation_id = invocation_id;
            drop_storage.drop_schema(invocation_id, actor, schema.schema_id, 9_999, audit)
        });
        barrier.wait();
        (create.join().unwrap(), drop_schema.join().unwrap())
    });
    match (table_result, drop_result) {
        (Ok(_), Err(SchemaLifecycleError::Commit(CommitError::SchemaNotEmpty))) => {}
        (
            Err(
                TableLifecycleError::Unauthorized
                | TableLifecycleError::Commit(CommitError::Conflict),
            ),
            Ok(_),
        ) => {}
        (table, dropped) => {
            panic!("invalid schema-drop/table-create race: {table:?} / {dropped:?}")
        }
    }
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_creates_typed_table_for_schema_create_grant()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefault, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xd1);
    let denied = id(0xd2);
    let schema_id = id(0xd3);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "table_creator"),
        object_write_with_name(actor, 8, "table_creator"),
        principal_write_with_name(denied, 1, 1, "table_denied"),
        object_write_with_name(denied, 8, "table_denied"),
        object_write_with_parent_and_owner_and_name(schema_id, 1, None, admin_role, "table_schema"),
        schema_write(schema_id),
        grant_write(actor, schema_id, 7, 1, schema_id, admin_role),
        dependency_write(actor, schema_id, 5),
    ];
    let mut setup_audit = identity_audit_event(0xd4, admin_role, id(0xd4));
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(id(0xd4), admin_role, 9_999, &setup, &[setup_audit])
        .map_err(|error| format!("setup failed: {error:?}"))?;

    let denied_invocation = id(0xd5);
    let mut denied_audit = identity_audit_event(0xd5, denied, id(0xd6));
    denied_audit.correlation_id = denied_invocation;
    let denied_result = storage.create_table(CreateTableRequest {
        invocation_id: denied_invocation,
        authenticated_actor_id: denied,
        schema_id,
        name: "ledger".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Uuid,
            nullable: false,
            generated_uuid_identity: true,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: denied_audit,
    });
    assert!(matches!(
        denied_result,
        Err(TableLifecycleError::Unauthorized)
    ));

    let reserved_invocation = id(0xdb);
    let mut reserved_audit = identity_audit_event(0xdb, actor, id(0xdc));
    reserved_audit.correlation_id = reserved_invocation;
    let reserved_result = storage.create_table(CreateTableRequest {
        invocation_id: reserved_invocation,
        authenticated_actor_id: actor,
        schema_id: builtin_id(1),
        name: "forbidden_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Uuid,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: reserved_audit,
    });
    assert!(matches!(
        reserved_result,
        Err(TableLifecycleError::Unauthorized)
    ));

    let invocation_id = id(0xd7);
    let mut audit = identity_audit_event(0xd7, actor, id(0xd8));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_table(CreateTableRequest {
            invocation_id,
            authenticated_actor_id: actor,
            schema_id,
            name: "ledger".to_owned(),
            columns: vec![
                TableColumnDefinition {
                    name: "id".to_owned(),
                    scalar_type: ScalarType::Uuid,
                    nullable: false,
                    generated_uuid_identity: true,
                    default: None,
                },
                TableColumnDefinition {
                    name: "label".to_owned(),
                    scalar_type: ScalarType::Text,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: Some(TableColumnDefault::Literal(Some(ScalarValue::Text(
                        "untitled".to_owned(),
                    )))),
                },
                TableColumnDefinition {
                    name: "created_by".to_owned(),
                    scalar_type: ScalarType::Uuid,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: Some(TableColumnDefault::CurrentPrincipalId),
                },
            ],
            retention_deadline_us: 9_999,
            audit_event: audit,
        })
        .map_err(|error| format!("table creation failed: {error:?}"))?;
    assert_eq!(created.column_ids.len(), 3);
    assert_eq!(created.commit_sequence, 2);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let table_record = catalog
        .get(id_catalog_key(5, created.table_id).as_slice())?
        .ok_or("missing table record")?;
    let table = decode_registered_envelope(table_record.value())?;
    assert_eq!(table.family_code, 5);
    for (ordinal, column_id) in created.column_ids.iter().enumerate() {
        let column_record = catalog
            .get(id_catalog_key(6, *column_id).as_slice())?
            .ok_or("missing column record")?;
        let column = decode_registered_envelope(column_record.value())?;
        assert_eq!(column.family_code, 6);
        if ordinal > 0 {
            let expression_id: [u8; 16] = column
                .fields
                .iter()
                .find(|field| field.tag == 7)
                .ok_or("missing column default reference")?
                .payload
                .as_slice()
                .try_into()?;
            let expression = catalog
                .get(id_catalog_key(7, expression_id).as_slice())?
                .ok_or("missing default expression")?;
            let expression = decode_registered_envelope(expression.value())?;
            assert_eq!(expression.family_code, 7);
        }
        assert_eq!(
            u32::from_le_bytes(
                column
                    .fields
                    .iter()
                    .find(|field| field.tag == 2)
                    .unwrap()
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            ordinal as u32
        );
    }
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );

    let duplicate_invocation = id(0xd9);
    let mut duplicate_audit = identity_audit_event(0xd9, actor, id(0xda));
    duplicate_audit.correlation_id = duplicate_invocation;
    let duplicate = storage.create_table(CreateTableRequest {
        invocation_id: duplicate_invocation,
        authenticated_actor_id: actor,
        schema_id,
        name: "ledger".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Uuid,
            nullable: false,
            generated_uuid_identity: true,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: duplicate_audit,
    });
    assert!(matches!(
        duplicate,
        Err(TableLifecycleError::Commit(CommitError::InvalidCatalog))
    ));
    assert_eq!(
        lookup_outcome(&database, duplicate_invocation, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn storage_manager_drop_table_removes_owned_catalog_and_rows()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::Expr;
    use prdb_engine::catalog::{PolicyTarget, encode_policy_target};
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefault, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0x40);
    let grantee = id(0x47);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "table_drop_owner"),
        object_write_with_name(actor, 8, "table_drop_owner"),
        principal_write_with_name(grantee, 1, 1, "table_drop_grantee"),
        object_write_with_name(grantee, 8, "table_drop_grantee"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x40, admin_role, id(0x4f));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x4f), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x41);
    let mut schema_audit = identity_audit_event(0x42, actor, id(0x43));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "drop_table_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x44);
    let mut create_audit = identity_audit_event(0x45, actor, id(0x46));
    create_audit.correlation_id = create_invocation;
    let created = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "drop_me".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "label".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: Some(TableColumnDefault::Literal(Some(ScalarValue::Text(
                "default".to_owned(),
            )))),
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let table_grant = grant_write(grantee, created.table_id, 2, 1, created.table_id, actor);
    let grant_edge = dependency_write(grantee, created.table_id, 5);
    let mut advanced_grantee = principal_write_with_name(grantee, 1, 2, "table_drop_grantee");
    advanced_grantee.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(10, grantee).as_slice())?
            .ok_or("table-drop grant recipient missing")?
            .value()
            .to_vec(),
    );
    let grant_invocation = id(0x5a);
    let mut grant_audit = identity_audit_event(0x5b, actor, created.table_id);
    grant_audit.action = AuditAction::AuthorityChange;
    grant_audit.correlation_id = grant_invocation;
    storage.commit_authorized_object_grant_ddl(
        grant_invocation,
        actor,
        9_999,
        &[table_grant.clone(), grant_edge.clone(), advanced_grantee],
        &[grant_audit],
    )?;
    let policy_id = id(0x5c);
    let expression_id_for_policy = id(0x5d);
    let policy_key = id_catalog_key(13, policy_id);
    let policy_payload = encode_envelope(
        &family_schema(13).unwrap(),
        &policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: created.table_id.to_vec(),
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
                payload: expression_id_for_policy.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let policy_object = object_write_with_parent_and_owner_and_name(
        policy_id,
        10,
        None,
        actor,
        "drop_table_policy",
    );
    let policy_expression = expression_write(
        expression_id_for_policy,
        3,
        &[],
        &Expr::Literal(Some(ScalarValue::Boolean(true))),
    );
    let expression_object = object_write_with_parent_and_owner_and_name(
        expression_id_for_policy,
        5,
        None,
        actor,
        "drop_table_policy_expression",
    );
    let policy_target_payload = encode_policy_target(&PolicyTarget {
        policy_id,
        kind: 2,
        target_id: admin_role,
    })?;
    let policy_target = CatalogWrite {
        key: policy_target_payload[32..].to_vec(),
        expected_value: None,
        payload: Some(policy_target_payload),
    };
    let policy_expression_edge = dependency_write(policy_id, expression_id_for_policy, 2);
    let policy_target_edge = dependency_write(policy_id, admin_role, 5);
    let mut table_epoch_update =
        table_write_with_versions(created.table_id, schema.schema_id, 1, 2);
    table_epoch_update.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, created.table_id).as_slice())?
            .ok_or("table missing before policy setup")?
            .value()
            .to_vec(),
    );
    let policy_invocation = id(0x5e);
    let mut policy_audit = identity_audit_event(0x5f, actor, policy_id);
    policy_audit.correlation_id = policy_invocation;
    policy_audit.target_ids = vec![policy_id];
    storage.commit_authorized_policy_ddl(
        policy_invocation,
        actor,
        9_999,
        &[
            CatalogWrite {
                key: policy_key.clone(),
                expected_value: None,
                payload: Some(policy_payload),
            },
            policy_object,
            policy_expression,
            expression_object,
            policy_target.clone(),
            policy_expression_edge.clone(),
            policy_target_edge.clone(),
            table_epoch_update,
        ],
        &[policy_audit],
    )?;
    let check_constraint_id = id(0x60);
    let check_expression_id = id(0x61);
    let check_constraint_key = id_catalog_key(8, check_constraint_id);
    let check_constraint_payload = encode_envelope(
        &family_schema(8).unwrap(),
        &check_constraint_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: created.table_id.to_vec(),
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
    let check_constraint_object = object_write_with_parent_and_owner_and_name(
        check_constraint_id,
        6,
        None,
        actor,
        "drop_table_check",
    );
    let check_expression_key = id_catalog_key(7, check_expression_id);
    let check_expression = CatalogWrite {
        key: check_expression_key.clone(),
        expected_value: None,
        payload: Some(encode_envelope(
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
                    payload: prdb_engine::expression::encode_expression(&Expr::Literal(Some(
                        ScalarValue::Boolean(true),
                    )))?,
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Ids,
                    payload: 0u32.to_le_bytes().to_vec(),
                },
            ],
        )?),
    };
    let check_expression_object = object_write_with_parent_and_owner_and_name(
        check_expression_id,
        5,
        None,
        actor,
        "drop_table_check_expression",
    );
    let check_expression_edge = dependency_write(check_constraint_id, check_expression_id, 2);
    let mut table_constraint_epoch =
        table_write_with_versions(created.table_id, schema.schema_id, 1, 3);
    table_constraint_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, created.table_id).as_slice())?
            .ok_or("table missing before CHECK setup")?
            .value()
            .to_vec(),
    );
    let constraint_invocation = id(0x62);
    let mut constraint_audit = identity_audit_event(0x63, actor, check_constraint_id);
    constraint_audit.correlation_id = constraint_invocation;
    constraint_audit.target_ids = vec![check_constraint_id];
    storage.commit_authorized_constraint_ddl(
        constraint_invocation,
        actor,
        9_999,
        &[
            CatalogWrite {
                key: check_constraint_key.clone(),
                expected_value: None,
                payload: Some(check_constraint_payload),
            },
            check_constraint_object,
            check_expression,
            check_expression_object,
            check_expression_edge.clone(),
            table_constraint_epoch,
        ],
        &[constraint_audit],
    )?;
    let payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Text("stored".to_owned()))],
    )?;
    let row_writes = (1u64..=65_537)
        .map(|row_number| {
            let mut row_id = [0; 16];
            row_id[8..].copy_from_slice(&row_number.to_be_bytes());
            RowWrite {
                key: table_row_key(created.table_id, row_id),
                expected_version: None,
                payload: Some(payload.clone()),
            }
        })
        .collect::<Vec<_>>();
    let row_key = row_writes[0].key;
    let read = database.begin_read()?;
    let column_record = read
        .open_table(CATALOG)?
        .get(id_catalog_key(6, created.column_ids[0]).as_slice())?
        .ok_or("created column missing")?;
    let column = decode_registered_envelope(column_record.value())?;
    let expression_id: [u8; 16] = column
        .fields
        .iter()
        .find(|field| field.tag == 7)
        .ok_or("default expression missing")?
        .payload
        .as_slice()
        .try_into()?;
    drop(read);
    let before_drop_limit = commit_rows(&database, id(0x54), actor, 9_999, &row_writes[..65_536])?;
    let over_limit_sequence = commit_rows(
        &database,
        id(0x55),
        actor,
        9_999,
        &[row_writes[65_536].clone()],
    )?;

    let denied_invocation = id(0x48);
    let mut denied_audit = identity_audit_event(0x4b, id(0x4c), id(0x4e));
    denied_audit.correlation_id = denied_invocation;
    let denied = storage.drop_table(
        denied_invocation,
        id(0x4c),
        created.table_id,
        9_999,
        denied_audit,
    );
    assert!(matches!(denied, Err(TableLifecycleError::Unauthorized)));

    let over_limit_drop_invocation = id(0x56);
    let mut over_limit_drop_audit = identity_audit_event(0x57, actor, created.table_id);
    over_limit_drop_audit.correlation_id = over_limit_drop_invocation;
    assert_eq!(
        storage.drop_table(
            over_limit_drop_invocation,
            actor,
            created.table_id,
            9_999,
            over_limit_drop_audit,
        ),
        Err(TableLifecycleError::Commit(
            CommitError::TableDropLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, over_limit_sequence);
    assert_eq!(
        lookup_outcome(&database, over_limit_drop_invocation, actor, 1)?
            .ok_or("missing table-drop row-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let last_row_key = row_writes[65_536].key;
    assert_eq!(
        commit_rows(
            &database,
            id(0x58),
            actor,
            9_999,
            &[RowWrite {
                key: last_row_key,
                expected_version: Some(over_limit_sequence),
                payload: None,
            }],
        )?,
        before_drop_limit + 2
    );

    let stale_writer_invocation = id(0x52);
    let stale_row_id = id(0x53);
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(created.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_payload = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Text(
            "staged before table drop".to_owned(),
        ))],
    )?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: created.table_id,
            row: stale_row_id,
        },
        &stale_payload,
    )?;

    let drop_invocation = id(0x50);
    let mut drop_audit = identity_audit_event(0x49, actor, id(0x4a));
    drop_audit.correlation_id = drop_invocation;
    let dropped = storage
        .drop_table(drop_invocation, actor, created.table_id, 9_999, drop_audit)
        .map_err(|error| format!("table drop failed: {error:?}"))?;
    assert_eq!(dropped.table_id, created.table_id);
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(created.table_id, stale_row_id))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale table-drop writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(2, created.table_id),
        id_catalog_key(5, created.table_id),
        id_catalog_key(2, created.column_ids[0]),
        id_catalog_key(6, created.column_ids[0]),
        id_catalog_key(2, expression_id),
        id_catalog_key(7, expression_id),
        table_grant.key,
        grant_edge.key,
        policy_key,
        id_catalog_key(2, policy_id),
        id_catalog_key(7, expression_id_for_policy),
        policy_target.key,
        policy_expression_edge.key,
        policy_target_edge.key,
        check_constraint_key,
        id_catalog_key(2, check_constraint_id),
        id_catalog_key(7, check_expression_id),
        id_catalog_key(2, check_expression_id),
        check_expression_edge.key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "record remained: {key:?}"
        );
    }
    let grantee_key = id_catalog_key(10, grantee);
    let grantee_record = decode_registered_envelope(
        catalog
            .get(grantee_key.as_slice())?
            .ok_or("table-drop grant recipient missing after cleanup")?
            .value(),
    )?;
    let grantee_epoch = grantee_record
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("table-drop grant recipient epoch missing")?;
    assert_eq!(
        u64::from_le_bytes(grantee_epoch.payload.as_slice().try_into()?),
        3
    );
    assert!(read.open_table(ROWS)?.get(&row_key[..])?.is_none());
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_drop_table_removes_owned_primary_and_unique_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x6e);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x6f, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x6f),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "unique_drop_owner"),
            object_write_with_name(actor, 8, "unique_drop_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x70);
    let mut schema_audit = identity_audit_event(0x71, actor, id(0x72));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "unique_drop_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x73);
    let mut create_audit = identity_audit_event(0x74, actor, id(0x75));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "unique_drop_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;

    let not_null_invocation = id(0x80);
    let mut not_null_audit = identity_audit_event(0x81, actor, table.column_ids[0]);
    not_null_audit.correlation_id = not_null_invocation;
    storage.set_column_nullable(
        not_null_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        false,
        9_999,
        not_null_audit,
    )?;
    let not_null_constraint_id = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let prefix = 8u16.to_be_bytes();
        let mut constraint_id = None;
        for entry in catalog.range(prefix.as_slice()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            let record = decode_registered_envelope(value.value())?;
            if record
                .fields
                .iter()
                .any(|field| field.tag == 1 && field.payload.as_slice() == table.table_id)
                && record
                    .fields
                    .iter()
                    .any(|field| field.tag == 2 && field.payload.as_slice() == 1u16.to_le_bytes())
                && record.fields.iter().any(|field| {
                    field.tag == 3
                        && field.payload[4..]
                            .chunks_exact(16)
                            .any(|id| id == table.column_ids[0])
                })
            {
                constraint_id = Some(<[u8; 16]>::try_from(&key.value()[3..19])?);
                break;
            }
        }
        constraint_id.ok_or("missing generated NOT NULL constraint")?
    };

    let unique_constraint_id = id(0x76);
    let unique_index_id = id(0x77);
    let primary_constraint_id = id(0x7e);
    let primary_index_id = id(0x7f);
    let unique_constraint = constraint_write(
        unique_constraint_id,
        table.table_id,
        3,
        &table.column_ids,
        None,
        &[],
        Some(unique_index_id),
    );
    let primary_constraint = constraint_write(
        primary_constraint_id,
        table.table_id,
        2,
        &table.column_ids,
        None,
        &[],
        Some(primary_index_id),
    );
    let (unique_index_key, unique_index_payload) = owned_unique_index_definition(
        unique_index_id,
        table.table_id,
        &table.column_ids,
        unique_constraint_id,
    );
    let (primary_index_key, primary_index_payload) = owned_unique_index_definition(
        primary_index_id,
        table.table_id,
        &table.column_ids,
        primary_constraint_id,
    );
    let stale_constraint_invocation = id(0x82);
    let stale_constraint_row = id(0x83);
    let mut stale_constraint_writer =
        storage.begin_invocation(stale_constraint_invocation, actor, 9_999, 1, 1024)?;
    let stale_constraint_schema =
        stale_constraint_writer.load_catalog_table_schema(table.table_id)?;
    stale_constraint_writer.load_catalog_indexes(&stale_constraint_schema)?;
    let stale_constraint_tuple = encode_tuple(
        &stale_constraint_schema.columns,
        stale_constraint_schema.schema_version,
        &[Some(ScalarValue::Integer(51))],
    )?;
    stale_constraint_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: stale_constraint_row,
        },
        &stale_constraint_tuple,
    )?;
    let table_key = id_catalog_key(5, table.table_id);
    let current_table = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(table_key.as_slice())?
        .ok_or("unique table record missing before constraint add")?
        .value()
        .to_vec();
    let mut table_epoch = table_write_with_versions(table.table_id, schema.schema_id, 1, 3);
    table_epoch.expected_value = Some(current_table);
    let constraint_invocation = id(0x78);
    let mut constraint_audit = identity_audit_event(0x79, actor, unique_constraint_id);
    constraint_audit.correlation_id = constraint_invocation;
    storage.commit_authorized_constraint_ddl(
        constraint_invocation,
        actor,
        9_999,
        &[
            unique_constraint,
            object_write(unique_constraint_id, 6),
            CatalogWrite {
                key: unique_index_key.clone(),
                expected_value: None,
                payload: Some(unique_index_payload),
            },
            object_write(unique_index_id, 7),
            dependency_write(unique_constraint_id, unique_index_id, 4),
            primary_constraint,
            object_write(primary_constraint_id, 6),
            CatalogWrite {
                key: primary_index_key.clone(),
                expected_value: None,
                payload: Some(primary_index_payload),
            },
            object_write(primary_index_id, 7),
            dependency_write(primary_constraint_id, primary_index_id, 4),
            table_epoch,
        ],
        &[constraint_audit],
    )?;
    assert!(matches!(
        stale_constraint_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(
        row(
            &database,
            &table_row_key(table.table_id, stale_constraint_row)
        )?
        .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, stale_constraint_invocation, actor, 1)?
            .ok_or("missing stale PRIMARY KEY/UNIQUE writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut insert = storage.begin_invocation(id(0x7a), actor, 9_999, 1, 1024)?;
    let table_schema = insert.load_catalog_table_schema(table.table_id)?;
    insert.load_catalog_indexes(&table_schema)?;
    let tuple = encode_tuple(
        &table_schema.columns,
        table_schema.schema_version,
        &[Some(ScalarValue::Integer(42))],
    )?;
    let row_id = id(0x7b);
    insert.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: row_id,
        },
        &tuple,
    )?;
    insert.commit()?;
    let row_key = table_row_key(table.table_id, row_id);
    let (forward_count_before, reverse_count_before) = {
        let read = database.begin_read()?;
        let forward = read.open_table(schema::INDEXES)?;
        let reverse = read.open_table(schema::ROW_INDEXES)?;
        (forward.iter()?.count(), reverse.iter()?.count())
    };
    assert_eq!(forward_count_before, 2);
    assert_eq!(reverse_count_before, 2);

    let drop_invocation = id(0x7c);
    let mut drop_audit = identity_audit_event(0x7d, actor, table.table_id);
    drop_audit.correlation_id = drop_invocation;
    storage.drop_table(drop_invocation, actor, table.table_id, 9_999, drop_audit)?;
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing primary/unique table-drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, unique_constraint_id),
        id_catalog_key(2, unique_constraint_id),
        unique_index_key,
        id_catalog_key(2, unique_index_id),
        dependency_write(unique_constraint_id, unique_index_id, 4).key,
        id_catalog_key(8, primary_constraint_id),
        id_catalog_key(2, primary_constraint_id),
        primary_index_key,
        id_catalog_key(2, primary_index_id),
        dependency_write(primary_constraint_id, primary_index_id, 4).key,
        id_catalog_key(8, not_null_constraint_id),
        id_catalog_key(2, not_null_constraint_id),
        id_catalog_key(6, table.column_ids[0]),
        id_catalog_key(2, table.column_ids[0]),
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "owned primary/unique record remained: {key:?}"
        );
    }
    assert!(read.open_table(ROWS)?.get(&row_key[..])?.is_none());
    assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 0);
    assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 0);
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn unique_constraint_backfill_racing_duplicate_insert_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let actor = id(0xbc);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xbd, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xbd),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "unique_constraint_race_owner"),
            object_write_with_name(actor, 8, "unique_constraint_race_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0xbe);
    let mut schema_audit = identity_audit_event(0xbf, actor, id(0xc0));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "unique_race_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0xc1);
    let mut create_audit = identity_audit_event(0xc2, actor, id(0xc3));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "unique_race_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;

    let existing_invocation = id(0xc4);
    let existing_row_id = id(0xc5);
    let mut existing_writer =
        storage.begin_invocation(existing_invocation, actor, 9_999, 2, 1024)?;
    let table_schema = existing_writer.load_catalog_table_schema(table.table_id)?;
    existing_writer.load_catalog_indexes(&table_schema)?;
    let duplicate_tuple = encode_tuple(
        &table_schema.columns,
        table_schema.schema_version,
        &[Some(ScalarValue::Integer(37))],
    )?;
    existing_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: existing_row_id,
        },
        &duplicate_tuple,
    )?;
    existing_writer.commit()?;

    let writer_invocation = id(0xc6);
    let duplicate_row_id = id(0xc7);
    let mut pending_writer = storage.begin_invocation(writer_invocation, actor, 9_999, 2, 1024)?;
    let pending_schema = pending_writer.load_catalog_table_schema(table.table_id)?;
    pending_writer.load_catalog_indexes(&pending_schema)?;
    let pending_tuple = encode_tuple(
        &pending_schema.columns,
        pending_schema.schema_version,
        &[Some(ScalarValue::Integer(37))],
    )?;
    pending_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: duplicate_row_id,
        },
        &pending_tuple,
    )?;

    let constraint_id = id(0xc8);
    let index_id = id(0xc9);
    let constraint = constraint_write(
        constraint_id,
        table.table_id,
        3,
        &table.column_ids,
        None,
        &[],
        Some(index_id),
    );
    let (index_key, index_payload) =
        owned_unique_index_definition(index_id, table.table_id, &table.column_ids, constraint_id);
    let table_key = id_catalog_key(5, table.table_id);
    let table_bytes = {
        let read = database.begin_read()?;
        read.open_table(CATALOG)?
            .get(table_key.as_slice())?
            .ok_or("table record missing before unique constraint race")?
            .value()
            .to_vec()
    };
    let current_record = decode_registered_envelope(&table_bytes)?;
    let current_epoch = u64::from_le_bytes(
        current_record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("table write epoch missing before unique constraint race")?
            .payload
            .as_slice()
            .try_into()?,
    );
    let mut table_epoch = table_write_with_versions(
        table.table_id,
        schema.schema_id,
        table_schema.schema_version,
        current_epoch + 1,
    );
    table_epoch.expected_value = Some(table_bytes);
    let constraint_invocation = id(0xca);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, constraint_result) = std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            pending_writer.commit()
        });
        let constraint_barrier = barrier.clone();
        let constraint_storage = &storage;
        let constraint = scope.spawn(move || {
            let mut audit = identity_audit_event(0xcb, actor, constraint_id);
            audit.correlation_id = constraint_invocation;
            constraint_barrier.wait();
            constraint_storage.commit_authorized_constraint_ddl(
                constraint_invocation,
                actor,
                9_999,
                &[
                    constraint,
                    object_write(constraint_id, 6),
                    CatalogWrite {
                        key: index_key,
                        expected_value: None,
                        payload: Some(index_payload),
                    },
                    object_write(index_id, 7),
                    dependency_write(constraint_id, index_id, 4),
                    table_epoch,
                ],
                &[audit],
            )
        });
        barrier.wait();
        (writer.join().unwrap(), constraint.join().unwrap())
    });

    let (writer_status, constraint_status) = match (writer_result, constraint_result) {
        (Ok(_), Err(CommitError::UniqueViolation)) => {
            (OutcomeStatus::Committed, OutcomeStatus::Aborted)
        }
        (Err(TransactionError::Commit(CommitError::Conflict)), Ok(_)) => {
            (OutcomeStatus::Aborted, OutcomeStatus::Committed)
        }
        (writer, constraint) => {
            panic!(
                "unexpected duplicate-insert/unique-constraint race: {writer:?} / {constraint:?}"
            )
        }
    };
    assert_eq!(commit_sequence(&database)?, before_race + 1);
    assert_eq!(
        lookup_outcome(&database, writer_invocation, actor, 1)?
            .ok_or("missing duplicate-writer race outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, constraint_invocation, actor, 1)?
            .ok_or("missing unique-constraint race outcome")?
            .status,
        constraint_status
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let rows = read.open_table(ROWS)?;
    let indexes = read.open_table(schema::INDEXES)?;
    let reverse_indexes = read.open_table(schema::ROW_INDEXES)?;
    if constraint_status == OutcomeStatus::Committed {
        assert!(
            catalog
                .get(id_catalog_key(8, constraint_id).as_slice())?
                .is_some()
        );
        assert!(
            rows.get(table_row_key(table.table_id, duplicate_row_id).as_slice())?
                .is_none()
        );
        assert_eq!(indexes.iter()?.count(), 1);
        assert_eq!(reverse_indexes.iter()?.count(), 1);
    } else {
        assert!(
            catalog
                .get(id_catalog_key(8, constraint_id).as_slice())?
                .is_none()
        );
        assert!(
            rows.get(table_row_key(table.table_id, duplicate_row_id).as_slice())?
                .is_some()
        );
        assert_eq!(indexes.iter()?.count(), 0);
        assert_eq!(reverse_indexes.iter()?.count(), 0);
    }
    drop(reverse_indexes);
    drop(indexes);
    drop(rows);
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_drop_table_racing_invocation_leaves_no_orphan_row()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x31);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x32, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x32),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_race_owner"),
            object_write_with_name(actor, 8, "drop_race_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x33);
    let mut schema_audit = identity_audit_event(0x34, actor, id(0x35));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "drop_race_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x36);
    let mut create_audit = identity_audit_event(0x37, actor, id(0x38));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "racing_drop".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;

    let writer_invocation = id(0x39);
    let staged_row_id = id(0x3a);
    let mut writer = storage.begin_invocation(writer_invocation, actor, 9_999, 1, 1024)?;
    let table_schema = writer.load_catalog_table_schema(table.table_id)?;
    writer.load_catalog_indexes(&table_schema)?;
    let payload = encode_tuple(
        &table_schema.columns,
        table_schema.schema_version,
        &[Some(ScalarValue::Text("racing commit".to_owned()))],
    )?;
    writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: staged_row_id,
        },
        &payload,
    )?;

    let barrier = Arc::new(Barrier::new(3));
    let drop_invocation = id(0x3b);
    let (commit_result, drop_result) = std::thread::scope(|scope| {
        let commit_barrier = barrier.clone();
        let commit = scope.spawn(move || {
            commit_barrier.wait();
            writer.commit()
        });
        let drop_barrier = barrier.clone();
        let drop_storage = &storage;
        let drop = scope.spawn(move || {
            let mut audit = identity_audit_event(0x3c, actor, table.table_id);
            audit.correlation_id = drop_invocation;
            drop_barrier.wait();
            drop_storage.drop_table(drop_invocation, actor, table.table_id, 9_999, audit)
        });
        barrier.wait();
        (commit.join().unwrap(), drop.join().unwrap())
    });

    let writer_status = match commit_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(TransactionError::Commit(CommitError::Conflict)) => OutcomeStatus::Aborted,
        other => panic!("unexpected table-drop race writer result: {other:?}"),
    };
    assert!(
        drop_result.is_ok(),
        "table drop lost race unexpectedly: {drop_result:?}"
    );
    assert_eq!(
        lookup_outcome(&database, writer_invocation, actor, 1)?
            .ok_or("missing racing writer outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing racing table-drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(row(&database, &table_row_key(table.table_id, staged_row_id))?.is_none());
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .is_none()
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_drop_table_byte_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefinition, TableLifecycleError,
    };
    use prdb_engine::tuple::MAX_TUPLE_BYTES;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x93);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x94, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x94),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_byte_limit_owner"),
            object_write_with_name(actor, 8, "drop_byte_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x95);
    let mut schema_audit = identity_audit_event(0x96, actor, id(0x97));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "drop_byte_limit_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x98);
    let mut create_audit = identity_audit_event(0x99, actor, id(0x9a));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "bounded_drop_bytes".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Bytea,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    }];
    let empty_tuple = encode_tuple(&columns, 1, &[Some(ScalarValue::Bytea(Vec::new()))])?;
    let full_tuple = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(vec![
            b'x';
            MAX_TUPLE_BYTES
                - empty_tuple.len()
        ]))],
    )?;
    let one_byte_over_tuple = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(vec![
            b'y';
            1_281 - empty_tuple.len()
        ]))],
    )?;
    let exact_boundary_tuple = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(vec![
            b'z';
            1_280 - empty_tuple.len()
        ]))],
    )?;
    assert_eq!(full_tuple.len(), MAX_TUPLE_BYTES);
    assert_eq!(one_byte_over_tuple.len(), 1_281);
    assert_eq!(exact_boundary_tuple.len(), 1_280);
    assert_eq!(
        64 * full_tuple.len() + one_byte_over_tuple.len(),
        64 * 1024 * 1024 + 1
    );
    assert_eq!(
        64 * full_tuple.len() + exact_boundary_tuple.len(),
        64 * 1024 * 1024
    );
    let mut rows = (1u64..=64)
        .map(|row_number| {
            let mut row_id = [0; 16];
            row_id[8..].copy_from_slice(&row_number.to_be_bytes());
            RowWrite {
                key: table_row_key(table.table_id, row_id),
                expected_version: None,
                payload: Some(full_tuple.clone()),
            }
        })
        .collect::<Vec<_>>();
    let mut final_row_id = [0; 16];
    final_row_id[8..].copy_from_slice(&65u64.to_be_bytes());
    let final_row_key = table_row_key(table.table_id, final_row_id);
    rows.push(RowWrite {
        key: final_row_key,
        expected_version: None,
        payload: Some(one_byte_over_tuple),
    });
    let first_row_key = rows[0].key;
    let rows_invocation = id(0x9b);
    let before_drop = commit_rows(&database, rows_invocation, actor, 9_999, &rows)?;
    let over_limit_invocation = id(0x9c);
    let mut over_limit_audit = identity_audit_event(0x9d, actor, table.table_id);
    over_limit_audit.correlation_id = over_limit_invocation;
    assert_eq!(
        storage.drop_table(
            over_limit_invocation,
            actor,
            table.table_id,
            9_999,
            over_limit_audit,
        ),
        Err(TableLifecycleError::Commit(
            CommitError::TableDropLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, before_drop);
    assert_eq!(
        lookup_outcome(&database, over_limit_invocation, actor, 1)?
            .ok_or("missing table-drop byte-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert!(row(&database, &final_row_key)?.is_some());
    let exact_row_update = RowWrite {
        key: final_row_key,
        expected_version: Some(before_drop),
        payload: Some(exact_boundary_tuple),
    };
    let update_result = commit_rows(&database, id(0x9e), actor, 9_999, &[exact_row_update]);
    assert_eq!(update_result, Ok(before_drop + 1));
    let boundary_invocation = id(0x9f);
    let mut boundary_audit = identity_audit_event(0xa0, actor, table.table_id);
    boundary_audit.correlation_id = boundary_invocation;
    let drop_result = storage.drop_table(
        boundary_invocation,
        actor,
        table.table_id,
        9_999,
        boundary_audit,
    );
    assert!(
        drop_result.is_ok(),
        "exact byte-boundary drop returned {drop_result:?}"
    );
    assert_eq!(drop_result.unwrap().table_id, table.table_id);
    assert_eq!(commit_sequence(&database)?, before_drop + 2);
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact table-drop byte-boundary outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(row(&database, &first_row_key)?.is_none());
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_drop_table_index_byte_limit_boundary_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;
    use prdb_engine::tuple::MAX_TUPLE_BYTES;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x92);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x93, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x93),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_index_byte_limit_owner"),
            object_write_with_name(actor, 8, "drop_index_byte_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;

    let columns = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    }];
    let row_count = 32u64;
    let budget = 64 * 1024 * 1024;
    let full_tuple = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(vec![b'x'; MAX_TUPLE_BYTES - 25]))],
    )?;
    let full_key = {
        let mut row_id = [0; 16];
        row_id[8..].copy_from_slice(&1u64.to_be_bytes());
        typed_bytea_index_key(id(0x44), &table_row_key(id(16), row_id), &full_tuple).key
    };
    let locator_bytes = [0; 32];
    let full_contribution = full_tuple.len() + full_key.len() + locator_bytes.len();
    let preceding_bytes = (row_count as usize - 1) * full_contribution;
    assert!(preceding_bytes < budget);

    let make_final_tuple = |target: usize, zero_count: usize| {
        let value_length = (target - 92 - zero_count) / 2;
        let mut value = vec![b'y'; value_length];
        value[..zero_count].fill(0);
        encode_tuple(&columns, 1, &[Some(ScalarValue::Bytea(value))]).unwrap()
    };
    let final_over_tuple = make_final_tuple(budget + 1 - preceding_bytes, 1);
    let final_exact_tuple = make_final_tuple(budget - preceding_bytes, 0);

    let make_write = |row_number: u64, payload: Vec<u8>, expected_version| {
        let mut row_id = [0; 16];
        row_id[8..].copy_from_slice(&row_number.to_be_bytes());
        let row_key = table_row_key(id(16), row_id);
        let index = typed_bytea_index_key(id(0x44), &row_key, &payload);
        (
            RowWrite {
                key: row_key,
                expected_version,
                payload: Some(payload),
            },
            IndexWrite {
                key: index.key,
                row_key,
                expected_version,
                present: true,
                unique: false,
                null_exempt: false,
            },
        )
    };
    let mut rows = Vec::with_capacity(row_count as usize);
    let mut indexes = Vec::with_capacity(row_count as usize);
    for row_number in 1..row_count {
        let (row, index) = make_write(row_number, full_tuple.clone(), None);
        rows.push(row);
        indexes.push(index);
    }
    let (final_row, final_index) = make_write(row_count, final_over_tuple, None);
    let final_row_key = final_row.key;
    rows.push(final_row);
    indexes.push(final_index);
    let accounted_bytes = |row_payload: &[u8], index_key: &[u8]| {
        preceding_bytes + row_payload.len() + index_key.len() + locator_bytes.len()
    };
    assert_eq!(
        accounted_bytes(
            rows.last().unwrap().payload.as_deref().unwrap(),
            indexes.last().unwrap().key.as_slice(),
        ),
        budget + 1
    );
    let seed_sequence =
        commit_rows_and_indexes(&database, id(0x94), actor, 9_999, &rows, &indexes)?;
    let over_limit_invocation = id(0x95);
    let mut over_limit_audit = identity_audit_event(0x96, actor, id(16));
    over_limit_audit.correlation_id = over_limit_invocation;
    assert_eq!(
        storage.drop_table(
            over_limit_invocation,
            actor,
            id(16),
            9_999,
            over_limit_audit
        ),
        Err(TableLifecycleError::Commit(
            CommitError::IndexDdlLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, seed_sequence);
    assert_eq!(
        lookup_outcome(&database, over_limit_invocation, actor, 1)?
            .ok_or("missing table-drop index-byte-limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert!(row(&database, &final_row_key)?.is_some());

    let (updated_row, mut new_index) =
        make_write(row_count, final_exact_tuple, Some(seed_sequence));
    let old_key = indexes.last().unwrap().key.clone();
    let updated_index = IndexWrite {
        key: old_key,
        row_key: final_row_key,
        expected_version: Some(seed_sequence),
        present: false,
        unique: false,
        null_exempt: false,
    };
    new_index.expected_version = None;
    assert_ne!(updated_index.key, new_index.key);
    let exact_sequence = commit_rows_and_indexes(
        &database,
        id(0x97),
        actor,
        9_999,
        &[updated_row],
        &[updated_index, new_index],
    )?;
    let exact_invocation = id(0x98);
    let mut exact_audit = identity_audit_event(0x99, actor, id(16));
    exact_audit.correlation_id = exact_invocation;
    let dropped = storage.drop_table(exact_invocation, actor, id(16), 9_999, exact_audit);
    assert!(
        dropped.is_ok(),
        "exact index byte-boundary drop returned {dropped:?}"
    );
    assert_eq!(commit_sequence(&database)?, exact_sequence + 1);
    assert_eq!(
        lookup_outcome(&database, exact_invocation, actor, 1)?
            .ok_or("missing exact table-drop index-byte-boundary outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(row(&database, &final_row_key)?.is_none());
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_drop_table_index_entry_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x90);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x91, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x91),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_index_limit_owner"),
            object_write_with_name(actor, 8, "drop_index_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let index_id = id(0x88);
    let (index_key, index_payload) = index_definition(index_id, id(16));
    assert_eq!(
        storage.commit_catalog_and_audit(
            id(0x92),
            id(9),
            9_999,
            &[
                CatalogWrite {
                    key: index_key.clone(),
                    expected_value: None,
                    payload: Some(index_payload),
                },
                object_write(index_id, 7),
            ],
            &[audit_event(0x92)],
        )?,
        2
    );

    let payload = bytea_tuple(b"index cleanup limit");
    let make_batch = |start: u64, end: u64| {
        let mut rows = Vec::with_capacity((end - start + 1) as usize);
        let mut indexes = Vec::with_capacity((end - start + 1) as usize * 2);
        for row_number in start..=end {
            let mut row_id = [0; 16];
            row_id[8..].copy_from_slice(&row_number.to_be_bytes());
            let row_key = table_row_key(id(16), row_id);
            let first = typed_bytea_index_key(id(0x44), &row_key, &payload);
            let second = typed_bytea_index_key(index_id, &row_key, &payload);
            rows.push(RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(payload.clone()),
            });
            for derived in [first, second] {
                indexes.push(IndexWrite {
                    key: derived.key,
                    row_key,
                    expected_version: None,
                    present: true,
                    unique: false,
                    null_exempt: false,
                });
            }
        }
        (rows, indexes)
    };
    let (first_rows, first_indexes) = make_batch(1, 32_768);
    assert_eq!(first_indexes.len(), 65_536);
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(0x93),
            actor,
            9_999,
            &first_rows,
            &first_indexes,
        )?,
        3
    );
    let (last_rows, last_indexes) = make_batch(32_769, 32_769);
    assert_eq!(
        commit_rows_and_indexes(&database, id(0x94), actor, 9_999, &last_rows, &last_indexes,)?,
        4
    );

    let over_limit_invocation = id(0x95);
    let mut over_limit_audit = identity_audit_event(0x95, actor, id(16));
    over_limit_audit.correlation_id = over_limit_invocation;
    assert_eq!(
        storage.drop_table(
            over_limit_invocation,
            actor,
            id(16),
            9_999,
            over_limit_audit,
        ),
        Err(TableLifecycleError::Commit(
            CommitError::IndexDdlLimitExceeded
        ))
    );
    assert_eq!(commit_sequence(&database)?, 4);
    assert_eq!(
        lookup_outcome(&database, over_limit_invocation, actor, 1)?
            .ok_or("missing table index-entry limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(CATALOG)?
            .get(id_catalog_key(5, id(16)).as_slice())?
            .is_some()
    );
    assert!(read.open_table(ROWS)?.get(&last_rows[0].key[..])?.is_some());
    assert!(
        read.open_table(schema::INDEXES)?
            .get(&last_indexes[0].key[..])?
            .is_some()
    );
    drop(read);

    let last_row_key = last_rows[0].key;
    let first_index_key = typed_bytea_index_key(id(0x44), &last_row_key, &payload).key;
    let second_index_key = typed_bytea_index_key(index_id, &last_row_key, &payload).key;
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(0x96),
            actor,
            9_999,
            &[RowWrite {
                key: last_row_key,
                expected_version: Some(4),
                payload: None,
            }],
            &[
                IndexWrite {
                    key: first_index_key,
                    row_key: last_row_key,
                    expected_version: Some(4),
                    present: false,
                    unique: false,
                    null_exempt: false,
                },
                IndexWrite {
                    key: second_index_key,
                    row_key: last_row_key,
                    expected_version: Some(4),
                    present: false,
                    unique: false,
                    null_exempt: false,
                },
            ],
        )?,
        5
    );
    let boundary_invocation = id(0x97);
    let mut boundary_audit = identity_audit_event(0x97, actor, id(16));
    boundary_audit.correlation_id = boundary_invocation;
    assert_eq!(
        storage
            .drop_table(boundary_invocation, actor, id(16), 9_999, boundary_audit,)?
            .table_id,
        id(16)
    );
    assert_eq!(commit_sequence(&database)?, 6);
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact table index-entry boundary outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(id_catalog_key(5, id(16)).as_slice())?.is_none());
    for dropped_index_id in [id(0x44), index_id] {
        assert!(
            catalog
                .get(id_catalog_key(9, dropped_index_id).as_slice())?
                .is_none()
        );
        assert!(
            catalog
                .get(id_catalog_key(2, dropped_index_id).as_slice())?
                .is_none()
        );
    }
    assert!(read.open_table(ROWS)?.get(&last_row_key[..])?.is_none());
    for dropped_index_id in [id(0x44), index_id] {
        assert!(
            read.open_table(schema::INDEXES)?
                .range(dropped_index_id.as_slice()..)?
                .next()
                .transpose()?
                .is_none_or(|(key, _)| !key.value().starts_with(&dropped_index_id))
        );
    }
    assert!(
        read.open_table(schema::ROW_INDEXES)?
            .range(id(16).as_slice()..)?
            .next()
            .transpose()?
            .is_none_or(|(key, _)| !key.value().starts_with(&id(16)))
    );
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_renames_tables_and_drop_column_rewrites_rows()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0x60);
    let successor_owner = id(0xd0);
    let successor_role = id(0xd6);
    let outsider = id(0xd7);
    let revocation_admin = id(0xc8);
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "column_drop_owner"),
        object_write_with_name(actor, 8, "column_drop_owner"),
        principal_write_with_name(successor_owner, 1, 1, "successor_owner"),
        object_write_with_name(successor_owner, 8, "successor_owner"),
        principal_write_with_name(successor_role, 2, 1, "successor_role"),
        object_write_with_name(successor_role, 9, "successor_role"),
        membership_write(successor_owner, successor_role),
        dependency_write(successor_owner, successor_role, 5),
        principal_write_with_name(outsider, 1, 1, "table_ddl_outsider"),
        object_write_with_name(outsider, 8, "table_ddl_outsider"),
        principal_write_with_name(revocation_admin, 1, 1, "role_revocation_admin"),
        object_write_with_name(revocation_admin, 8, "role_revocation_admin"),
        credential_verifier_write(
            id(0xc9),
            revocation_admin,
            &[0xc9; 32],
            1,
            i64::MAX,
            None,
            1,
        ),
        membership_write(revocation_admin, admin_role),
        dependency_write(revocation_admin, admin_role, 5),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x61, admin_role, id(0x62));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x62), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x63);
    let mut schema_audit = identity_audit_event(0x64, actor, id(0x65));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_drop_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x66);
    let mut create_audit = identity_audit_event(0x67, actor, id(0x68));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "column_drop_me".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "obsolete".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "kept".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let rejected_column_rename = id(0x80);
    let mut rejected_column_rename_audit = identity_audit_event(0x81, actor, id(0x82));
    rejected_column_rename_audit.correlation_id = rejected_column_rename;
    let duplicate_column_rename = storage.rename_column(
        rejected_column_rename,
        actor,
        table.table_id,
        table.column_ids[0],
        "kept",
        9_999,
        rejected_column_rename_audit,
    );
    assert!(
        matches!(
            duplicate_column_rename,
            Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
                CommitError::InvalidCatalog
            ))
        ),
        "unexpected duplicate column rename result: {duplicate_column_rename:?}"
    );
    assert_eq!(
        lookup_outcome(&database, rejected_column_rename, actor, 1)?
            .ok_or("missing duplicate column-name outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let stale_column_writer_invocation = id(0x86);
    let stale_column_row = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x87),
    };
    let mut stale_column_writer =
        storage.begin_invocation(stale_column_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_column_schema = stale_column_writer.load_catalog_table_schema(table.table_id)?;
    stale_column_writer.load_catalog_indexes(&stale_column_schema)?;
    let stale_column_tuple = encode_tuple(
        &stale_column_schema.columns,
        stale_column_schema.schema_version,
        &[
            Some(ScalarValue::Integer(17)),
            Some(ScalarValue::Text("old shape".to_owned())),
        ],
    )?;
    stale_column_writer.stage_put(stale_column_row, &stale_column_tuple)?;
    let column_rename = id(0x83);
    let mut column_rename_audit = identity_audit_event(0x84, actor, id(0x85));
    column_rename_audit.correlation_id = column_rename;
    let column_rename_result = storage.rename_column(
        column_rename,
        actor,
        table.table_id,
        table.column_ids[0],
        "retired",
        9_999,
        column_rename_audit,
    );
    assert!(
        column_rename_result.is_ok(),
        "column rename failed: {column_rename_result:?}"
    );
    let column_rename_sequence = column_rename_result?;
    assert!(column_rename_sequence > table.commit_sequence);
    assert!(matches!(
        stale_column_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(
        row(
            &database,
            &table_row_key(table.table_id, stale_column_row.row)
        )?
        .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, stale_column_writer_invocation, actor, 1)?
            .ok_or("missing stale column-rename writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let column_object = decode_registered_envelope(
            catalog
                .get(id_catalog_key(2, table.column_ids[0]).as_slice())?
                .ok_or("renamed column object missing")?
                .value(),
        )?;
        assert_eq!(
            column_object
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("column object name missing")?
                .payload,
            b"retired"
        );
        assert_eq!(
            u64::from_le_bytes(
                column_object
                    .fields
                    .iter()
                    .find(|field| field.tag == 5)
                    .ok_or("column object version missing")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            2
        );
        let column_record = decode_registered_envelope(
            catalog
                .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
                .ok_or("renamed column record missing")?
                .value(),
        )?;
        assert_eq!(
            column_record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("column record name missing")?
                .payload,
            b"retired"
        );
    }
    let stale_writer_invocation = id(0x5b);
    let stale_row_id = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x5c),
    };
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 2, 64)?;
    let pinned_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    assert_eq!(pinned_schema.schema_version, 1);
    stale_writer.load_catalog_indexes(&pinned_schema)?;
    let stale_payload = prdb_engine::tuple::encode_tuple(
        &pinned_schema.columns,
        pinned_schema.schema_version,
        &[
            Some(ScalarValue::Integer(99)),
            Some(ScalarValue::Text("stale".to_owned())),
        ],
    )?;
    stale_writer.stage_put(stale_row_id, &stale_payload)?;
    let rename_invocation = id(0x76);
    let mut rename_audit = identity_audit_event(0x77, actor, id(0x78));
    rename_audit.correlation_id = rename_invocation;
    let rename_sequence = storage.rename_table(
        rename_invocation,
        actor,
        table.table_id,
        "renamed_column_drop_me",
        9_999,
        rename_audit,
    )?;
    assert!(rename_sequence > table.commit_sequence);
    let stale_result = stale_writer.commit();
    assert!(
        matches!(
            stale_result,
            Err(prdb_engine::transaction::TransactionError::Commit(
                CommitError::Conflict
            ))
        ),
        "stale writer returned {stale_result:?}"
    );
    assert!(row(&database, &table_row_key(table.table_id, stale_row_id.row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale-writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let duplicate_table = storage.create_table(CreateTableRequest {
        invocation_id: id(0x79),
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "occupied_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: {
            let mut event = identity_audit_event(0x7a, actor, id(0x7b));
            event.correlation_id = id(0x79);
            event
        },
    })?;
    assert_ne!(duplicate_table.table_id, table.table_id);
    let duplicate_rename_invocation = id(0x7c);
    let mut duplicate_rename_audit = identity_audit_event(0x7d, actor, id(0x7e));
    duplicate_rename_audit.correlation_id = duplicate_rename_invocation;
    let duplicate_rename = storage.rename_table(
        duplicate_rename_invocation,
        actor,
        table.table_id,
        "occupied_table",
        9_999,
        duplicate_rename_audit,
    );
    assert!(
        matches!(
            duplicate_rename,
            Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
                prdb_engine::commit::CommitError::InvalidCatalog
            ))
        ),
        "duplicate rename returned {duplicate_rename:?}"
    );
    assert_eq!(
        lookup_outcome(&database, duplicate_rename_invocation, actor, 1)?
            .ok_or("missing duplicate table-name outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let table_object = decode_registered_envelope(
            catalog
                .get(id_catalog_key(2, table.table_id).as_slice())?
                .ok_or("renamed table object missing")?
                .value(),
        )?;
        assert_eq!(
            table_object
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table name missing")?
                .payload,
            b"renamed_column_drop_me"
        );
        let table_record = decode_registered_envelope(
            catalog
                .get(id_catalog_key(5, table.table_id).as_slice())?
                .ok_or("renamed table record missing")?
                .value(),
        )?;
        assert_eq!(
            u64::from_le_bytes(
                table_record
                    .fields
                    .iter()
                    .find(|field| field.tag == 2)
                    .ok_or("schema version missing")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            1
        );
        assert_eq!(
            u64::from_le_bytes(
                table_record
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("write epoch missing")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            3
        );
    }
    let row_key = table_row_key(table.table_id, id(0x69));
    let old_payload = encode_tuple(
        &[
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        ],
        1,
        &[
            Some(ScalarValue::Integer(73)),
            Some(ScalarValue::Text("preserved".to_owned())),
        ],
    )?;
    let row_record = RowRecord {
        row_version: table.commit_sequence,
        payload: old_payload,
    }
    .encode()?;
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&row_key[..], &row_record[..])?;
    write.commit()?;

    let index_id = id(0x70);
    let (index_catalog_key, index_definition) =
        index_definition_with_columns(index_id, table.table_id, &[table.column_ids[1]]);
    let index_invocation = id(0x71);
    let mut index_audit = identity_audit_event(0x72, actor, index_id);
    index_audit.correlation_id = index_invocation;
    storage.commit_authorized_index_ddl(
        index_invocation,
        actor,
        9_999,
        &[
            CatalogWrite {
                key: index_catalog_key.clone(),
                expected_value: None,
                payload: Some(index_definition),
            },
            object_write(index_id, 7),
        ],
        &[index_audit],
    )?;
    let old_index_key = prdb_engine::index_key::derive_index_key(
        index_id,
        row_key[16..].try_into()?,
        1,
        &table.column_ids,
        &[
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        ],
        &RowRecord::decode(&row_record)?.payload,
        &[table.column_ids[1]],
    )?;
    let old_index_row_version = {
        let read = database.begin_read()?;
        let indexes = read.open_table(schema::INDEXES)?;
        let locator = indexes
            .get(old_index_key.key.as_slice())?
            .ok_or("index entry missing after index build")?;
        IndexLocator::decode_for_key(&old_index_key.key, locator.value())?.row_version
    };

    let denied_drop_invocation = id(0x73);
    let mut denied_drop_audit = identity_audit_event(0x74, actor, id(0x75));
    denied_drop_audit.correlation_id = denied_drop_invocation;
    assert!(matches!(
        storage.drop_column(
            denied_drop_invocation,
            actor,
            table.table_id,
            table.column_ids[1],
            9_999,
            denied_drop_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::ColumnHasDependencies
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, denied_drop_invocation, actor, 1)?
            .ok_or("missing denied column-drop outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let stale_drop_invocation = id(0x88);
    let stale_drop_row = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x89),
    };
    let mut stale_drop_writer =
        storage.begin_invocation(stale_drop_invocation, actor, 9_999, 1, 1024)?;
    let stale_drop_schema = stale_drop_writer.load_catalog_table_schema(table.table_id)?;
    stale_drop_writer.load_catalog_indexes(&stale_drop_schema)?;
    let stale_drop_tuple = encode_tuple(
        &stale_drop_schema.columns,
        stale_drop_schema.schema_version,
        &[
            Some(ScalarValue::Integer(23)),
            Some(ScalarValue::Text("staged before drop".to_owned())),
        ],
    )?;
    stale_drop_writer.stage_put(stale_drop_row, &stale_drop_tuple)?;

    let drop_invocation = id(0x6a);
    let mut drop_audit = identity_audit_event(0x6b, actor, id(0x6c));
    drop_audit.correlation_id = drop_invocation;
    let dropped = storage.drop_column(
        drop_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        9_999,
        drop_audit,
    )?;
    assert_eq!(dropped.column_id, table.column_ids[0]);
    assert!(matches!(
        stale_drop_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(
        row(
            &database,
            &table_row_key(table.table_id, stale_drop_row.row)
        )?
        .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, stale_drop_invocation, actor, 1)?
            .ok_or("missing stale DROP COLUMN writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .is_none()
    );
    let surviving = decode_registered_envelope(
        catalog
            .get(id_catalog_key(6, table.column_ids[1]).as_slice())?
            .ok_or("surviving column missing")?
            .value(),
    )?;
    assert_eq!(
        u32::from_le_bytes(
            surviving
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("surviving ordinal missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        0
    );
    let table_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("schema version missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        2
    );
    let stored = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&row_key[..])?
            .ok_or("row missing")?
            .value(),
    )?;
    let new_schema = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let tuple = prdb_engine::tuple::decode_tuple(&new_schema, 2, &stored.payload)?;
    assert_eq!(
        tuple.values,
        vec![Some(ScalarValue::Text("preserved".to_owned()))]
    );
    let rebuilt_index_key = prdb_engine::index_key::derive_index_key(
        index_id,
        row_key[16..].try_into()?,
        2,
        &[table.column_ids[1]],
        &[ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        }],
        &stored.payload,
        &[table.column_ids[1]],
    )?;
    let indexes = read.open_table(schema::INDEXES)?;
    assert_eq!(old_index_key.key, rebuilt_index_key.key);
    let rebuilt_locator = indexes
        .get(rebuilt_index_key.key.as_slice())?
        .ok_or("index entry missing after schema rewrite")?;
    assert_eq!(
        IndexLocator::decode_for_key(&rebuilt_index_key.key, rebuilt_locator.value())?.row_version,
        stored.row_version,
    );
    assert_ne!(stored.row_version, old_index_row_version);
    let mut old_reverse_key = row_key.to_vec();
    old_reverse_key.extend_from_slice(&old_index_key.key[..old_index_key.key.len() - 16]);
    let mut rebuilt_reverse_key = row_key.to_vec();
    rebuilt_reverse_key
        .extend_from_slice(&rebuilt_index_key.key[..rebuilt_index_key.key.len() - 16]);
    let reverse_indexes = read.open_table(schema::ROW_INDEXES)?;
    assert_eq!(old_reverse_key, rebuilt_reverse_key);
    assert!(
        reverse_indexes
            .get(rebuilt_reverse_key.as_slice())?
            .is_some()
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(read);
    let stale_owner_writer_invocation = id(0xdb);
    let stale_owner_row = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0xda),
    };
    let mut stale_owner_writer =
        storage.begin_invocation(stale_owner_writer_invocation, actor, 9_999, 2, 64)?;
    let stale_owner_schema = stale_owner_writer.load_catalog_table_schema(table.table_id)?;
    stale_owner_writer.load_catalog_indexes(&stale_owner_schema)?;
    let stale_owner_tuple = prdb_engine::tuple::encode_tuple(
        &stale_owner_schema.columns,
        stale_owner_schema.schema_version,
        &[Some(ScalarValue::Text("stale".to_owned()))],
    )?;
    stale_owner_writer.stage_put(stale_owner_row, &stale_owner_tuple)?;
    let transfer_invocation = id(0xd1);
    let mut transfer_audit = identity_audit_event(0xd2, actor, table.table_id);
    transfer_audit.correlation_id = transfer_invocation;
    let transfer_result = storage.set_table_owner(
        transfer_invocation,
        actor,
        table.table_id,
        successor_role,
        9_999,
        transfer_audit,
    );
    assert!(
        transfer_result.is_ok(),
        "table owner transfer failed: {transfer_result:?}"
    );
    let transfer_sequence = transfer_result?;
    assert!(transfer_sequence > table.commit_sequence);
    assert!(matches!(
        stale_owner_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(
        row(
            &database,
            &table_row_key(table.table_id, stale_owner_row.row)
        )?
        .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, stale_owner_writer_invocation, actor, 1)?
            .ok_or("missing stale ownership-transfer writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let object = decode_registered_envelope(
            catalog
                .get(id_catalog_key(2, table.table_id).as_slice())?
                .ok_or("table object disappeared after ownership transfer")?
                .value(),
        )?;
        assert_eq!(
            object
                .fields
                .iter()
                .find(|field| field.tag == 4)
                .ok_or("table owner field missing")?
                .payload,
            successor_role
        );
        let mut successor_grants_prefix = 12u16.to_be_bytes().to_vec();
        successor_grants_prefix.push(1);
        successor_grants_prefix.extend_from_slice(&successor_role);
        assert!(
            catalog
                .range(successor_grants_prefix.as_slice()..)?
                .next()
                .transpose()?
                .is_none_or(|(key, _)| !key.value().starts_with(&successor_grants_prefix))
        );
    }
    let outsider_transfer_invocation = id(0xd8);
    let mut outsider_transfer_audit = identity_audit_event(0xd9, outsider, table.table_id);
    outsider_transfer_audit.correlation_id = outsider_transfer_invocation;
    assert!(matches!(
        storage.set_table_owner(
            outsider_transfer_invocation,
            outsider,
            table.table_id,
            actor,
            9_999,
            outsider_transfer_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(&database, outsider_transfer_invocation, outsider, 1)?
            .ok_or("missing unauthorized ownership-transfer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let successor_rename_invocation = id(0xd3);
    let mut successor_rename_audit = identity_audit_event(0xd4, successor_owner, table.table_id);
    successor_rename_audit.correlation_id = successor_rename_invocation;
    storage.rename_table(
        successor_rename_invocation,
        successor_owner,
        table.table_id,
        "successor_owned_table",
        9_999,
        successor_rename_audit,
    )?;
    let membership_revoke_invocation = id(0xdc);
    let mut membership_revoke_audit = identity_audit_event(0xdd, revocation_admin, successor_owner);
    membership_revoke_audit.correlation_id = membership_revoke_invocation;
    membership_revoke_audit.action = AuditAction::AuthorityChange;
    storage
        .set_role_membership(
            membership_revoke_invocation,
            revocation_admin,
            prdb_engine::storage_manager::RoleMembershipChange {
                member_id: successor_owner,
                role_id: successor_role,
                grant: false,
            },
            9_999,
            membership_revoke_audit,
        )
        .map_err(|error| format!("membership revocation failed: {error:?}"))?;
    let revoked_owner_rename_invocation = id(0xde);
    let mut revoked_owner_rename_audit =
        identity_audit_event(0xdf, successor_owner, table.table_id);
    revoked_owner_rename_audit.correlation_id = revoked_owner_rename_invocation;
    assert!(matches!(
        storage.rename_table(
            revoked_owner_rename_invocation,
            successor_owner,
            table.table_id,
            "revoked_role_owner_table",
            9_999,
            revoked_owner_rename_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Unauthorized)
    ));
    assert_eq!(
        lookup_outcome(
            &database,
            revoked_owner_rename_invocation,
            successor_owner,
            1
        )?
        .ok_or("missing role-owner revocation outcome")?
        .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let object = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(2, table.table_id).as_slice())?
                .ok_or("table object disappeared after revoked owner operation")?
                .value(),
        )?;
        assert_eq!(
            object
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table name missing after revoked owner operation")?
                .payload,
            b"successor_owned_table"
        );
    }
    let return_invocation = id(0xd5);
    let mut return_audit = identity_audit_event(0xd6, actor, table.table_id);
    return_audit.correlation_id = return_invocation;
    storage.set_table_owner(
        return_invocation,
        actor,
        table.table_id,
        actor,
        9_999,
        return_audit,
    )?;
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_sets_and_drops_not_null_with_existing_row_validation()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x31);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "nullability_owner"),
        object_write_with_name(actor, 8, "nullability_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x32, admin_role, id(0x33));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x33), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x34);
    let mut schema_audit = identity_audit_event(0x35, actor, id(0x36));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "nullability_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x37);
    let mut create_audit = identity_audit_event(0x38, actor, id(0x39));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "nullable_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let row_key = table_row_key(table.table_id, id(0x3a));
    let null_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        }],
        1,
        &[None],
    )?;
    let null_row = RowRecord {
        row_version: table.commit_sequence,
        payload: null_tuple,
    }
    .encode()?;
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&row_key[..], &null_row[..])?;
    write.commit()?;

    let denied_invocation = id(0x3b);
    let mut denied_audit = identity_audit_event(0x3c, actor, id(0x3d));
    denied_audit.correlation_id = denied_invocation;
    assert!(matches!(
        storage.set_column_nullable(
            denied_invocation,
            actor,
            table.table_id,
            table.column_ids[0],
            false,
            9_999,
            denied_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::NotNullViolation
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, denied_invocation, actor, 1)?
            .ok_or("missing NOT NULL rejection outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let column = decode_registered_envelope(
        read.open_table(CATALOG)?
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .ok_or("column missing after rejected NOT NULL")?
            .value(),
    )?;
    assert_eq!(
        column
            .fields
            .iter()
            .find(|field| field.tag == 6)
            .ok_or("nullable field missing")?
            .payload,
        [1]
    );
    drop(read);

    let write = database.begin_write()?;
    write.open_table(ROWS)?.remove(&row_key[..])?;
    write.commit()?;
    let stale_writer_invocation = id(0x44);
    let stale_row_key = table_row_key(table.table_id, id(0x45));
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Text("staged before NOT NULL".to_owned()))],
    )?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: id(0x45),
        },
        &stale_tuple,
    )?;
    let set_invocation = id(0x3e);
    let mut set_audit = identity_audit_event(0x3f, actor, id(0x40));
    set_audit.correlation_id = set_invocation;
    storage.set_column_nullable(
        set_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        false,
        9_999,
        set_audit,
    )?;
    let stale_result = stale_writer.commit();
    assert!(matches!(
        stale_result,
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &stale_row_key)?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale writer outcome after SET NOT NULL")?
            .status,
        OutcomeStatus::Aborted
    );
    let not_null_constraint_key = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let prefix = 8u16.to_be_bytes();
        let mut keys = Vec::new();
        for entry in catalog.range(prefix.as_slice()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            let key = key.value().to_vec();
            if key.len() != 19 {
                continue;
            }
            let record = decode_registered_envelope(value.value())?;
            if record
                .fields
                .iter()
                .any(|field| field.tag == 1 && field.payload.as_slice() == table.table_id)
                && record
                    .fields
                    .iter()
                    .any(|field| field.tag == 2 && field.payload.as_slice() == 1u16.to_le_bytes())
            {
                keys.push(key);
            }
        }
        assert_eq!(keys.len(), 1, "expected one NOT NULL constraint");
        keys.pop()
            .ok_or("NOT NULL constraint missing after SET NOT NULL")?
    };
    let drop_invocation = id(0x41);
    let mut drop_audit = identity_audit_event(0x42, actor, id(0x43));
    drop_audit.correlation_id = drop_invocation;
    storage.set_column_nullable(
        drop_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        true,
        9_999,
        drop_audit,
    )?;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let column = decode_registered_envelope(
        catalog
            .get(id_catalog_key(6, table.column_ids[0]).as_slice())?
            .ok_or("column missing after DROP NOT NULL")?
            .value(),
    )?;
    assert_eq!(
        column
            .fields
            .iter()
            .find(|field| field.tag == 6)
            .ok_or("nullable field missing")?
            .payload,
        [1]
    );
    let table_record = decode_registered_envelope(
        catalog
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing")?
            .value(),
    )?;
    assert_eq!(
        u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or("schema version missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        1
    );
    assert_eq!(
        u64::from_le_bytes(
            table_record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("write epoch missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        3
    );
    assert!(catalog.get(not_null_constraint_key.as_slice())?.is_none());
    let constraint_id: [u8; 16] = not_null_constraint_key[3..19].try_into()?;
    assert!(
        catalog
            .get(id_catalog_key(2, constraint_id).as_slice())?
            .is_none()
    );
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_sets_replaces_and_drops_column_defaults()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefault, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x46);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "column_default_owner"),
        object_write_with_name(actor, 8, "column_default_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x47, admin_role, id(0x48));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x48), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x49);
    let mut schema_audit = identity_audit_event(0x4a, actor, id(0x4b));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_default_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x4c);
    let mut create_audit = identity_audit_event(0x4d, actor, id(0x4e));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "default_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let stale_writer_invocation = id(0x5b);
    let stale_row_id = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x5c),
    };
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Integer(7))],
    )?;
    stale_writer.stage_put(stale_row_id, &stale_tuple)?;
    let set_invocation = id(0x4f);
    let mut set_audit = identity_audit_event(0x50, actor, id(0x51));
    set_audit.correlation_id = set_invocation;
    let set_result =
        storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
            invocation_id: set_invocation,
            authenticated_actor_id: actor,
            table_id: table.table_id,
            column_id: table.column_ids[0],
            default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(42)))),
            retention_deadline_us: 9_999,
            audit_event: set_audit,
        });
    assert!(
        set_result.is_ok(),
        "setting a typed default failed: {set_result:?}"
    );
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(table.table_id, stale_row_id.row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale writer outcome after default change")?
            .status,
        OutcomeStatus::Aborted
    );
    let replace_invocation = id(0x52);
    let mut replace_audit = identity_audit_event(0x53, actor, id(0x54));
    replace_audit.correlation_id = replace_invocation;
    storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
        invocation_id: replace_invocation,
        authenticated_actor_id: actor,
        table_id: table.table_id,
        column_id: table.column_ids[0],
        default: Some(TableColumnDefault::Literal(Some(ScalarValue::Integer(43)))),
        retention_deadline_us: 9_999,
        audit_event: replace_audit,
    })?;
    let drop_invocation = id(0x55);
    let mut drop_audit = identity_audit_event(0x56, actor, id(0x57));
    drop_audit.correlation_id = drop_invocation;
    storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
        invocation_id: drop_invocation,
        authenticated_actor_id: actor,
        table_id: table.table_id,
        column_id: table.column_ids[0],
        default: None,
        retention_deadline_us: 9_999,
        audit_event: drop_audit,
    })?;
    let empty_drop_invocation = id(0x58);
    let mut empty_drop_audit = identity_audit_event(0x59, actor, id(0x5a));
    empty_drop_audit.correlation_id = empty_drop_invocation;
    assert_eq!(
        storage.alter_column_default(prdb_engine::storage_manager::AlterColumnDefaultRequest {
            invocation_id: empty_drop_invocation,
            authenticated_actor_id: actor,
            table_id: table.table_id,
            column_id: table.column_ids[0],
            default: None,
            retention_deadline_us: 9_999,
            audit_event: empty_drop_audit,
        }),
        Err(prdb_engine::storage_manager::TableLifecycleError::InvalidRequest)
    );
    Ok(())
}

#[test]
fn storage_manager_adds_column_and_backfills_existing_rows()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        AddTableColumnRequest, CreateTableRequest, TableColumnDefault, TableColumnDefinition,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x5d);
    let admin_role = builtin_owner_id();
    let setup = vec![
        principal_write_with_name(actor, 1, 1, "add_column_owner"),
        object_write_with_name(actor, 8, "add_column_owner"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ];
    let mut setup_audit = identity_audit_event(0x5e, admin_role, id(0x5f));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x5f), admin_role, 9_999, &setup, &[setup_audit])?;
    let schema_invocation = id(0x60);
    let mut schema_audit = identity_audit_event(0x61, actor, id(0x62));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "add_column_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x63);
    let mut create_audit = identity_audit_event(0x64, actor, id(0x65));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "add_column_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "number".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let row_id = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x66),
    };
    let mut insert = storage.begin_invocation(id(0x67), actor, 9_999, 2, 1024)?;
    let old_schema = insert.load_catalog_table_schema(table.table_id)?;
    insert.load_catalog_indexes(&old_schema)?;
    let old_tuple = prdb_engine::tuple::encode_tuple(
        &old_schema.columns,
        old_schema.schema_version,
        &[Some(ScalarValue::Integer(7))],
    )?;
    insert.stage_put(row_id, &old_tuple)?;
    insert.commit()?;

    let index_id = id(0x6b);
    let (index_key, index_payload) =
        index_definition_with_columns(index_id, table.table_id, &[table.column_ids[0]]);
    let index_invocation = id(0x6c);
    let mut index_audit = identity_audit_event(0x6d, actor, index_id);
    index_audit.correlation_id = index_invocation;
    storage.commit_authorized_index_ddl(
        index_invocation,
        actor,
        9_999,
        &[
            CatalogWrite {
                key: index_key.clone(),
                expected_value: None,
                payload: Some(index_payload),
            },
            object_write(index_id, 7),
        ],
        &[index_audit],
    )?;

    let stale_writer_invocation = id(0x71);
    let stale_row_id = prdb_engine::overlay::RowId {
        table: table.table_id,
        row: id(0x72),
    };
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, actor, 9_999, 2, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(table.table_id)?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = prdb_engine::tuple::encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Integer(99))],
    )?;
    stale_writer.stage_put(stale_row_id, &stale_tuple)?;

    let add_invocation = id(0x68);
    let mut add_audit = identity_audit_event(0x69, actor, id(0x6a));
    add_audit.correlation_id = add_invocation;
    let added_result = storage.add_table_column(AddTableColumnRequest {
        invocation_id: add_invocation,
        authenticated_actor_id: actor,
        table_id: table.table_id,
        column: TableColumnDefinition {
            name: "label".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: false,
            generated_uuid_identity: false,
            default: Some(TableColumnDefault::Literal(Some(ScalarValue::Text(
                "ready".to_owned(),
            )))),
        },
        retention_deadline_us: 9_999,
        audit_event: add_audit,
    });
    assert!(
        added_result.is_ok(),
        "adding column failed: {added_result:?}"
    );
    let added = added_result?;
    assert_eq!(added.schema_version, 2);
    let stale_result = stale_writer.commit();
    assert!(
        matches!(
            stale_result,
            Err(prdb_engine::transaction::TransactionError::Commit(
                CommitError::Conflict
            ))
        ),
        "stale writer returned {stale_result:?}"
    );
    assert!(row(&database, &table_row_key(table.table_id, stale_row_id.row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, actor, 1)?
            .ok_or("missing stale writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let stored = row(&database, &table_row_key(table.table_id, row_id.row))?
        .ok_or("backfilled row is missing")?;
    let tuple = prdb_engine::tuple::decode_tuple(
        &[
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        ],
        added.schema_version,
        &stored.payload,
    )?;
    assert_eq!(
        tuple.values,
        vec![
            Some(ScalarValue::Integer(7)),
            Some(ScalarValue::Text("ready".to_owned())),
        ]
    );
    let added_schema_ids = [table.column_ids[0], added.column_id];
    let added_schema_columns = [
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
    ];
    let indexed_key = prdb_engine::index_key::derive_index_key(
        index_id,
        row_id.row,
        added.schema_version,
        &added_schema_ids,
        &added_schema_columns,
        &stored.payload,
        &[table.column_ids[0]],
    )?;
    let read = database.begin_read()?;
    let index_record = read
        .open_table(CATALOG)?
        .get(index_key.as_slice())?
        .ok_or("index catalog record disappeared")?;
    assert_eq!(
        u64::from_le_bytes(
            decode_registered_envelope(index_record.value())?
                .fields
                .iter()
                .find(|field| field.tag == 10)
                .ok_or("index schema version is missing")?
                .payload
                .as_slice()
                .try_into()?
        ),
        added.schema_version
    );
    let locator = read
        .open_table(schema::INDEXES)?
        .get(indexed_key.key.as_slice())?
        .ok_or("rewritten index entry is missing")?;
    assert_eq!(
        IndexLocator::decode_for_key(&indexed_key.key, locator.value())?.row_version,
        stored.row_version
    );

    let nullable_add_invocation = id(0x6e);
    let mut nullable_add_audit = identity_audit_event(0x6f, actor, id(0x70));
    nullable_add_audit.correlation_id = nullable_add_invocation;
    let nullable_added = storage.add_table_column(AddTableColumnRequest {
        invocation_id: nullable_add_invocation,
        authenticated_actor_id: actor,
        table_id: table.table_id,
        column: TableColumnDefinition {
            name: "comment".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        },
        retention_deadline_us: 9_999,
        audit_event: nullable_add_audit,
    })?;
    assert_eq!(nullable_added.schema_version, 3);
    let stored = row(&database, &table_row_key(table.table_id, row_id.row))?
        .ok_or("row backfilled with NULL is missing")?;
    let nullable_tuple = prdb_engine::tuple::decode_tuple(
        &[
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: false,
            },
            prdb_engine::tuple::ColumnSchema {
                scalar_type: ScalarType::Text,
                nullable: true,
            },
        ],
        nullable_added.schema_version,
        &stored.payload,
    )?;
    assert_eq!(
        nullable_tuple.values,
        vec![
            Some(ScalarValue::Integer(7)),
            Some(ScalarValue::Text("ready".to_owned())),
            None,
        ]
    );
    let nullable_schema_ids = [
        table.column_ids[0],
        added.column_id,
        nullable_added.column_id,
    ];
    let nullable_schema_columns = [
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        prdb_engine::tuple::ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
    ];
    let nullable_indexed_key = prdb_engine::index_key::derive_index_key(
        index_id,
        row_id.row,
        nullable_added.schema_version,
        &nullable_schema_ids,
        &nullable_schema_columns,
        &stored.payload,
        &[table.column_ids[0]],
    )?;
    let read = database.begin_read()?;
    let index_record = read
        .open_table(CATALOG)?
        .get(index_key.as_slice())?
        .ok_or("index catalog record disappeared after NULL backfill")?;
    assert_eq!(
        u64::from_le_bytes(
            decode_registered_envelope(index_record.value())?
                .fields
                .iter()
                .find(|field| field.tag == 10)
                .ok_or("index schema version is missing after NULL backfill")?
                .payload
                .as_slice()
                .try_into()?
        ),
        nullable_added.schema_version
    );
    let locator = read
        .open_table(schema::INDEXES)?
        .get(nullable_indexed_key.key.as_slice())?
        .ok_or("rewritten index entry is missing after NULL backfill")?;
    assert_eq!(
        IndexLocator::decode_for_key(&nullable_indexed_key.key, locator.value())?.row_version,
        stored.row_version
    );
    Ok(())
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
    } else if kind == 1 {
        format!("schema_{}", object_id[15])
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
    let stale_check_invocation = id(0xb7);
    let stale_check_row = id(0xb8);
    let mut stale_check_writer =
        storage.begin_invocation(stale_check_invocation, admin, 9_999, 1, 1024)?;
    let stale_check_schema = stale_check_writer.load_catalog_table_schema(table_id)?;
    stale_check_writer.load_catalog_indexes(&stale_check_schema)?;
    let stale_check_tuple = encode_tuple(
        &stale_check_schema.columns,
        stale_check_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"pinned before CHECK".to_vec()))],
    )?;
    stale_check_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: table_id,
            row: stale_check_row,
        },
        &stale_check_tuple,
    )?;
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
    assert!(matches!(
        stale_check_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(table_id, stale_check_row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_check_invocation, admin, 1)?
            .ok_or("missing stale CHECK constraint writer outcome")?
            .status,
        OutcomeStatus::Aborted
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
fn storage_manager_drop_table_restricts_external_foreign_key_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xb4);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "external_fk_drop_admin"),
        object_write_with_name(actor, 8, "external_fk_drop_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xb0, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb1), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let before_drop = commit_sequence(&database)?;
    let drop_invocation = id(0xb2);
    let mut drop_audit = identity_audit_event(0xb3, actor, id(81));
    drop_audit.correlation_id = drop_invocation;
    assert_eq!(
        storage.drop_table(drop_invocation, actor, id(81), 9_999, drop_audit),
        Err(TableLifecycleError::Commit(
            CommitError::TableHasDependencies
        ))
    );

    assert_eq!(commit_sequence(&database)?, before_drop);
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing rejected table-drop outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(5, id(81)),
        id_catalog_key(5, id(82)),
        id_catalog_key(8, id(85)),
        id_catalog_key(8, id(86)),
        dependency_write(id(86), id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_some(),
            "record removed: {key:?}"
        );
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn foreign_key_creation_racing_referenced_table_drop_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xb5);
    let foreign_key_key = id_catalog_key(8, id(86));
    let foreign_key_object_key = id_catalog_key(2, id(86));
    let target_edge = dependency_write(id(86), id(85), 3);
    let target_edge_key = target_edge.key.clone();
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != foreign_key_key
                && write.key != foreign_key_object_key
                && write.key != target_edge.key
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "foreign_key_race_admin"),
        object_write_with_name(actor, 8, "foreign_key_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xb6, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb7), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut foreign_key_definition = foreign_key_catalog_writes(true, true)
        .into_iter()
        .find(|write| write.key == foreign_key_key)
        .ok_or("missing foreign-key definition")?;
    foreign_key_definition.expected_value = None;
    let foreign_key_object = object_write(id(86), 6);
    let mut source_table_update = table_write_with_versions(id(82), id(80), 1, 2);
    source_table_update.expected_value = table_write(id(82), id(80)).payload;
    let mut target_table_update = table_write_with_versions(id(81), id(80), 1, 2);
    target_table_update.expected_value = table_write(id(81), id(80)).payload;
    let fk_invocation = id(0xb8);
    let drop_invocation = id(0xb9);
    let barrier = Arc::new(Barrier::new(3));
    let (fk_result, drop_result) = std::thread::scope(|scope| {
        let fk_barrier = barrier.clone();
        let fk_storage = &storage;
        let fk = scope.spawn(move || {
            let mut audit = identity_audit_event(0xba, actor, id(86));
            audit.correlation_id = fk_invocation;
            fk_barrier.wait();
            fk_storage.commit_authorized_foreign_key_ddl(
                fk_invocation,
                actor,
                9_999,
                &[
                    foreign_key_definition,
                    foreign_key_object,
                    target_edge,
                    source_table_update,
                    target_table_update,
                ],
                &[audit],
            )
        });
        let drop_barrier = barrier.clone();
        let drop_storage = &storage;
        let drop = scope.spawn(move || {
            let mut audit = identity_audit_event(0xbb, actor, id(81));
            audit.correlation_id = drop_invocation;
            drop_barrier.wait();
            drop_storage.drop_table(drop_invocation, actor, id(81), 9_999, audit)
        });
        barrier.wait();
        (fk.join().unwrap(), drop.join().unwrap())
    });

    let (fk_status, drop_status) = match (fk_result, drop_result) {
        (Ok(_), Err(TableLifecycleError::Commit(CommitError::TableHasDependencies))) => {
            (OutcomeStatus::Committed, OutcomeStatus::Aborted)
        }
        (
            Err(
                CommitError::Conflict
                | CommitError::InvalidCatalog
                | CommitError::DdlAuthorizationDenied,
            ),
            Ok(_),
        ) => (OutcomeStatus::Aborted, OutcomeStatus::Committed),
        (fk, drop) => panic!("unexpected FK-create/table-drop race: {fk:?} / {drop:?}"),
    };
    assert_eq!(commit_sequence(&database)?, 2);
    assert_eq!(
        lookup_outcome(&database, fk_invocation, actor, 1)?
            .ok_or("missing FK race outcome")?
            .status,
        fk_status
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing table-drop race outcome")?
            .status,
        drop_status
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    if drop_status == OutcomeStatus::Committed {
        assert!(catalog.get(id_catalog_key(5, id(81)).as_slice())?.is_none());
        assert!(catalog.get(foreign_key_key.as_slice())?.is_none());
        assert!(catalog.get(target_edge_key.as_slice())?.is_none());
        assert!(catalog.get(id_catalog_key(5, id(82)).as_slice())?.is_some());
    } else {
        assert!(catalog.get(id_catalog_key(5, id(81)).as_slice())?.is_some());
        assert!(catalog.get(foreign_key_key.as_slice())?.is_some());
        assert!(catalog.get(target_edge_key.as_slice())?.is_some());
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn foreign_key_creation_racing_referenced_primary_key_drop_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xb0);
    let foreign_key_key = id_catalog_key(8, id(86));
    let foreign_key_object_key = id_catalog_key(2, id(86));
    let target_edge = dependency_write(id(86), id(85), 3);
    let target_edge_key = target_edge.key.clone();
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| write.key != foreign_key_key && write.key != foreign_key_object_key)
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_pk_drop_race_admin"),
        object_write_with_name(actor, 8, "fk_pk_drop_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0xb1, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xb1), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut foreign_key_definition = foreign_key_catalog_writes(true, true)
        .into_iter()
        .find(|write| write.key == foreign_key_key)
        .ok_or("missing foreign-key definition")?;
    foreign_key_definition.expected_value = None;
    let foreign_key_object = object_write(id(86), 6);
    let mut source_table_update = table_write_with_versions(id(82), id(80), 1, 2);
    source_table_update.expected_value = table_write(id(82), id(80)).payload;
    let mut target_table_update = table_write_with_versions(id(81), id(80), 1, 2);
    target_table_update.expected_value = table_write(id(81), id(80)).payload;
    let fk_invocation = id(0xb2);
    let drop_invocation = id(0xb3);
    let barrier = Arc::new(Barrier::new(3));
    let (fk_result, drop_result) = std::thread::scope(|scope| {
        let fk_barrier = barrier.clone();
        let fk_storage = &storage;
        let fk = scope.spawn(move || {
            let mut audit = identity_audit_event(0xb4, actor, id(86));
            audit.correlation_id = fk_invocation;
            fk_barrier.wait();
            fk_storage.commit_authorized_foreign_key_ddl(
                fk_invocation,
                actor,
                9_999,
                &[
                    foreign_key_definition,
                    foreign_key_object,
                    target_edge,
                    source_table_update,
                    target_table_update,
                ],
                &[audit],
            )
        });
        let drop_barrier = barrier.clone();
        let drop_storage = &storage;
        let drop = scope.spawn(move || {
            let mut audit = identity_audit_event(0xb5, actor, id(85));
            audit.correlation_id = drop_invocation;
            drop_barrier.wait();
            drop_storage.drop_primary_key(drop_invocation, actor, id(85), 9_999, audit)
        });
        barrier.wait();
        (fk.join().unwrap(), drop.join().unwrap())
    });

    let (fk_status, drop_status) = match (fk_result, drop_result) {
        (
            Ok(_),
            Err(TableLifecycleError::Commit(CommitError::InvalidCatalog | CommitError::Conflict)),
        ) => (OutcomeStatus::Committed, OutcomeStatus::Aborted),
        (Err(CommitError::InvalidCatalog | CommitError::Conflict), Ok(_)) => {
            (OutcomeStatus::Aborted, OutcomeStatus::Committed)
        }
        (fk, drop) => panic!("unexpected FK-create/PRIMARY KEY-drop race: {fk:?} / {drop:?}"),
    };
    assert_eq!(commit_sequence(&database)?, 2);
    assert_eq!(
        lookup_outcome(&database, fk_invocation, actor, 1)?
            .ok_or("missing FK/key-drop race outcome")?
            .status,
        fk_status
    );
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing key-drop race outcome")?
            .status,
        drop_status
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    if fk_status == OutcomeStatus::Committed {
        for key in [
            id_catalog_key(8, id(85)),
            id_catalog_key(9, id(87)),
            foreign_key_key.clone(),
            target_edge_key,
        ] {
            assert!(catalog.get(key.as_slice())?.is_some(), "missing {key:?}");
        }
    } else {
        for key in [id_catalog_key(8, id(85)), id_catalog_key(9, id(87))] {
            assert!(catalog.get(key.as_slice())?.is_none(), "unexpected {key:?}");
        }
        assert!(catalog.get(foreign_key_key.as_slice())?.is_none());
        assert!(catalog.get(target_edge_key.as_slice())?.is_none());
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn foreign_key_removal_racing_referenced_table_drop_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xf1);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "foreign_key_remove_race_admin"),
        object_write_with_name(actor, 8, "foreign_key_remove_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xf2, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xf3), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let fk_invocation = id(0xf4);
    let table_invocation = id(0xf5);
    let barrier = Arc::new(Barrier::new(3));
    let (fk_result, table_result) = std::thread::scope(|scope| {
        let fk_barrier = barrier.clone();
        let fk_storage = &storage;
        let fk = scope.spawn(move || {
            let mut audit = identity_audit_event(0xf6, actor, id(86));
            audit.correlation_id = fk_invocation;
            fk_barrier.wait();
            fk_storage.drop_foreign_key(fk_invocation, actor, id(86), 9_999, audit)
        });
        let table_barrier = barrier.clone();
        let table_storage = &storage;
        let table = scope.spawn(move || {
            let mut audit = identity_audit_event(0xf7, actor, id(81));
            audit.correlation_id = table_invocation;
            table_barrier.wait();
            table_storage.drop_table(table_invocation, actor, id(81), 9_999, audit)
        });
        barrier.wait();
        (fk.join().unwrap(), table.join().unwrap())
    });

    assert!(fk_result.is_ok(), "FK drop failed: {fk_result:?}");
    let table_status = match table_result {
        Ok(dropped) => {
            assert_eq!(dropped.table_id, id(81));
            OutcomeStatus::Committed
        }
        Err(TableLifecycleError::Commit(CommitError::TableHasDependencies)) => {
            OutcomeStatus::Aborted
        }
        Err(TableLifecycleError::Commit(CommitError::Conflict)) => OutcomeStatus::Aborted,
        other => return Err(format!("unexpected target-table drop result: {other:?}").into()),
    };
    let fk_outcome = lookup_outcome(&database, fk_invocation, actor, 1)?
        .ok_or("missing FK-removal race outcome")?;
    assert_eq!(fk_outcome.status, OutcomeStatus::Committed);
    let table_outcome = lookup_outcome(&database, table_invocation, actor, 1)?
        .ok_or("missing table-drop race outcome")?;
    assert_eq!(table_outcome.status, table_status);
    assert_eq!(
        commit_sequence(&database)?,
        1 + u64::from(table_status == OutcomeStatus::Committed) + 1
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(id_catalog_key(8, id(86)).as_slice())?.is_none());
    assert_eq!(
        catalog.get(id_catalog_key(5, id(81)).as_slice())?.is_some(),
        table_status != OutcomeStatus::Committed
    );
    assert!(catalog.get(id_catalog_key(5, id(82)).as_slice())?.is_some());
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_drops_foreign_key_and_advances_both_table_epochs()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xcc);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "foreign_key_drop_admin"),
        object_write_with_name(actor, 8, "foreign_key_drop_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xcd, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xce), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut stale_source_writer = storage.begin_invocation(id(0xd1), actor, 9_999, 1, 1024)?;
    let stale_source_schema = stale_source_writer.load_catalog_table_schema(id(82))?;
    stale_source_writer.load_catalog_indexes(&stale_source_schema)?;
    let stale_source_tuple = encode_tuple(
        &stale_source_schema.columns,
        stale_source_schema.schema_version,
        &[None],
    )?;
    stale_source_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xd2),
        },
        &stale_source_tuple,
    )?;
    let mut stale_target_writer = storage.begin_invocation(id(0xd3), actor, 9_999, 1, 1024)?;
    let stale_target_schema = stale_target_writer.load_catalog_table_schema(id(81))?;
    stale_target_writer.load_catalog_indexes(&stale_target_schema)?;
    let stale_target_tuple = encode_tuple(
        &stale_target_schema.columns,
        stale_target_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"staged before FK drop".to_vec()))],
    )?;
    stale_target_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(81),
            row: id(0xd4),
        },
        &stale_target_tuple,
    )?;

    let invocation_id = id(0xcf);
    let mut audit = identity_audit_event(0xd0, actor, id(86));
    audit.correlation_id = invocation_id;
    assert_eq!(
        storage.drop_foreign_key(invocation_id, actor, id(86), 9_999, audit)?,
        2
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing foreign-key drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(matches!(
        stale_source_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(matches!(
        stale_target_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, id(86)),
        id_catalog_key(2, id(86)),
        dependency_write(id(86), id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "record remains: {key:?}"
        );
    }
    for table_id in [id(81), id(82)] {
        let key = id_catalog_key(5, table_id);
        let bytes = catalog
            .get(key.as_slice())?
            .ok_or("participating table disappeared during FK drop")?
            .value()
            .to_vec();
        assert_eq!(
            u64::from_le_bytes(
                decode_registered_envelope(&bytes)?
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("missing table write epoch")?
                    .payload
                    .as_slice()
                    .try_into()?
            ),
            2
        );
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_drops_self_referential_foreign_key_once_per_table()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe1);
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        constraint_write(id(86), id(81), 5, &[id(83)], Some(id(85)), &[id(83)], None),
        object_write(id(86), 6),
        dependency_write(id(86), id(85), 3),
        principal_write_with_name(actor, 1, 1, "self_fk_remove_admin"),
        object_write_with_name(actor, 8, "self_fk_remove_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xe2, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xe2), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let invocation_id = id(0xe3);
    let mut audit = identity_audit_event(0xe4, actor, id(86));
    audit.correlation_id = invocation_id;
    assert_eq!(
        storage.drop_foreign_key(invocation_id, actor, id(86), 9_999, audit)?,
        2
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(catalog.get(id_catalog_key(8, id(86)).as_slice())?.is_none());
    let table = catalog
        .get(id_catalog_key(5, id(81)).as_slice())?
        .ok_or("self-referencing table disappeared during FK drop")?
        .value()
        .to_vec();
    let epoch = decode_registered_envelope(&table)?
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .ok_or("missing self-referencing table write epoch")?
        .payload
        .as_slice()
        .try_into()?;
    assert_eq!(u64::from_le_bytes(epoch), 2);
    let other_table = catalog
        .get(id_catalog_key(5, id(82)).as_slice())?
        .ok_or("unrelated table disappeared during FK drop")?
        .value()
        .to_vec();
    let other_epoch = decode_registered_envelope(&other_table)?
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .ok_or("missing unrelated table write epoch")?
        .payload
        .as_slice()
        .try_into()?;
    assert_eq!(u64::from_le_bytes(other_epoch), 1);
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_creates_foreign_key_and_advances_both_table_epochs()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe5);
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86)) && write.key != id_catalog_key(2, id(86))
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_create_admin"),
        object_write_with_name(actor, 8, "fk_create_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0xe6, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xe6), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let invocation_id = id(0xe7);
    let mut audit = identity_audit_event(0xe8, actor, id(82));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_foreign_key(prdb_engine::storage_manager::CreateForeignKeyRequest {
            invocation_id,
            authenticated_actor_id: actor,
            source_table_id: id(82),
            source_column_ids: vec![id(84)],
            target_constraint_id: id(85),
            retention_deadline_us: 9_999,
            audit_event: audit,
        })
        .map_err(|error| format!("foreign-key create failed: {error:?}"))?;
    assert_ne!(created.foreign_key_id, [0; 16]);
    assert_eq!(created.commit_sequence, 2);
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing foreign-key create outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, created.foreign_key_id),
        id_catalog_key(2, created.foreign_key_id),
        dependency_write(created.foreign_key_id, id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_some(),
            "missing record: {key:?}"
        );
    }
    for table_id in [id(81), id(82)] {
        let key = id_catalog_key(5, table_id);
        let bytes = catalog
            .get(key.as_slice())?
            .ok_or("participating table disappeared during FK create")?
            .value()
            .to_vec();
        let epoch = decode_registered_envelope(&bytes)?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("missing participating table write epoch")?
            .payload
            .as_slice()
            .try_into()?;
        assert_eq!(u64::from_le_bytes(epoch), 2);
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn foreign_key_creation_racing_orphan_child_insert_is_atomic()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateForeignKeyRequest, TableLifecycleError};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe9);
    let fk_edge = dependency_write(id(86), id(85), 3).key;
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != fk_edge
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "fk_insert_race_admin"),
        object_write_with_name(actor, 8, "fk_insert_race_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
        grant_write(actor, id(81), 6, 1, id(81), admin_role),
        dependency_write(actor, id(81), 5),
    ]);
    let mut setup_audit = identity_audit_event(0xea, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xea), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let insert_invocation = id(0xeb);
    let insert_row = id(0xec);
    let mut insert = storage.begin_invocation(insert_invocation, actor, 9_999, 1, 1024)?;
    let source_schema = insert.load_catalog_table_schema(id(82))?;
    insert.load_catalog_indexes(&source_schema)?;
    let orphan = encode_tuple(
        &source_schema.columns,
        source_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"missing parent".to_vec()))],
    )?;
    insert.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: insert_row,
        },
        &orphan,
    )?;

    let fk_invocation = id(0xed);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (insert_result, fk_result) = std::thread::scope(|scope| {
        let insert_barrier = barrier.clone();
        let insert_thread = scope.spawn(move || {
            insert_barrier.wait();
            insert.commit()
        });
        let fk_barrier = barrier.clone();
        let fk_storage = &storage;
        let fk_thread = scope.spawn(move || {
            let mut audit = identity_audit_event(0xee, actor, id(82));
            audit.correlation_id = fk_invocation;
            fk_barrier.wait();
            fk_storage.create_foreign_key(CreateForeignKeyRequest {
                invocation_id: fk_invocation,
                authenticated_actor_id: actor,
                source_table_id: id(82),
                source_column_ids: vec![id(84)],
                target_constraint_id: id(85),
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        barrier.wait();
        (
            insert_thread
                .join()
                .expect("FK race insert thread panicked"),
            fk_thread.join().expect("FK race DDL thread panicked"),
        )
    });

    let (insert_status, fk_status, created_fk) = match (insert_result, fk_result) {
        (Ok(_), Err(TableLifecycleError::Commit(CommitError::ForeignKeyViolation))) => {
            (OutcomeStatus::Committed, OutcomeStatus::Aborted, None)
        }
        (
            Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)),
            Ok(created),
        ) => (
            OutcomeStatus::Aborted,
            OutcomeStatus::Committed,
            Some(created.foreign_key_id),
        ),
        (insert, fk) => panic!("unexpected FK-create/orphan-insert race: {insert:?} / {fk:?}"),
    };
    assert_eq!(commit_sequence(&database)?, before_race + 1);
    assert_eq!(
        lookup_outcome(&database, insert_invocation, actor, 1)?
            .ok_or("missing FK-race insert outcome")?
            .status,
        insert_status
    );
    assert_eq!(
        lookup_outcome(&database, fk_invocation, actor, 1)?
            .ok_or("missing FK-race DDL outcome")?
            .status,
        fk_status
    );
    assert_eq!(
        row(&database, &table_row_key(id(82), insert_row))?.is_some(),
        insert_status == OutcomeStatus::Committed
    );
    if let Some(foreign_key_id) = created_fk {
        let read = database.begin_read()?;
        assert!(
            read.open_table(CATALOG)?
                .get(id_catalog_key(8, foreign_key_id).as_slice())?
                .is_some()
        );
        drop(read);
        let drop_invocation = id(0xef);
        let mut drop_audit = identity_audit_event(0xf0, actor, foreign_key_id);
        drop_audit.correlation_id = drop_invocation;
        storage.drop_foreign_key(drop_invocation, actor, foreign_key_id, 9_999, drop_audit)?;
    }
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_creates_named_unique_index_and_backfills_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xd0);
    let foreign_key_edge = dependency_write(id(86), id(85), 3).key;
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != foreign_key_edge
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "named_index_admin"),
        object_write_with_name(actor, 8, "named_index_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xd1, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xd1), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut seed = storage.begin_invocation(id(0xd2), actor, 9_999, 2, 2048)?;
    let schema = seed.load_catalog_table_schema(id(82))?;
    seed.load_catalog_indexes(&schema)?;
    for (row_id, value) in [(id(0xd3), b"one".as_slice()), (id(0xd4), b"two".as_slice())] {
        let tuple = encode_tuple(
            &schema.columns,
            schema.schema_version,
            &[Some(ScalarValue::Bytea(value.to_vec()))],
        )?;
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: id(82),
                row: row_id,
            },
            &tuple,
        )?;
    }
    assert_eq!(seed.commit()?, 2);

    let stale_invocation = id(0xd5);
    let stale_row = id(0xd6);
    let mut stale_writer = storage.begin_invocation(stale_invocation, actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(82))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"three".to_vec()))],
    )?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: stale_row,
        },
        &stale_tuple,
    )?;

    let invocation_id = id(0xd7);
    let mut audit = identity_audit_event(0xd8, actor, id(82));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_index(prdb_engine::storage_manager::CreateIndexRequest {
            invocation_id,
            authenticated_actor_id: actor,
            table_id: id(82),
            name: "source_value_unique".to_owned(),
            column_ids: vec![id(84)],
            unique: true,
            retention_deadline_us: 9_999,
            audit_event: audit,
        })
        .map_err(|error| format!("named unique index creation failed: {error:?}"))?;
    assert_ne!(created.index_id, [0; 16]);
    assert_eq!(created.commit_sequence, 3);
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), stale_row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_invocation, actor, 1)?
            .ok_or("missing stale named-index writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing named-index creation outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let index_object = decode_registered_envelope(
        catalog
            .get(id_catalog_key(2, created.index_id).as_slice())?
            .ok_or("missing index object")?
            .value(),
    )?;
    assert!(
        index_object
            .fields
            .iter()
            .any(|field| { field.tag == 3 && field.payload.as_slice() == b"source_value_unique" })
    );
    assert!(
        index_object
            .fields
            .iter()
            .any(|field| field.tag == 2 && field.payload.as_slice() == id(80))
    );
    assert!(
        catalog
            .get(dependency_write(created.index_id, id(80), 1).key.as_slice())?
            .is_some()
    );
    let index_record = catalog
        .get(id_catalog_key(9, created.index_id).as_slice())?
        .ok_or("missing index definition")?;
    assert!(
        decode_registered_envelope(index_record.value())?
            .fields
            .iter()
            .any(|field| field.tag == 4 && field.payload.as_slice() == [1])
    );
    assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 2);
    assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 2);
    drop(index_record);
    drop(catalog);
    drop(read);
    let drop_invocation = id(0xd9);
    let mut drop_audit = identity_audit_event(0xda, actor, created.index_id);
    drop_audit.correlation_id = drop_invocation;
    let dropped = storage
        .drop_index(prdb_engine::storage_manager::DropIndexRequest {
            invocation_id: drop_invocation,
            authenticated_actor_id: actor,
            index_id: created.index_id,
            retention_deadline_us: 9_999,
            audit_event: drop_audit,
        })
        .map_err(|error| format!("named unique index drop failed: {error:?}"))?;
    assert_eq!(dropped.index_id, created.index_id);
    assert_eq!(dropped.commit_sequence, 4);
    assert_eq!(
        lookup_outcome(&database, drop_invocation, actor, 1)?
            .ok_or("missing named-index drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(2, created.index_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(dependency_write(created.index_id, id(80), 1).key.as_slice())?
            .is_none()
    );
    assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 0);
    assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 0);
    drop(catalog);
    drop(read);

    let stale_policy_writer_invocation = id(0xdb);
    let stale_policy_row = id(0xdc);
    let mut stale_policy_writer =
        storage.begin_invocation(stale_policy_writer_invocation, actor, 9_999, 1, 1024)?;
    let stale_policy_schema = stale_policy_writer.load_catalog_table_schema(id(82))?;
    stale_policy_writer.load_catalog_indexes(&stale_policy_schema)?;
    let stale_policy_tuple = encode_tuple(
        &stale_policy_schema.columns,
        stale_policy_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"staged before policy creation".to_vec(),
        ))],
    )?;
    stale_policy_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: stale_policy_row,
        },
        &stale_policy_tuple,
    )?;
    let policy_invocation = id(0xdd);
    let mut policy_audit = identity_audit_event(0xde, actor, id(82));
    policy_audit.correlation_id = policy_invocation;
    let policy = storage
        .create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
            invocation_id: policy_invocation,
            authenticated_actor_id: actor,
            table_id: id(82),
            name: "fresh_row_policy".to_owned(),
            kind: prdb_engine::storage_manager::PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients: vec![prdb_engine::authorization::Subject::Principal(actor)],
            target_column_ids: Vec::new(),
            using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                ScalarValue::Boolean(true),
            ))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: policy_audit,
        })
        .map_err(|error| format!("row policy creation failed: {error:?}"))?;
    assert_eq!(policy.commit_sequence, 5);
    assert!(matches!(
        stale_policy_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), stale_policy_row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_policy_writer_invocation, actor, 1)?
            .ok_or("missing stale policy writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        lookup_outcome(&database, policy_invocation, actor, 1)?
            .ok_or("missing policy creation outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let racing_writer_invocation = id(0xdf);
    let racing_row = id(0xe0);
    let mut racing_writer =
        storage.begin_invocation(racing_writer_invocation, actor, 9_999, 1, 1024)?;
    let racing_schema = racing_writer.load_catalog_table_schema(id(82))?;
    racing_writer.load_catalog_indexes(&racing_schema)?;
    let racing_tuple = encode_tuple(
        &racing_schema.columns,
        racing_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"raced policy creation".to_vec()))],
    )?;
    racing_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: racing_row,
        },
        &racing_tuple,
    )?;
    let racing_policy_invocation = id(0xe1);
    let before_policy_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (writer_result, policy_result) = std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            racing_writer.commit()
        });
        let policy_barrier = barrier.clone();
        let policy_storage = &storage;
        let policy = scope.spawn(move || {
            let mut audit = identity_audit_event(0xe2, actor, id(82));
            audit.correlation_id = racing_policy_invocation;
            policy_barrier.wait();
            policy_storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
                invocation_id: racing_policy_invocation,
                authenticated_actor_id: actor,
                table_id: id(82),
                name: "racing_row_policy".to_owned(),
                kind: prdb_engine::storage_manager::PolicyKind::Row,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Allow,
                recipients: vec![prdb_engine::authorization::Subject::Principal(actor)],
                target_column_ids: Vec::new(),
                using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(true),
                ))),
                with_check_expression: None,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        barrier.wait();
        (
            writer.join().expect("racing writer panicked"),
            policy.join().expect("racing policy DDL panicked"),
        )
    });
    let created_racing_policy =
        policy_result.map_err(|error| format!("racing row policy creation failed: {error:?}"))?;
    let writer_sequence = match writer_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    let writer_status = if writer_sequence.is_some() {
        OutcomeStatus::Committed
    } else {
        OutcomeStatus::Aborted
    };
    assert_eq!(
        row(&database, &table_row_key(id(82), racing_row))?.is_some(),
        writer_sequence.is_some()
    );
    assert_eq!(
        lookup_outcome(&database, racing_writer_invocation, actor, 1)?
            .ok_or("missing racing writer outcome")?
            .status,
        writer_status
    );
    assert_eq!(
        lookup_outcome(&database, racing_policy_invocation, actor, 1)?
            .ok_or("missing racing policy outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert_eq!(
        created_racing_policy.commit_sequence,
        before_policy_race + 1 + u64::from(writer_sequence.is_some())
    );
    assert_eq!(
        commit_sequence(&database)?,
        created_racing_policy.commit_sequence
    );
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_creates_unique_constraint_and_backfills_existing_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe9);
    let foreign_key_edge = dependency_write(id(86), id(85), 3).key;
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != foreign_key_edge
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "unique_create_admin"),
        object_write_with_name(actor, 8, "unique_create_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xea, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xea), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut insert = storage.begin_invocation(id(0xeb), actor, 9_999, 2, 2048)?;
    let schema = insert.load_catalog_table_schema(id(82))?;
    insert.load_catalog_indexes(&schema)?;
    for (row_id, value) in [
        (id(0xec), b"first".as_slice()),
        (id(0xed), b"second".as_slice()),
    ] {
        let tuple = encode_tuple(
            &schema.columns,
            schema.schema_version,
            &[Some(ScalarValue::Bytea(value.to_vec()))],
        )?;
        insert.stage_put(
            prdb_engine::overlay::RowId {
                table: id(82),
                row: row_id,
            },
            &tuple,
        )?;
    }
    assert_eq!(insert.commit()?, 2);

    let mut stale_writer = storage.begin_invocation(id(0xee), actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(82))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"third".to_vec()))],
    )?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xef),
        },
        &stale_tuple,
    )?;

    let invocation_id = id(0xf0);
    let mut audit = identity_audit_event(0xf1, actor, id(82));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_unique_constraint(
            prdb_engine::storage_manager::CreateUniqueConstraintRequest {
                invocation_id,
                authenticated_actor_id: actor,
                table_id: id(82),
                column_ids: vec![id(84)],
                retention_deadline_us: 9_999,
                audit_event: audit,
            },
        )
        .map_err(|error| format!("UNIQUE constraint creation failed: {error:?}"))?;
    assert_eq!(created.commit_sequence, 3);
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, id(0xee), actor, 1)?
            .ok_or("missing stale UNIQUE writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(8, created.constraint_id).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(id_catalog_key(2, created.constraint_id).as_slice())?
            .is_some()
    );
    assert!(
        catalog
            .get(id_catalog_key(2, created.index_id).as_slice())?
            .is_some()
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    let mut stale_drop_writer = storage.begin_invocation(id(0xe6), actor, 9_999, 1, 1024)?;
    let stale_drop_schema = stale_drop_writer.load_catalog_table_schema(id(82))?;
    stale_drop_writer.load_catalog_indexes(&stale_drop_schema)?;
    let stale_drop_tuple = encode_tuple(
        &stale_drop_schema.columns,
        stale_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"stale after UNIQUE drop".to_vec(),
        ))],
    )?;
    stale_drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xe7),
        },
        &stale_drop_tuple,
    )?;
    let drop_invocation_id = id(0xe2);
    let mut drop_audit = identity_audit_event(0xe3, actor, created.constraint_id);
    drop_audit.correlation_id = drop_invocation_id;
    assert_eq!(
        storage
            .drop_unique_constraint(
                drop_invocation_id,
                actor,
                created.constraint_id,
                9_999,
                drop_audit,
            )
            .map_err(|error| format!("UNIQUE drop failed: {error:?}"))?,
        4
    );
    assert!(matches!(
        stale_drop_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), id(0xe7)))?.is_none());
    assert_eq!(
        lookup_outcome(&database, id(0xe6), actor, 1)?
            .ok_or("missing stale UNIQUE-drop writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(8, created.constraint_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_none()
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_creates_primary_key_with_owned_unique_index()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xf2);
    let foreign_key_edge = dependency_write(id(86), id(85), 3).key;
    let mut setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(6, id(84))
                && write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != foreign_key_edge
        })
        .collect::<Vec<_>>();
    setup_writes.extend([
        column_write_not_null(id(84), id(82), "object_84"),
        principal_write_with_name(actor, 1, 1, "primary_create_admin"),
        object_write_with_name(actor, 8, "primary_create_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xf3, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xf3), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut stale_writer = storage.begin_invocation(id(0xd8), actor, 9_999, 1, 1024)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(82))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_tuple = encode_tuple(
        &stale_schema.columns,
        stale_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"stale before PRIMARY KEY add".to_vec(),
        ))],
    )?;
    let stale_row = prdb_engine::overlay::RowId {
        table: id(82),
        row: id(0xd9),
    };
    stale_writer.stage_put(stale_row, &stale_tuple)?;

    let invocation_id = id(0xf4);
    let mut audit = identity_audit_event(0xf5, actor, id(82));
    audit.correlation_id = invocation_id;
    let created = storage
        .create_primary_key(prdb_engine::storage_manager::CreatePrimaryKeyRequest {
            invocation_id,
            authenticated_actor_id: actor,
            table_id: id(82),
            column_ids: vec![id(84)],
            retention_deadline_us: 9_999,
            audit_event: audit,
        })
        .map_err(|error| format!("PRIMARY KEY creation failed: {error:?}"))?;
    assert_eq!(created.commit_sequence, 2);
    assert!(matches!(
        stale_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), stale_row.row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, id(0xd8), actor, 1)?
            .ok_or("missing stale PRIMARY KEY-add writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let constraint = catalog
        .get(id_catalog_key(8, created.constraint_id).as_slice())?
        .ok_or("missing created PRIMARY KEY")?;
    let decoded = decode_registered_envelope(constraint.value())?;
    let kind = decoded
        .fields
        .iter()
        .find(|field| field.tag == 2)
        .ok_or("missing PRIMARY KEY kind")?
        .payload
        .as_slice()
        .try_into()?;
    assert_eq!(u16::from_le_bytes(kind), 2);
    assert!(
        catalog
            .get(id_catalog_key(9, created.index_id).as_slice())?
            .is_some()
    );
    drop(constraint);
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    let drop_invocation_id = id(0xe4);
    let mut drop_audit = identity_audit_event(0xe5, actor, created.constraint_id);
    drop_audit.correlation_id = drop_invocation_id;
    assert_eq!(
        storage
            .drop_primary_key(
                drop_invocation_id,
                actor,
                created.constraint_id,
                9_999,
                drop_audit,
            )
            .map_err(|error| format!("PRIMARY KEY drop failed: {error:?}"))?,
        3
    );
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_refuses_to_drop_primary_key_referenced_by_foreign_key()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::TableLifecycleError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xe8);
    let mut setup_writes = foreign_key_catalog_writes(true, true);
    setup_writes.extend([
        principal_write_with_name(actor, 1, 1, "referenced_key_drop_admin"),
        object_write_with_name(actor, 8, "referenced_key_drop_admin"),
        membership_write(actor, admin_role),
        dependency_write(actor, admin_role, 5),
    ]);
    let mut setup_audit = identity_audit_event(0xe9, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xe9), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let invocation_id = id(0xea);
    let mut audit = identity_audit_event(0xeb, actor, id(85));
    audit.correlation_id = invocation_id;
    assert_eq!(
        storage.drop_primary_key(invocation_id, actor, id(85), 9_999, audit),
        Err(TableLifecycleError::Commit(CommitError::InvalidCatalog))
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing rejected referenced-key drop outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(8, id(85)),
        id_catalog_key(9, id(87)),
        dependency_write(id(86), id(85), 3).key,
    ] {
        assert!(catalog.get(key.as_slice())?.is_some(), "missing {key:?}");
    }
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_creates_and_drops_check_constraint() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0xf6);
    let foreign_key_edge = dependency_write(id(86), id(85), 3).key;
    let setup_writes = foreign_key_catalog_writes(false, true)
        .into_iter()
        .filter(|write| {
            write.key != id_catalog_key(8, id(86))
                && write.key != id_catalog_key(2, id(86))
                && write.key != foreign_key_edge
        })
        .chain([
            principal_write_with_name(actor, 1, 1, "check_create_admin"),
            object_write_with_name(actor, 8, "check_create_admin"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ])
        .collect::<Vec<_>>();
    let mut setup_audit = identity_audit_event(0xf7, admin_role, id(80));
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0xf7), admin_role, 9_999, &setup_writes, &[setup_audit])?;

    let mut insert = storage.begin_invocation(id(0xf8), actor, 9_999, 1, 1024)?;
    let schema = insert.load_catalog_table_schema(id(82))?;
    insert.load_catalog_indexes(&schema)?;
    let tuple = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(ScalarValue::Bytea(b"check row".to_vec()))],
    )?;
    insert.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: id(0xf9),
        },
        &tuple,
    )?;
    assert_eq!(insert.commit()?, 2);

    let invocation_id = id(0xfa);
    let mut invalid_audit = identity_audit_event(0xfb, actor, id(82));
    invalid_audit.correlation_id = invocation_id;
    assert!(matches!(
        storage.create_check_constraint(
            prdb_engine::storage_manager::CreateCheckConstraintRequest {
                invocation_id,
                authenticated_actor_id: actor,
                table_id: id(82),
                expression: prdb_engine::authorization::Expr::CurrentPrincipalId,
                dependency_column_ids: vec![],
                retention_deadline_us: 9_999,
                audit_event: invalid_audit,
            }
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::InvalidRequest)
    ));

    let invalid_row_check_id = id(0xfc);
    let mut invalid_row_check_audit = identity_audit_event(0xfd, actor, id(82));
    invalid_row_check_audit.correlation_id = invalid_row_check_id;
    assert!(matches!(
        storage.create_check_constraint(
            prdb_engine::storage_manager::CreateCheckConstraintRequest {
                invocation_id: invalid_row_check_id,
                authenticated_actor_id: actor,
                table_id: id(82),
                expression: prdb_engine::authorization::Expr::IsNull(Box::new(
                    prdb_engine::authorization::Expr::Field(0),
                )),
                dependency_column_ids: vec![id(84)],
                retention_deadline_us: 9_999,
                audit_event: invalid_row_check_audit,
            }
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::CheckViolation
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, invalid_row_check_id, actor, 1)?
            .ok_or("missing rejected CHECK creation outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut stale_add_writer = storage.begin_invocation(id(0xdc), actor, 9_999, 1, 1024)?;
    let stale_add_schema = stale_add_writer.load_catalog_table_schema(id(82))?;
    stale_add_writer.load_catalog_indexes(&stale_add_schema)?;
    let stale_add_tuple = encode_tuple(
        &stale_add_schema.columns,
        stale_add_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"stale before CHECK add".to_vec()))],
    )?;
    let stale_add_row = prdb_engine::overlay::RowId {
        table: id(82),
        row: id(0xdd),
    };
    stale_add_writer.stage_put(stale_add_row, &stale_add_tuple)?;

    let invocation_id = id(0xfe);
    let mut create_audit = identity_audit_event(0xff, actor, id(82));
    create_audit.correlation_id = invocation_id;
    let created = storage
        .create_check_constraint(prdb_engine::storage_manager::CreateCheckConstraintRequest {
            invocation_id,
            authenticated_actor_id: actor,
            table_id: id(82),
            expression: prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(true))),
            dependency_column_ids: vec![],
            retention_deadline_us: 9_999,
            audit_event: create_audit,
        })
        .map_err(|error| format!("CHECK constraint creation failed: {error:?}"))?;
    assert_eq!(created.commit_sequence, 3);
    assert!(matches!(
        stale_add_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), stale_add_row.row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, id(0xdc), actor, 1)?
            .ok_or("missing stale CHECK-add writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    prdb_engine::startup::validate_persisted_records(&database)?;

    let mut stale_drop_writer = storage.begin_invocation(id(0xe3), actor, 9_999, 1, 1024)?;
    let stale_drop_schema = stale_drop_writer.load_catalog_table_schema(id(82))?;
    stale_drop_writer.load_catalog_indexes(&stale_drop_schema)?;
    let stale_drop_tuple = encode_tuple(
        &stale_drop_schema.columns,
        stale_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"stale after CHECK drop".to_vec()))],
    )?;
    let stale_drop_row = prdb_engine::overlay::RowId {
        table: id(82),
        row: id(0xe4),
    };
    stale_drop_writer.stage_put(stale_drop_row, &stale_drop_tuple)?;

    let drop_invocation_id = id(0xe1);
    let mut drop_audit = identity_audit_event(0xe2, actor, created.constraint_id);
    drop_audit.correlation_id = drop_invocation_id;
    assert_eq!(
        storage.drop_check_constraint(
            drop_invocation_id,
            actor,
            created.constraint_id,
            9_999,
            drop_audit,
        )?,
        4
    );
    assert!(matches!(
        stale_drop_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(id(82), stale_drop_row.row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, id(0xe3), actor, 1)?
            .ok_or("missing stale CHECK-drop writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert!(
        catalog
            .get(id_catalog_key(8, created.constraint_id).as_slice())?
            .is_none()
    );
    assert!(
        catalog
            .get(id_catalog_key(7, created.expression_id).as_slice())?
            .is_none()
    );
    drop(catalog);
    drop(read);

    let mut remove_seed = storage.begin_invocation(id(0xd0), actor, 9_999, 1, 1024)?;
    let remove_seed_schema = remove_seed.load_catalog_table_schema(id(82))?;
    remove_seed.load_catalog_indexes(&remove_seed_schema)?;
    remove_seed.stage_delete(prdb_engine::overlay::RowId {
        table: id(82),
        row: id(0xf9),
    })?;
    remove_seed.commit()?;

    let insert_invocation = id(0xd1);
    let insert_row_id = id(0xd2);
    let mut racing_insert = storage.begin_invocation(insert_invocation, actor, 9_999, 1, 1024)?;
    let racing_schema = racing_insert.load_catalog_table_schema(id(82))?;
    racing_insert.load_catalog_indexes(&racing_schema)?;
    let violating_tuple = encode_tuple(
        &racing_schema.columns,
        racing_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"not null".to_vec()))],
    )?;
    racing_insert.stage_put(
        prdb_engine::overlay::RowId {
            table: id(82),
            row: insert_row_id,
        },
        &violating_tuple,
    )?;

    let check_invocation = id(0xd3);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (insert_result, check_result) = std::thread::scope(|scope| {
        let insert_barrier = barrier.clone();
        let insert = scope.spawn(move || {
            insert_barrier.wait();
            racing_insert.commit()
        });
        let check_barrier = barrier.clone();
        let check_storage = &storage;
        let check = scope.spawn(move || {
            let mut audit = identity_audit_event(0xd4, actor, id(82));
            audit.correlation_id = check_invocation;
            check_barrier.wait();
            check_storage.create_check_constraint(
                prdb_engine::storage_manager::CreateCheckConstraintRequest {
                    invocation_id: check_invocation,
                    authenticated_actor_id: actor,
                    table_id: id(82),
                    expression: prdb_engine::authorization::Expr::IsNull(Box::new(
                        prdb_engine::authorization::Expr::Field(0),
                    )),
                    dependency_column_ids: vec![id(84)],
                    retention_deadline_us: 9_999,
                    audit_event: audit,
                },
            )
        });
        barrier.wait();
        (
            insert.join().expect("CHECK race insert thread panicked"),
            check.join().expect("CHECK race DDL thread panicked"),
        )
    });
    match (insert_result, check_result) {
        (
            Ok(_),
            Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
                CommitError::CheckViolation,
            )),
        ) => {
            assert_eq!(
                lookup_outcome(&database, insert_invocation, actor, 1)?
                    .ok_or("missing winning CHECK-race insert outcome")?
                    .status,
                OutcomeStatus::Committed
            );
            assert_eq!(
                lookup_outcome(&database, check_invocation, actor, 1)?
                    .ok_or("missing aborted CHECK-race DDL outcome")?
                    .status,
                OutcomeStatus::Aborted
            );
            assert!(row(&database, &table_row_key(id(82), insert_row_id))?.is_some());
            assert_eq!(commit_sequence(&database)?, before_race + 1);
        }
        (
            Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)),
            Ok(created_check),
        ) => {
            assert_eq!(
                lookup_outcome(&database, insert_invocation, actor, 1)?
                    .ok_or("missing aborted CHECK-race insert outcome")?
                    .status,
                OutcomeStatus::Aborted
            );
            assert_eq!(
                lookup_outcome(&database, check_invocation, actor, 1)?
                    .ok_or("missing winning CHECK-race DDL outcome")?
                    .status,
                OutcomeStatus::Committed
            );
            assert!(row(&database, &table_row_key(id(82), insert_row_id))?.is_none());
            assert_eq!(commit_sequence(&database)?, before_race + 1);
            storage.drop_check_constraint(
                id(0xd5),
                actor,
                created_check.constraint_id,
                9_999,
                identity_audit_event(0xd6, actor, created_check.constraint_id),
            )?;
        }
        (insert_result, check_result) => panic!(
            "unexpected CHECK creation race outcomes: insert={insert_result:?}, check={check_result:?}"
        ),
    }
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn storage_manager_drop_referencing_table_removes_owned_foreign_key()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut foreign_key_writes = foreign_key_catalog_writes(true, true);
    foreign_key_writes.extend([
        constraint_write(id(88), id(82), 5, &[id(84)], Some(id(85)), &[id(83)], None),
        object_write(id(88), 6),
        dependency_write(id(88), id(85), 3),
    ]);
    let mut catalog_audit = identity_audit_event(0xa1, admin_role, id(0x80));
    catalog_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xa1),
        admin_role,
        9_999,
        &foreign_key_writes,
        &[catalog_audit],
    )?;

    let actor = id(0xa2);
    let mut setup_audit = identity_audit_event(0xa3, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xa3),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "drop_referencing_table_admin"),
            object_write_with_name(actor, 8, "drop_referencing_table_admin"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let target_table_key = id_catalog_key(5, id(81));
    let target_epoch_before = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let bytes = catalog
            .get(target_table_key.as_slice())?
            .ok_or("missing target table before drop")?
            .value()
            .to_vec();
        let record = decode_registered_envelope(&bytes)?;
        u64::from_le_bytes(
            record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("missing target write epoch")?
                .payload
                .as_slice()
                .try_into()?,
        )
    };
    let mut stale_target_writer = storage.begin_invocation(id(0xa6), admin_role, 9_999, 1, 1024)?;
    let stale_target_schema = stale_target_writer.load_catalog_table_schema(id(81))?;
    stale_target_writer.load_catalog_indexes(&stale_target_schema)?;
    let stale_target_tuple = encode_tuple(
        &stale_target_schema.columns,
        stale_target_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"staged before FK owner drop".to_vec(),
        ))],
    )?;
    let stale_target_row = id(0xa7);
    stale_target_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(81),
            row: stale_target_row,
        },
        &stale_target_tuple,
    )?;

    let invocation_id = id(0xa4);
    let mut drop_audit = identity_audit_event(0xa5, actor, id(82));
    drop_audit.correlation_id = invocation_id;
    let dropped = storage.drop_table(invocation_id, actor, id(82), 9_999, drop_audit);
    assert!(
        dropped.is_ok(),
        "dropping the FK owner table returned {dropped:?}"
    );
    assert_eq!(dropped.unwrap().table_id, id(82));
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing referencing-table drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    assert!(matches!(
        stale_target_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, id(0xa6), admin_role, 1)?
            .ok_or("missing stale target-writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(2, id(82)),
        id_catalog_key(5, id(82)),
        id_catalog_key(2, id(84)),
        id_catalog_key(6, id(84)),
        id_catalog_key(2, id(86)),
        id_catalog_key(8, id(86)),
        id_catalog_key(2, id(88)),
        id_catalog_key(8, id(88)),
        dependency_write(id(86), id(85), 3).key,
        dependency_write(id(88), id(85), 3).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "record remained: {key:?}"
        );
    }
    for key in [
        id_catalog_key(2, id(81)),
        id_catalog_key(5, id(81)),
        id_catalog_key(8, id(85)),
        id_catalog_key(9, id(87)),
        dependency_write(id(85), id(87), 4).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_some(),
            "target record missing: {key:?}"
        );
    }
    let target_bytes = catalog
        .get(target_table_key.as_slice())?
        .ok_or("target table missing after drop")?
        .value()
        .to_vec();
    let target_epoch_after = u64::from_le_bytes(
        decode_registered_envelope(&target_bytes)?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("missing target write epoch after drop")?
            .payload
            .as_slice()
            .try_into()?,
    );
    assert_eq!(target_epoch_after, target_epoch_before + 1);
    assert!(
        read.open_table(ROWS)?
            .get(table_row_key(id(81), stale_target_row).as_slice())?
            .is_none()
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_drop_table_removes_self_referential_foreign_key()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_role = builtin_owner_id();
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let schema_id = id(80);
    let table_id = id(82);
    let column_id = id(84);
    let primary_id = id(85);
    let foreign_key_id = id(86);
    let index_id = id(87);
    let catalog_writes = [
        schema_write(schema_id),
        table_write(table_id, schema_id),
        column_write_not_null(column_id, table_id, "object_84"),
        object_write(schema_id, 1),
        object_write(table_id, 3),
        object_write(column_id, 4),
    ];
    let mut catalog_audit = identity_audit_event(0x91, admin_role, table_id);
    catalog_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(
            id(0x91),
            admin_role,
            9_999,
            &catalog_writes,
            &[catalog_audit],
        )
        .map_err(|error| format!("self-FK catalog setup failed: {error:?}"))?;

    let actor = id(0x92);
    let mut setup_audit = identity_audit_event(0x93, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(
            id(0x93),
            admin_role,
            9_999,
            &[
                principal_write_with_name(actor, 1, 1, "self_fk_drop_admin"),
                object_write_with_name(actor, 8, "self_fk_drop_admin"),
                membership_write(actor, admin_role),
                dependency_write(actor, admin_role, 5),
            ],
            &[setup_audit],
        )
        .map_err(|error| format!("self-FK actor setup failed: {error:?}"))?;

    let primary_constraint = constraint_write(
        primary_id,
        table_id,
        2,
        &[column_id],
        None,
        &[],
        Some(index_id),
    );
    let (primary_index_key, primary_index_payload) =
        owned_unique_index_definition(index_id, table_id, &[column_id], primary_id);
    let table_key = id_catalog_key(5, table_id);
    let current_table = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(table_key.as_slice())?
        .ok_or("self-FK table missing before primary key add")?
        .value()
        .to_vec();
    let mut primary_table_epoch = table_write_with_versions(table_id, schema_id, 1, 2);
    primary_table_epoch.expected_value = Some(current_table);
    let primary_invocation = id(0x96);
    let mut primary_audit = identity_audit_event(0x97, actor, primary_id);
    primary_audit.correlation_id = primary_invocation;
    storage
        .commit_authorized_constraint_ddl(
            primary_invocation,
            actor,
            9_999,
            &[
                primary_constraint,
                object_write(primary_id, 6),
                CatalogWrite {
                    key: primary_index_key,
                    expected_value: None,
                    payload: Some(primary_index_payload),
                },
                object_write(index_id, 7),
                dependency_write(primary_id, index_id, 4),
                primary_table_epoch,
            ],
            &[primary_audit],
        )
        .map_err(|error| format!("self-FK primary setup failed: {error:?}"))?;

    let foreign_key = constraint_write(
        foreign_key_id,
        table_id,
        5,
        &[column_id],
        Some(primary_id),
        &[column_id],
        None,
    );
    let current_table = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(table_key.as_slice())?
        .ok_or("self-FK table missing before foreign key add")?
        .value()
        .to_vec();
    let mut foreign_key_table_epoch = table_write_with_versions(table_id, schema_id, 1, 3);
    foreign_key_table_epoch.expected_value = Some(current_table);
    let foreign_key_invocation = id(0x98);
    let mut foreign_key_audit = identity_audit_event(0x99, admin_role, foreign_key_id);
    foreign_key_audit.action = AuditAction::AuthorityChange;
    foreign_key_audit.correlation_id = foreign_key_invocation;
    storage
        .commit_catalog_and_audit(
            foreign_key_invocation,
            admin_role,
            9_999,
            &[
                foreign_key,
                object_write(foreign_key_id, 6),
                dependency_write(foreign_key_id, primary_id, 3),
                foreign_key_table_epoch,
            ],
            &[foreign_key_audit],
        )
        .map_err(|error| format!("self-FK foreign-key setup failed: {error:?}"))?;

    let self_row_id = id(0x9c);
    let mut insert = storage.begin_invocation(id(0x9d), actor, 9_999, 1, 1024)?;
    let table_schema = insert.load_catalog_table_schema(table_id)?;
    insert.load_catalog_indexes(&table_schema)?;
    let self_tuple = encode_tuple(
        &table_schema.columns,
        table_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"self".to_vec()))],
    )?;
    insert.stage_put(
        prdb_engine::overlay::RowId {
            table: table_id,
            row: self_row_id,
        },
        &self_tuple,
    )?;
    insert.commit()?;
    let self_row_key = table_row_key(table_id, self_row_id);
    {
        let read = database.begin_read()?;
        assert!(
            read.open_table(ROWS)?.get(&self_row_key[..])?.is_some(),
            "self-referencing row was not committed"
        );
        assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 1);
        assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 1);
    }

    let invocation_id = id(0x9a);
    let mut drop_audit = identity_audit_event(0x9b, actor, table_id);
    drop_audit.correlation_id = invocation_id;
    let dropped = storage.drop_table(invocation_id, actor, table_id, 9_999, drop_audit);
    assert!(
        dropped.is_ok(),
        "dropping the self-FK table returned {dropped:?}"
    );
    assert_eq!(dropped.unwrap().table_id, table_id);
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing self-FK table-drop outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    for key in [
        id_catalog_key(2, table_id),
        id_catalog_key(5, table_id),
        id_catalog_key(2, column_id),
        id_catalog_key(6, column_id),
        id_catalog_key(2, primary_id),
        id_catalog_key(8, primary_id),
        id_catalog_key(2, foreign_key_id),
        id_catalog_key(8, foreign_key_id),
        id_catalog_key(2, index_id),
        id_catalog_key(9, index_id),
        dependency_write(foreign_key_id, primary_id, 3).key,
        dependency_write(primary_id, index_id, 4).key,
    ] {
        assert!(
            catalog.get(key.as_slice())?.is_none(),
            "self-FK table-drop record remained: {key:?}"
        );
    }
    assert!(read.open_table(ROWS)?.get(&self_row_key[..])?.is_none());
    assert_eq!(read.open_table(schema::INDEXES)?.iter()?.count(), 0);
    assert_eq!(read.open_table(schema::ROW_INDEXES)?.iter()?.count(), 0);
    drop(read);
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
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
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 128,
            max_invocation_age: Duration::from_secs(15 * 60),
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        },
    )?;
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
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
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
                max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
                max_token_lifecycle_requests_per_minute: 30,
                max_admin_cancellation_requests_per_minute: 30,
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
                max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
                max_token_lifecycle_requests_per_minute: 30,
                max_admin_cancellation_requests_per_minute: 30,
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
                max_invocation_scan_work: 0,
                max_token_lifecycle_requests_per_minute: 30,
                max_admin_cancellation_requests_per_minute: 30,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroInvocationScanWorkLimit,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 1,
                max_invocation_age: Duration::from_secs(30),
                max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
                max_token_lifecycle_requests_per_minute: 0,
                max_admin_cancellation_requests_per_minute: 30,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroTokenLifecycleRequestLimit,
        ))
    );
    assert_eq!(
        StorageManager::with_config(
            &database,
            StorageManagerConfig {
                ddl_fence_timeout: Duration::from_millis(100),
                max_active_invocations: 1,
                max_invocation_age: Duration::from_secs(30),
                max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
                max_token_lifecycle_requests_per_minute: 30,
                max_admin_cancellation_requests_per_minute: 0,
            },
        )
        .err(),
        Some(StorageManagerInitError::Config(
            prdb_engine::storage_manager::StorageManagerConfigError::ZeroAdminCancellationRequestLimit,
        ))
    );
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_invocation_age: Duration::from_secs(30),
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
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
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
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

    let mut pending_commit =
        storage.begin_invocation(id(204), id(9), retention_deadline_us, 2, 64)?;
    let cancelled_row_key = table_row_key(id(205), id(206));
    pending_commit.stage_put(
        prdb_engine::overlay::RowId {
            table: id(205),
            row: id(206),
        },
        b"must not commit",
    )?;
    let writer = database.begin_write()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let commit_thread = scope.spawn(move || pending_commit.commit());
        assert_eq!(
            storage.cancel_owned_invocation(id(9), id(204), now_us)?,
            prdb_engine::storage_manager::OwnerCancellationResult::Accepted
        );
        assert!(matches!(
            status_storage
                .get_invocation_status(id(204), id(9), now_us)?
                .unwrap()
                .status,
            InvocationStatus::Active {
                cancellation_requested: true,
                ..
            }
        ));
        writer.abort()?;
        assert!(matches!(
            commit_thread
                .join()
                .map_err(|_| std::io::Error::other("commit thread panicked"))?,
            Err(prdb_engine::transaction::TransactionError::CancellationRequested)
        ));
        Ok(())
    })?;
    assert!(matches!(
        storage
            .get_invocation_status(id(204), id(9), now_us)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(InvocationOutcome {
            status: OutcomeStatus::Aborted,
            ..
        })
    ));
    assert!(row(&database, &cancelled_row_key)?.is_none());
    let committed = storage.begin_invocation(id(207), id(9), retention_deadline_us, 2, 64)?;
    committed.commit()?;
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
            status: OutcomeStatus::Aborted,
            ..
        })
    ));
    assert!(matches!(
        restarted_storage
            .get_invocation_status(id(207), id(9), now_us)?
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
fn storage_manager_runtime_and_metadata_reads_enforce_authorization()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientMode};
    use prdb_engine::storage_manager::{
        AdministrativeInvocationListError, InvocationStatus, InvocationStatusConsistency,
        MAX_ADMIN_INVOCATION_PAGE,
    };
    use zeroize::{Zeroize, Zeroizing};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_id = id(230);
    let ordinary_id = id(231);
    let backup_admin_id = id(235);
    let user_manager_id = id(238);
    let manage_users_role = id(239);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(admin_id, 1, 1, "runtime_admin"),
        object_write_with_name(admin_id, 8, "runtime_admin"),
        principal_write_with_name(ordinary_id, 1, 1, "runtime_user"),
        object_write_with_name(ordinary_id, 8, "runtime_user"),
        principal_write_with_name(backup_admin_id, 1, 1, "backup_admin"),
        object_write_with_name(backup_admin_id, 8, "backup_admin"),
        principal_write_with_name(user_manager_id, 1, 1, "token_manager"),
        object_write_with_name(user_manager_id, 8, "token_manager"),
        principal_write_with_name(manage_users_role, 2, 1, "manage_users"),
        object_write_with_name(manage_users_role, 9, "manage_users"),
        membership_write_with_grantor(admin_id, admin_role, admin_role),
        dependency_write(admin_id, admin_role, 5),
        membership_write_with_grantor(backup_admin_id, admin_role, admin_role),
        dependency_write(backup_admin_id, admin_role, 5),
        membership_write_with_grantor(user_manager_id, manage_users_role, admin_role),
        dependency_write(user_manager_id, manage_users_role, 5),
        grant_write(
            manage_users_role,
            database_id,
            12,
            1,
            database_id,
            admin_role,
        ),
        dependency_write(manage_users_role, database_id, 5),
        credential_verifier_write(id(236), backup_admin_id, &[0x6a; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(237), admin_id, &[0x5a; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(242), ordinary_id, &[0x6b; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(240), user_manager_id, &[0x4b; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(241), user_manager_id, &[0x3c; 32], 1, i64::MAX, None, 1),
    ];
    let mut setup_audit = audit_event(216);
    setup_audit.actor_id = Some(admin_role);
    storage
        .commit_catalog_and_audit(id(216), admin_role, 9_999, &records, &[setup_audit])
        .map_err(|error| format!("setup catalog commit: {error:?}"))?;

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;
    let mut first = storage.begin_invocation(id(232), admin_id, retention_deadline_us, 2, 64)?;
    let mut second =
        storage.begin_invocation(id(233), ordinary_id, retention_deadline_us, 2, 64)?;

    assert!(matches!(
        storage.list_runtime_invocations(ordinary_id, now_us, 1, None),
        Err(AdministrativeInvocationListError::Unauthorized)
    ));
    let first_page = storage.list_runtime_invocations(admin_id, now_us, 1, None)?;
    assert_eq!(first_page.invocations.len(), 1);
    assert_eq!(first_page.invocations[0].invocation_id, id(232));
    assert_eq!(first_page.invocations[0].principal_id, admin_id);
    assert_eq!(first_page.observed_at_us, now_us);
    assert_eq!(
        first_page.consistency,
        InvocationStatusConsistency::BestEffort
    );
    let cursor = first_page.continuation.unwrap();
    assert!(matches!(
        storage.list_runtime_invocations(backup_admin_id, now_us, 1, Some(&cursor)),
        Err(AdministrativeInvocationListError::InvalidCursor)
    ));
    let second_page = storage.list_runtime_invocations(admin_id, now_us, 1, Some(&cursor))?;
    assert_eq!(second_page.invocations.len(), 1);
    assert_eq!(second_page.invocations[0].invocation_id, id(233));
    assert_eq!(second_page.invocations[0].principal_id, ordinary_id);
    assert!(matches!(
        storage.get_administrative_invocation_status(admin_id, id(233), now_us)?,
        Some(snapshot) if matches!(snapshot.status, InvocationStatus::Active { .. })
    ));
    assert!(matches!(
        storage.get_administrative_invocation_status(ordinary_id, id(233), now_us),
        Err(AdministrativeInvocationListError::Unauthorized)
    ));
    assert_eq!(
        storage.get_administrative_invocation_status(admin_id, id(239), now_us)?,
        None
    );

    let stale = storage
        .list_runtime_invocations(admin_id, now_us, 1, None)?
        .continuation
        .unwrap();
    let mut third = storage.begin_invocation(id(234), ordinary_id, retention_deadline_us, 2, 64)?;
    assert!(matches!(
        storage.list_runtime_invocations(admin_id, now_us, 1, Some(&stale)),
        Err(AdministrativeInvocationListError::StaleCursor)
    ));
    assert!(matches!(
        storage.list_runtime_invocations(admin_id, now_us, 0, None),
        Err(AdministrativeInvocationListError::InvalidPageLimit)
    ));
    assert!(
        storage
            .list_runtime_invocations(admin_id, now_us, MAX_ADMIN_INVOCATION_PAGE + 1, None)
            .is_err()
    );

    third.abort()?;
    assert!(matches!(
        storage.get_administrative_invocation_status(admin_id, id(234), now_us)?,
        Some(snapshot) if matches!(&snapshot.status, InvocationStatus::Terminal(outcome)
            if outcome.status == OutcomeStatus::Aborted)
    ));

    struct AcceptMetadata;
    impl prdb_engine::supervisor::SubmissionMetadataValidator for AcceptMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &prdb_engine::supervisor::SubmissionMetadata) -> bool {
            true
        }
    }
    let staging = prdb_engine::supervisor::StagingStore::new(
        prdb_engine::supervisor::StagingLimits::default(),
        std::sync::Arc::new(AcceptMetadata),
    )?;

    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    let visible_schema_invocation = id(243);
    let mut visible_schema_audit = identity_audit_event(0xf0, admin_id, id(0xf1));
    visible_schema_audit.correlation_id = visible_schema_invocation;
    let visible_schema = storage.create_schema(
        visible_schema_invocation,
        admin_id,
        "visible_app",
        admin_id,
        retention_deadline_us,
        visible_schema_audit,
    )?;
    let hidden_schema_invocation = id(244);
    let mut hidden_schema_audit = identity_audit_event(0xf2, admin_id, id(0xf3));
    hidden_schema_audit.correlation_id = hidden_schema_invocation;
    let hidden_schema = storage.create_schema(
        hidden_schema_invocation,
        admin_id,
        "hidden_app",
        admin_id,
        retention_deadline_us,
        hidden_schema_audit,
    )?;
    let table_invocation = id(245);
    let mut table_audit = identity_audit_event(0xf4, admin_id, id(0xf5));
    table_audit.correlation_id = table_invocation;
    let visible_table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: admin_id,
        schema_id: visible_schema.schema_id,
        name: "metrics".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "reading".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "reading_extra".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us,
        audit_event: table_audit,
    })?;
    let ungranted_table_invocation = id(248);
    let mut ungranted_table_audit = identity_audit_event(0xf9, admin_id, id(0xfa));
    ungranted_table_audit.correlation_id = ungranted_table_invocation;
    let _ungranted_table = storage.create_table(CreateTableRequest {
        invocation_id: ungranted_table_invocation,
        authenticated_actor_id: admin_id,
        schema_id: visible_schema.schema_id,
        name: "ungranted_metrics".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "secret".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us,
        audit_event: ungranted_table_audit,
    })?;
    let hidden_table_invocation = id(246);
    let mut hidden_table_audit = identity_audit_event(0xf6, admin_id, id(0xf7));
    hidden_table_audit.correlation_id = hidden_table_invocation;
    let _hidden_table = storage.create_table(CreateTableRequest {
        invocation_id: hidden_table_invocation,
        authenticated_actor_id: admin_id,
        schema_id: hidden_schema.schema_id,
        name: "secrets".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us,
        audit_event: hidden_table_audit,
    })?;
    let read = database.begin_read()?;
    let mut ordinary_epoch = principal_write_with_name(ordinary_id, 1, 2, "runtime_user");
    ordinary_epoch.expected_value = Some(
        read.open_table(CATALOG)?
            .get(id_catalog_key(10, ordinary_id).as_slice())?
            .ok_or("ordinary metadata principal is missing")?
            .value()
            .to_vec(),
    );
    drop(read);
    let usage_grant = grant_write(
        ordinary_id,
        visible_schema.schema_id,
        1,
        1,
        visible_schema.schema_id,
        admin_id,
    );
    let select_grant = grant_write(
        ordinary_id,
        visible_table.table_id,
        2,
        1,
        visible_table.table_id,
        admin_id,
    );
    let metadata_grant_invocation = id(247);
    let mut metadata_grant_audit = identity_audit_event(0xf8, admin_id, visible_table.table_id);
    metadata_grant_audit.action = AuditAction::AuthorityChange;
    metadata_grant_audit.correlation_id = metadata_grant_invocation;
    storage.commit_authorized_object_grant_ddl(
        metadata_grant_invocation,
        admin_id,
        retention_deadline_us,
        &[
            usage_grant.clone(),
            dependency_write(ordinary_id, visible_schema.schema_id, 5),
            select_grant,
            dependency_write(ordinary_id, visible_table.table_id, 5),
            ordinary_epoch,
        ],
        &[metadata_grant_audit],
    )?;
    let mut control = ClientConnectionSession::default();
    control.accept_hello(ClientMode::Control, "runtime-status")?;
    control.authenticate(&storage, id(237), &[0x5a; 32])?;
    let encode_invocation_id = |invocation_id: [u8; 16]| {
        let mut encoded = String::with_capacity(32);
        for byte in invocation_id {
            encoded.push_str(&format!("{byte:02x}"));
        }
        format!("{{\"invocation_id\":\"{encoded}\"}}").into_bytes()
    };
    let active_status = control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 1,
            response_sequence: 0,
            payload: encode_invocation_id(id(233)),
        },
    )?;
    assert_eq!(active_status.payload, b"{\"state\":\"active\"}");
    let terminal_status = control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 2,
            response_sequence: 0,
            payload: encode_invocation_id(id(234)),
        },
    )?;
    assert_eq!(terminal_status.payload, b"{\"state\":\"aborted\"}");
    let cancelled = control.handle_cancel_prepared_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 9,
            request_id: 3,
            response_sequence: 0,
            payload: encode_invocation_id(id(233)),
        },
    )?;
    assert_eq!(cancelled.payload, b"{\"state\":\"accepted\"}");
    let mut discovery_control = ClientConnectionSession::default();
    discovery_control.accept_hello(ClientMode::Control, "schema-discovery")?;
    discovery_control.authenticate(&storage, id(242), &[0x6b; 32])?;
    let schema_discovery = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 1,
            payload: br#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let schema_discovery = match schema_discovery {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("metadata discovery unexpectedly started an invocation".into());
        }
    };
    let schema_discovery = String::from_utf8(schema_discovery)?;
    assert!(schema_discovery.contains("\"schemas\":["));
    assert!(schema_discovery.contains("\"name\":\"information_schema\""));
    assert!(schema_discovery.contains("\"name\":\"visible_app\""));
    assert!(!schema_discovery.contains("\"name\":\"hidden_app\""));
    let public_relations =
        prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
            prdb_engine::ipc::ServiceRole::Gateway,
            &storage,
            &staging,
            &mut discovery_control,
            &prdb_engine::ipc::IpcFrame {
                operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
                request_id: 2,
                payload: br#"{"phase":"read","operation_version":1,"operation":"metadata.relations.list","arguments":{"schema_id":"80808080808080808080808080808002"}}"#.to_vec(),
            },
            prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
                retention_period_us: 60_000_000,
                max_rows: 1,
                max_bytes: 1,
            },
        )?;
    let public_relations = match public_relations {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("relation discovery unexpectedly started an invocation".into());
        }
    };
    let public_relations = String::from_utf8(public_relations)?;
    assert!(public_relations.contains("\"name\":\"columns\""));
    assert!(public_relations.contains("\"name\":\"schemata\""));
    assert!(public_relations.contains("\"name\":\"tables\""));
    let public_columns = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 3,
            payload: br#"{"phase":"read","operation_version":1,"operation":"metadata.columns.list","arguments":{"schema_id":"80808080808080808080808080808002","relation_id":"81818181818181818181818181818101"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let public_columns = match public_columns {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("column discovery unexpectedly started an invocation".into());
        }
    };
    let public_columns = String::from_utf8(public_columns)?;
    assert!(public_columns.contains("\"name\":\"table_schema\""));
    assert!(public_columns.contains("\"scalar_type\":\"text\""));
    assert!(public_columns.contains("\"nullable\":false"));
    let hex_id = |value: [u8; 16]| {
        value
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let visible_relations = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 5,
            payload: format!(
                "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.relations.list\",\"arguments\":{{\"schema_id\":\"{}\"}}}}",
                hex_id(visible_schema.schema_id)
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let visible_relations = match visible_relations {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("granted relation discovery unexpectedly started an invocation".into());
        }
    };
    assert!(
        visible_relations.contains("\"name\":\"metrics\""),
        "{visible_relations}"
    );
    assert!(!visible_relations.contains("\"name\":\"ungranted_metrics\""));
    let visible_columns = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 6,
            payload: format!(
                "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.columns.list\",\"arguments\":{{\"schema_id\":\"{}\",\"relation_id\":\"{}\"}}}}",
                hex_id(visible_schema.schema_id),
                hex_id(visible_table.table_id)
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("visible column gateway dispatch: {error:?}"))?;
    let visible_columns = match visible_columns {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("granted column discovery unexpectedly started an invocation".into());
        }
    };
    assert!(visible_columns.contains("\"name\":\"reading\""));
    assert!(visible_columns.contains("\"scalar_type\":\"integer\""));
    let hidden_relations = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 7,
            payload: format!(
                "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.relations.list\",\"arguments\":{{\"schema_id\":\"{}\"}}}}",
                hex_id(hidden_schema.schema_id)
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("hidden relation gateway dispatch: {error:?}"))?;
    let hidden_relations = match hidden_relations {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ungranted relation discovery unexpectedly started an invocation".into());
        }
    };
    assert_eq!(hidden_relations, br#"{"relations":[]}"#);
    let unavailable_relations = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 8,
            payload: br#"{"phase":"read","operation_version":1,"operation":"metadata.relations.list","arguments":{"schema_id":"f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let unavailable_relations = match unavailable_relations {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("relation discovery unexpectedly started an invocation".into());
        }
    };
    assert_eq!(unavailable_relations, br#"{"relations":[]}"#);

    let metadata_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let metadata_address = metadata_listener.local_addr()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let server = scope.spawn(|| {
            use std::io::{Read, Write};
            for _ in 0..2 {
                let (mut stream, _) = metadata_listener.accept().unwrap();
                let mut decoder = prdb_engine::client_protocol::ClientFrameDecoder::default();
                let mut session = ClientConnectionSession::default();
                let mut input = [0u8; 4096];
                loop {
                    let count = stream.read(&mut input).unwrap();
                    if count == 0 {
                        break;
                    }
                    for request in decoder.push(&input[..count]).unwrap() {
                        let request_id = request.request_id;
                        let result = match request.kind {
                            1 | 2 => session.handle_handshake_request(&storage, request),
                            10 => session.handle_admin_request(
                                &storage,
                                request,
                                retention_deadline_us,
                            ),
                            kind => panic!("unexpected metadata client frame kind {kind}"),
                        };
                        let response = result.unwrap_or_else(|error| {
                            prdb_engine::client_protocol::ClientFrame {
                                kind: prdb_engine::client_protocol::CLIENT_ERROR_KIND,
                                request_id,
                                response_sequence: 0,
                                payload: format!("{error:?}").into_bytes(),
                            }
                        });
                        stream.write_all(&response.encode().unwrap()).unwrap();
                    }
                }
            }
        });

        let mut secret = [0x6b; 32];
        let mut metadata_client =
            prdb_engine::client_submit::MetadataClient::connect_authenticated(
                metadata_address,
                id(242),
                &mut secret,
            )?;
        assert_eq!(secret, [0; 32]);
        let schemas = metadata_client.list_schemas()?;
        let schema_page = metadata_client.list_schemas_page(1, None)?;
        assert_eq!(schema_page.records.len(), 1);
        let schema_page_cursor = schema_page
            .next_cursor
            .as_deref()
            .ok_or("expected multiple visible schemas")?;
        assert!(matches!(
            metadata_client.list_relations_page(
                prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
                1,
                Some(schema_page_cursor),
            ),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        let final_schema_page = metadata_client.list_schemas_page(1, Some(schema_page_cursor))?;
        assert_eq!(final_schema_page.records.len(), 1);
        assert!(final_schema_page.next_cursor.is_none());
        let schema = schemas
            .iter()
            .find(|schema| schema.name == "visible_app")
            .ok_or("grant-visible schema missing through MetadataClient")?;
        assert!(!schemas.iter().any(|schema| schema.name == "hidden_app"));
        let public_schema_id = prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID;
        let public_relation_page =
            metadata_client.list_relations_page(public_schema_id, 1, None)?;
        let public_cursor = public_relation_page
            .next_cursor
            .as_deref()
            .ok_or("expected multiple public information_schema relations")?;
        let second_public_relation =
            metadata_client.list_relations_page(public_schema_id, 1, Some(public_cursor))?;
        assert_eq!(second_public_relation.records.len(), 1);
        let second_public_cursor = second_public_relation
            .next_cursor
            .as_deref()
            .ok_or("expected third public information_schema relation")?;
        let third_public_relation =
            metadata_client.list_relations_page(public_schema_id, 1, Some(second_public_cursor))?;
        assert_eq!(third_public_relation.records.len(), 1);
        assert!(third_public_relation.next_cursor.is_none());
        let relations = metadata_client.list_relations(schema.schema_id)?;
        let relation = relations
            .iter()
            .find(|relation| relation.name == "metrics")
            .ok_or("grant-visible relation missing through MetadataClient")?;
        assert!(
            !relations
                .iter()
                .any(|relation| relation.name == "ungranted_metrics")
        );
        let columns = metadata_client.list_columns(schema.schema_id, relation.relation_id)?;
        let column = columns
            .iter()
            .find(|column| column.name == "reading")
            .ok_or("grant-visible column missing through MetadataClient")?;
        let first_column_page =
            metadata_client.list_columns_page(schema.schema_id, relation.relation_id, 1, None)?;
        assert_eq!(first_column_page.records[0].name, "reading");
        let cursor = first_column_page
            .next_cursor
            .as_deref()
            .ok_or("expected a second declared column page")?;
        let rename_invocation = id(249);
        let mut rename_audit = identity_audit_event(0xfb, admin_id, visible_table.column_ids[1]);
        rename_audit.correlation_id = rename_invocation;
        storage.rename_column(
            rename_invocation,
            admin_id,
            visible_table.table_id,
            visible_table.column_ids[1],
            "reading_current",
            retention_deadline_us,
            rename_audit,
        )?;
        let second_column_page = metadata_client.list_columns_page(
            schema.schema_id,
            relation.relation_id,
            1,
            Some(cursor),
        )?;
        assert_eq!(second_column_page.records[0].name, "reading_extra");
        assert!(second_column_page.next_cursor.is_none());
        assert!(matches!(
            metadata_client.list_columns_page(
                schema.schema_id,
                relation.relation_id,
                1,
                Some(cursor),
            ),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        let current_columns =
            metadata_client.list_columns(schema.schema_id, relation.relation_id)?;
        assert!(
            current_columns
                .iter()
                .any(|column| column.name == "reading_current")
        );
        let mut requested_scope = prdb_engine::submission_scope::RequestedScopeBuilder::new();
        requested_scope
            .include_column(
                schema,
                relation,
                column,
                &[prdb_engine::authorization::Operation::Select],
            )
            .map_err(|error| format!("requested-scope selection failed: {error:?}"))?;
        let decoded_scope = prdb_engine::submission_scope::decode_requested_scope_v1(
            &requested_scope
                .encode()
                .map_err(|error| format!("requested-scope encoding failed: {error:?}"))?,
        )
        .map_err(|error| format!("requested-scope decoding failed: {error:?}"))?;
        assert!(decoded_scope.contains(&(
            prdb_engine::authorization::Operation::Usage,
            prdb_engine::authorization::AccessTarget::Schema(schema.schema_id),
        )));
        assert!(decoded_scope.contains(&(
            prdb_engine::authorization::Operation::Select,
            prdb_engine::authorization::AccessTarget::Column {
                table_id: relation.relation_id,
                column_id: column.column_id,
            },
        )));
        let revoked_cursor = metadata_client
            .list_schemas_page(1, None)?
            .next_cursor
            .ok_or("expected a cursor before schema USAGE is revoked")?;
        let usage_dependency = dependency_write(ordinary_id, visible_schema.schema_id, 5);
        let (grant_bytes, dependency_bytes, principal_bytes) = {
            let read = database.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let grant_bytes = catalog
                .get(usage_grant.key.as_slice())?
                .ok_or("schema USAGE grant disappeared before revocation")?
                .value()
                .to_vec();
            let dependency_bytes = catalog
                .get(usage_dependency.key.as_slice())?
                .ok_or("schema USAGE dependency disappeared before revocation")?
                .value()
                .to_vec();
            let principal_bytes = catalog
                .get(id_catalog_key(10, ordinary_id).as_slice())?
                .ok_or("metadata principal disappeared before grant revocation")?
                .value()
                .to_vec();
            (grant_bytes, dependency_bytes, principal_bytes)
        };
        let mut ordinary_epoch = principal_write_with_name(ordinary_id, 1, 3, "runtime_user");
        ordinary_epoch.expected_value = Some(principal_bytes);
        let revoke_schema_usage_invocation = id(250);
        let mut revoke_schema_usage_audit =
            identity_audit_event(0xfc, admin_id, visible_schema.schema_id);
        revoke_schema_usage_audit.action = AuditAction::AuthorityChange;
        revoke_schema_usage_audit.correlation_id = revoke_schema_usage_invocation;
        storage.commit_authorized_object_grant_ddl(
            revoke_schema_usage_invocation,
            admin_id,
            retention_deadline_us,
            &[
                catalog_delete(usage_grant.clone(), grant_bytes),
                catalog_delete(usage_dependency, dependency_bytes),
                ordinary_epoch,
            ],
            &[revoke_schema_usage_audit],
        )?;
        assert!(matches!(
            metadata_client.list_schemas_page(1, Some(&revoked_cursor)),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));

        let foreign_cursor = metadata_client
            .list_relations_page(
                prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
                1,
                None,
            )?
            .next_cursor
            .ok_or("expected a public relation cursor for the cross-connection test")?;
        drop(metadata_client);
        let mut second_secret = [0x6b; 32];
        let mut second_client = prdb_engine::client_submit::MetadataClient::connect_authenticated(
            metadata_address,
            id(242),
            &mut second_secret,
        )?;
        assert!(matches!(
            second_client.list_relations_page(
                prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
                1,
                Some(&foreign_cursor),
            ),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        drop(second_client);
        server.join().map_err(|_| "metadata TCP server panicked")?;
        Ok(())
    })?;

    let token_request = prdb_engine::ipc::IpcFrame {
        operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
        request_id: 4,
        payload: format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"tokens.list\",\"arguments\":{{\"user_id\":\"{}\"}}}}",
            id(235).iter().map(|byte| format!("{byte:02x}")).collect::<String>()
        ).into_bytes(),
    };
    let token_inventory = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &token_request,
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let token_inventory = match token_inventory {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN read unexpectedly started an invocation".into());
        }
    };
    let token_inventory = String::from_utf8(token_inventory)?;
    assert!(token_inventory.contains(&format!(
        "\"token_id\":\"{}\"",
        format!("{:02x}", 236).repeat(16)
    )));
    assert!(token_inventory.contains("test token"));
    assert!(token_inventory.contains("\"revoked\":false"));
    assert!(!token_inventory.contains("verifier"));
    assert!(!token_inventory.contains("secret"));
    let runtime_page = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 5,
            payload: b"{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"runtime.invocations.list\",\"arguments\":{\"limit\":\"1\"}}".to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let runtime_page = match runtime_page {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN runtime listing unexpectedly started an invocation".into());
        }
    };
    assert!(runtime_page.contains(&format!(
        "\"invocation_id\":\"{}\"",
        format!("{:02x}", 232).repeat(16)
    )));
    assert!(runtime_page.contains("\"consistency\":\"best_effort\""));
    let runtime_cursor = runtime_page
        .split("\"next_cursor\":\"")
        .nth(1)
        .ok_or("missing ADMIN runtime listing cursor")?
        .get(..98)
        .ok_or("invalid ADMIN runtime listing cursor")?;
    let runtime_second_page = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 6,
            payload: format!(
                "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"runtime.invocations.list\",\"arguments\":{{\"limit\":\"1\",\"cursor\":\"{runtime_cursor}\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let runtime_second_page = match runtime_second_page {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN runtime listing unexpectedly started an invocation".into());
        }
    };
    assert!(runtime_second_page.contains(&format!(
        "\"invocation_id\":\"{}\"",
        format!("{:02x}", 233).repeat(16)
    )));
    assert!(runtime_second_page.contains("\"cancellation_requested\":true"));
    let mut user_manager_control = ClientConnectionSession::default();
    user_manager_control.accept_hello(ClientMode::Control, "token-manager")?;
    user_manager_control.authenticate(&storage, id(240), &[0x4b; 32])?;
    let issue_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 1,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{}\",\"label\":\"issued by ADMIN\",\"expires_at_us\":\"{}\"}}}}",
                id(231).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
                now_us + 3_600_000_000
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let issue_prepare = match issue_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN issue prepare unexpectedly started".into());
        }
    };
    let issue_operation_id = issue_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN issue operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN issue operation ID")?;
    let issue = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 2,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{issue_operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let mut issue = match issue {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN issue start returned a worker invocation".into());
        }
    };
    assert!(issue.contains("\"outcome\":\"committed\""));
    let issued_token_id = issue
        .split("\"token_id\":\"")
        .nth(1)
        .ok_or("missing one-time issued token ID")?
        .get(..32)
        .ok_or("invalid one-time issued token ID")?;
    let issued_secret = issue
        .split("\"token_secret\":\"")
        .nth(1)
        .ok_or("missing one-time issued token secret")?
        .get(..44)
        .ok_or("invalid one-time issued token secret")?;
    let mut credentials_payload = Zeroizing::new(format!(
        "{{\"token_id\":\"{issued_token_id}\",\"token_secret\":\"{issued_secret}\"}}"
    ));
    let credentials =
        prdb_engine::client_protocol::decode_client_credentials(credentials_payload.as_bytes())?;
    credentials_payload.zeroize();
    let mut issued_session = ClientConnectionSession::default();
    issued_session.accept_hello(ClientMode::Query, "new-token")?;
    issued_session.authenticate(&storage, credentials.token_id, &credentials.token_secret)?;
    assert!(storage
        .list_tokens(admin_id, ordinary_id)?
        .iter()
        .any(|token| token.token_id == credentials.token_id && token.label == "issued by ADMIN"));
    let issuance_status = control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 7,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{issue_operation_id}\"}}").into_bytes(),
        },
    )?;
    assert_eq!(issuance_status.payload, b"{\"state\":\"committed\"}");
    issue.zeroize();
    let prepare_revoke = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 3,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.revoke\",\"arguments\":{{\"token_id\":\"{}\"}}}}",
                id(240).iter().map(|byte| format!("{byte:02x}")).collect::<String>()
            ).into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let prepare_revoke = match prepare_revoke {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN revoke prepare unexpectedly started".into());
        }
    };
    let operation_id = prepare_revoke
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN operation ID")?;
    assert!(prepare_revoke.ends_with("\",\"state\":\"prepared\"}"));
    let wrong_operation = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 4,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
                format!("{:02x}", 241).repeat(16)
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    );
    assert!(matches!(
        wrong_operation,
        Err(prdb_engine::supervisor_ipc::GatewayRequestError::Client(
            prdb_engine::client_protocol::ClientSessionError::WrongMode
        ))
    ));
    let revoke = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 5,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let revoke = match revoke {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN revoke start returned a worker invocation".into());
        }
    };
    drop(revoke);
    let mut manager_recovery_control = ClientConnectionSession::default();
    manager_recovery_control.accept_hello(ClientMode::Control, "token-manager-recovery")?;
    manager_recovery_control.authenticate(&storage, id(241), &[0x3c; 32])?;
    let owner_reconciled_revoke = manager_recovery_control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 6,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{operation_id}\"}}").into_bytes(),
        },
    )?;
    assert_eq!(
        owner_reconciled_revoke.payload,
        b"{\"state\":\"committed\"}"
    );
    let create_user_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut manager_recovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 7,
            payload: br#"{"phase":"prepare","operation_version":1,"operation":"users.create","arguments":{"name":"new_runtime_user"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("users.create PREPARE failed: {error:?}"))?;
    let create_user_prepare = match create_user_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN user-create prepare unexpectedly started an invocation".into());
        }
    };
    let create_user_operation_id = create_user_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN user-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN user-create operation ID")?;
    let create_user = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut manager_recovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 8,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{create_user_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("users.create START failed: {error:?}"))?;
    let create_user = match create_user {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN user-create start returned a worker invocation".into());
        }
    };
    assert!(create_user.contains("\"outcome\":\"committed\""));
    let created_user_id_wire = create_user
        .split("\"user_id\":\"")
        .nth(1)
        .ok_or("missing created user ID")?
        .get(..32)
        .ok_or("invalid created user ID")?;
    let created_user_id: [u8; 16] = std::array::from_fn(|index| {
        u8::from_str_radix(&created_user_id_wire[index * 2..index * 2 + 2], 16).unwrap()
    });
    let create_user_issue_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut manager_recovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 9,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{created_user_id_wire}\",\"label\":\"new user login\",\"expires_at_us\":\"{}\"}}}}",
                now_us + 3_600_000_000
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("new-user token PREPARE failed: {error:?}"))?;
    let create_user_issue_prepare = match create_user_issue_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("new-user token prepare unexpectedly started an invocation".into());
        }
    };
    let create_user_issue_operation_id = create_user_issue_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing new-user token operation ID")?
        .get(..32)
        .ok_or("invalid new-user token operation ID")?;
    let create_user_issue = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut manager_recovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 10,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{create_user_issue_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("new-user token START failed: {error:?}"))?;
    let mut create_user_issue = match create_user_issue {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("new-user token start returned a worker invocation".into());
        }
    };
    let created_user_credentials = Zeroizing::new(format!(
        "{{\"token_id\":\"{}\",\"token_secret\":\"{}\"}}",
        create_user_issue
            .split("\"token_id\":\"")
            .nth(1)
            .ok_or("missing new-user token ID")?
            .get(..32)
            .ok_or("invalid new-user token ID")?,
        create_user_issue
            .split("\"token_secret\":\"")
            .nth(1)
            .ok_or("missing new-user token secret")?
            .get(..44)
            .ok_or("invalid new-user token secret")?
    ));
    let created_user_credentials = prdb_engine::client_protocol::decode_client_credentials(
        created_user_credentials.as_bytes(),
    )?;
    let mut created_user_session = ClientConnectionSession::default();
    created_user_session.accept_hello(ClientMode::Query, "new-runtime-user")?;
    created_user_session
        .authenticate(
            &storage,
            created_user_credentials.token_id,
            &created_user_credentials.token_secret,
        )
        .map_err(|error| format!("new-user AUTH failed: {error:?}"))?;
    assert_eq!(storage.list_tokens(admin_id, created_user_id)?.len(), 1);
    create_user_issue.zeroize();
    let reconciled_revoke = control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 8,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{operation_id}\"}}").into_bytes(),
        },
    )?;
    assert_eq!(reconciled_revoke.payload, b"{\"state\":\"committed\"}");
    let create_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 9,
            payload: br#"{"phase":"prepare","operation_version":1,"operation":"roles.create","arguments":{"name":"runtime_readers"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let create_role_prepare = match create_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-create prepare unexpectedly started an invocation".into());
        }
    };
    let create_role_operation_id = create_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN role-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN role-create operation ID")?;
    let create_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 10,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{create_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let create_role = match create_role {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-create start returned a worker invocation".into());
        }
    };
    assert!(create_role.contains("\"outcome\":\"committed\""));
    let created_role_id_wire = create_role
        .split("\"role_id\":\"")
        .nth(1)
        .ok_or("missing created role ID")?
        .get(..32)
        .ok_or("invalid created role ID")?;
    let created_role_id: [u8; 16] = std::array::from_fn(|index| {
        u8::from_str_radix(&created_role_id_wire[index * 2..index * 2 + 2], 16).unwrap()
    });
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let role_bytes = catalog
        .get(id_catalog_key(10, created_role_id).as_slice())?
        .ok_or("created role principal is missing")?
        .value()
        .to_vec();
    drop(catalog);
    drop(read);
    let role_record = decode_registered_envelope(&role_bytes)?;
    assert_eq!(
        role_record
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .ok_or("created role has no kind")?
            .payload,
        2u16.to_le_bytes()
    );
    assert_eq!(
        role_record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("created role has no login state")?
            .payload,
        [0]
    );
    let grant_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 11,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"roles.grant\",\"arguments\":{{\"member_id\":\"{created_user_id_wire}\",\"role_id\":\"{created_role_id_wire}\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let grant_role_prepare = match grant_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-grant prepare unexpectedly started an invocation".into());
        }
    };
    let grant_role_operation_id = grant_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN role-grant operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN role-grant operation ID")?;
    let grant_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 12,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{grant_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let grant_role = match grant_role {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-grant start returned a worker invocation".into());
        }
    };
    assert!(grant_role.contains("\"outcome\":\"committed\""));
    let role_session = storage.authenticate_query_session(
        created_user_credentials.token_id,
        &created_user_credentials.token_secret,
    )?;
    assert!(role_session.role_ids().contains(&created_role_id));
    let revoke_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 13,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"roles.revoke\",\"arguments\":{{\"member_id\":\"{created_user_id_wire}\",\"role_id\":\"{created_role_id_wire}\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let revoke_role_prepare = match revoke_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-revoke prepare unexpectedly started an invocation".into());
        }
    };
    let revoke_role_operation_id = revoke_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN role-revoke operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN role-revoke operation ID")?;
    let revoke_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 14,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{revoke_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let revoke_role = match revoke_role {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-revoke start returned a worker invocation".into());
        }
    };
    assert!(revoke_role.contains("\"outcome\":\"committed\""));
    let role_session = storage.authenticate_query_session(
        created_user_credentials.token_id,
        &created_user_credentials.token_secret,
    )?;
    assert!(!role_session.role_ids().contains(&created_role_id));
    let privilege_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 15,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"privileges.grant\",\"arguments\":{{\"grantee_id\":\"{created_user_id_wire}\",\"privilege\":\"manage_roles\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let privilege_prepare = match privilege_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN privilege grant prepare unexpectedly started an invocation".into());
        }
    };
    let privilege_operation_id = privilege_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN privilege operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN privilege operation ID")?;
    let privilege_grant = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 16,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{privilege_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let privilege_grant = match privilege_grant {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN privilege grant start returned a worker invocation".into());
        }
    };
    assert!(privilege_grant.contains("\"outcome\":\"committed\""));
    let mut delegated_admin = ClientConnectionSession::default();
    delegated_admin.accept_hello(ClientMode::Control, "delegated-role-manager")?;
    delegated_admin.authenticate(
        &storage,
        created_user_credentials.token_id,
        &created_user_credentials.token_secret,
    )?;
    let delegated_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut delegated_admin,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 1,
            payload: br#"{"phase":"prepare","operation_version":1,"operation":"roles.create","arguments":{"name":"delegated_role"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let delegated_role_prepare = match delegated_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("delegated role-create prepare unexpectedly started an invocation".into());
        }
    };
    let delegated_role_operation_id = delegated_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing delegated role-create operation ID")?
        .get(..32)
        .ok_or("invalid delegated role-create operation ID")?;
    let delegated_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut delegated_admin,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 2,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{delegated_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    assert!(matches!(
        &delegated_role,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response)
            if String::from_utf8_lossy(&response.payload).contains("\"outcome\":\"committed\"")
    ));
    let delegated_role_response = match &delegated_role {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8_lossy(&response.payload)
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => unreachable!(),
    };
    let delegated_role_id = delegated_role_response
        .split("\"role_id\":\"")
        .nth(1)
        .ok_or("missing delegated role ID")?
        .get(..32)
        .ok_or("invalid delegated role ID")?;
    let drop_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut delegated_admin,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 3,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"roles.drop\",\"arguments\":{{\"role_id\":\"{delegated_role_id}\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let drop_role_prepare = match drop_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-drop prepare unexpectedly started an invocation".into());
        }
    };
    let drop_role_operation_id = drop_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing delegated role-drop operation ID")?
        .get(..32)
        .ok_or("invalid delegated role-drop operation ID")?;
    let drop_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut delegated_admin,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 4,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{drop_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    assert!(matches!(
        drop_role,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response)
            if String::from_utf8_lossy(&response.payload).contains("\"outcome\":\"committed\"")
    ));
    let login_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 17,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"users.login\",\"arguments\":{{\"user_id\":\"{created_user_id_wire}\",\"enabled\":\"false\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let login_prepare = match login_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN user-login prepare unexpectedly started an invocation".into());
        }
    };
    let login_operation_id = login_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN user-login operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN user-login operation ID")?;
    let login_change = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 18,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{login_operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    assert!(matches!(
        login_change,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response)
            if String::from_utf8_lossy(&response.payload).contains("\"outcome\":\"committed\"")
    ));
    let schema_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 19,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.create\",\"arguments\":{{\"name\":\"protocol_schema\",\"owner_id\":\"{}\"}}}}",
                admin_id.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let schema_prepare = match schema_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN schema-create prepare unexpectedly started an operation".into());
        }
    };
    let schema_operation_id = schema_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN schema-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN schema-create operation ID")?;
    let schema_create = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 20,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{schema_operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let schema_create = match schema_create {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN schema-create start returned a worker invocation".into());
        }
    };
    assert!(schema_create.contains("\"outcome\":\"committed\""));
    assert!(schema_create.contains("\"schema_id\":"));
    let created_schema_id = schema_create
        .split("\"schema_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN schema ID")?
        .get(..32)
        .ok_or("invalid ADMIN schema ID")?;
    let table_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 21,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.create\",\"arguments\":{{\"schema_id\":\"{created_schema_id}\",\"name\":\"protocol_table\",\"columns\":[{{\"name\":\"id\",\"type\":\"uuid\",\"nullable\":false,\"generated_uuid_identity\":true}},{{\"name\":\"label\",\"type\":\"text\",\"nullable\":true,\"generated_uuid_identity\":false}}]}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let table_prepare = match table_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN table-create prepare unexpectedly started an operation".into());
        }
    };
    let table_operation_id = table_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN table-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN table-create operation ID")?;
    let table_create = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 22,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{table_operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let table_create = match table_create {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN table-create start returned a worker invocation".into());
        }
    };
    assert!(table_create.contains("\"outcome\":\"committed\""));
    assert!(table_create.contains("\"table_id\":"));
    assert!(table_create.contains("\"column_ids\":[\""));
    let table_id_wire = table_create
        .split("\"table_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN table ID")?
        .get(..32)
        .ok_or("invalid ADMIN table ID")?;
    let column_ids_wire = table_create
        .split("\"column_ids\":[\"")
        .nth(1)
        .ok_or("missing ADMIN column IDs")?;
    let first_column_id_wire = column_ids_wire
        .get(..32)
        .ok_or("invalid first ADMIN column ID")?;
    let default_column_id_wire = column_ids_wire
        .split("\",\"")
        .nth(1)
        .and_then(|wire| wire.get(..32))
        .ok_or("invalid second ADMIN column ID")?;
    let add_column_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        23,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.add_column\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"column\":{{\"name\":\"note\",\"type\":\"text\",\"nullable\":true,\"generated_uuid_identity\":false}}}}}}"
        ),
        60_000_000,
    )?;
    let add_column_operation_id = add_column_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN add-column operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN add-column operation ID")?;
    let add_column = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        24,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{add_column_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(add_column.contains("\"outcome\":\"committed\""));
    assert!(add_column.contains("\"schema_version\":\"2\""));
    let default_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        25,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.set_column_default\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"column_id\":\"{default_column_id_wire}\",\"type\":\"text\",\"default\":{{\"kind\":\"literal\",\"value\":\"ready\"}}}}}}"
        ),
        60_000_000,
    )?;
    let default_operation_id = default_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN default operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN default operation ID")?;
    let default_set = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        26,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{default_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(default_set.contains("\"outcome\":\"committed\""));
    let default_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        27,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.drop_column_default\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"column_id\":\"{default_column_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let default_drop_operation_id = default_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN default-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN default-drop operation ID")?;
    let default_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        28,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{default_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(default_drop.contains("\"outcome\":\"committed\""));
    let column_rename_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        33,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.rename_column\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"column_id\":\"{first_column_id_wire}\",\"name\":\"row_id\"}}}}"
        ),
        60_000_000,
    )?;
    let column_rename_id = column_rename_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN column-rename operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN column-rename operation ID")?;
    let column_rename = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        34,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{column_rename_id}\"}}"),
        60_000_000,
    )?;
    assert!(column_rename.contains("\"outcome\":\"committed\""));
    assert!(column_rename.contains(&format!("\"column_id\":\"{first_column_id_wire}\"")));
    assert!(column_rename.contains("\"name\":\"row_id\""));
    let owner_transfer_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        35,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.set_owner\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"owner_id\":\"{created_user_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let owner_transfer_id = owner_transfer_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN table-owner transfer operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN table-owner transfer operation ID")?;
    let owner_transfer = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        36,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{owner_transfer_id}\"}}"),
        60_000_000,
    )?;
    assert!(owner_transfer.contains("\"outcome\":\"committed\""));
    assert!(owner_transfer.contains(&format!("\"owner_id\":\"{created_user_id_wire}\"")));
    let table_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        37,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.drop\",\"arguments\":{{\"table_id\":\"{table_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let table_drop_id = table_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN table-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN table-drop operation ID")?;
    let table_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        38,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{table_drop_id}\"}}"),
        60_000_000,
    )?;
    assert!(table_drop.contains("\"outcome\":\"committed\""));
    assert!(table_drop.contains(&format!("\"table_id\":\"{table_id_wire}\"")));
    let admin_id_wire = admin_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let empty_schema_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        39,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.create\",\"arguments\":{{\"name\":\"protocol_empty\",\"owner_id\":\"{admin_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let empty_schema_create_id = empty_schema_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing empty-schema create operation ID")?
        .get(..32)
        .ok_or("invalid empty-schema create operation ID")?;
    let empty_schema_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        40,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{empty_schema_create_id}\"}}"),
        60_000_000,
    )?;
    let empty_schema_id = empty_schema_create
        .split("\"schema_id\":\"")
        .nth(1)
        .ok_or("missing empty schema ID")?
        .get(..32)
        .ok_or("invalid empty schema ID")?;
    let schema_owner_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        41,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.set_owner\",\"arguments\":{{\"schema_id\":\"{empty_schema_id}\",\"owner_id\":\"{created_user_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let schema_owner_operation_id = schema_owner_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN schema-owner transfer operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN schema-owner transfer operation ID")?;
    let schema_owner_transfer = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        42,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{schema_owner_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(schema_owner_transfer.contains("\"outcome\":\"committed\""));
    assert!(schema_owner_transfer.contains(&format!("\"owner_id\":\"{created_user_id_wire}\"")));
    let schema_rename_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        43,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.rename\",\"arguments\":{{\"schema_id\":\"{empty_schema_id}\",\"name\":\"protocol_renamed\"}}}}"
        ),
        60_000_000,
    )?;
    let schema_rename_operation_id = schema_rename_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN schema-rename operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN schema-rename operation ID")?;
    let schema_rename = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        44,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{schema_rename_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(schema_rename.contains("\"outcome\":\"committed\""));
    assert!(schema_rename.contains("\"name\":\"protocol_renamed\""));
    let empty_schema_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        45,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.drop\",\"arguments\":{{\"schema_id\":\"{empty_schema_id}\"}}}}"
        ),
        60_000_000,
    )?;
    let empty_schema_drop_id = empty_schema_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing empty-schema drop operation ID")?
        .get(..32)
        .ok_or("invalid empty-schema drop operation ID")?;
    let empty_schema_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        46,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{empty_schema_drop_id}\"}}"),
        60_000_000,
    )?;
    assert!(empty_schema_drop.contains("\"outcome\":\"committed\""));
    assert!(empty_schema_drop.contains(&format!("\"schema_id\":\"{empty_schema_id}\"")));

    let mut foreign_key_setup_audit = identity_audit_event(0xd1, admin_role, id(80));
    foreign_key_setup_audit.action = AuditAction::AuthorityChange;
    let mut fk_setup_writes = foreign_key_catalog_writes(true, true)
        .into_iter()
        .filter(|write| write.key != id_catalog_key(6, id(84)))
        .collect::<Vec<_>>();
    fk_setup_writes.push(column_write_not_null(id(84), id(82), "object_84"));
    storage
        .commit_catalog_and_audit(
            id(0xd2),
            admin_role,
            60_000_000,
            &fk_setup_writes,
            &[foreign_key_setup_audit],
        )
        .map_err(|error| format!("FK ADMIN fixture setup failed: {error:?}"))?;
    let mut references_audit = identity_audit_event(0xd3, admin_role, backup_admin_id);
    references_audit.action = AuditAction::AuthorityChange;
    let mut references_admin_epoch =
        principal_write_with_name(backup_admin_id, 1, 2, "backup_admin");
    references_admin_epoch.expected_value =
        principal_write_with_name(backup_admin_id, 1, 1, "backup_admin").payload;
    storage
        .commit_catalog_and_audit(
            id(0xd3),
            admin_role,
            60_000_000,
            &[
                grant_write(backup_admin_id, id(81), 6, 1, id(81), admin_role),
                dependency_write(backup_admin_id, id(81), 5),
                references_admin_epoch,
            ],
            &[references_audit],
        )
        .map_err(|error| format!("FK REFERENCES grant setup failed: {error:?}"))?;
    let source_table_id_wire = id(82)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let source_column_id_wire = id(84)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let target_constraint_id_wire = id(85)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut foreign_key_control = ClientConnectionSession::default();
    foreign_key_control.accept_hello(ClientMode::Control, "fk-admin")?;
    foreign_key_control.authenticate(&storage, id(236), &[0x6a; 32])?;
    let unique_table_id_wire = id(82)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let unique_column_id_wire = id(84)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let primary_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        47,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.create_primary_key\",\"arguments\":{{\"table_id\":\"{unique_table_id_wire}\",\"column_ids\":[\"{unique_column_id_wire}\"]}}}}"
        ),
        60_000_000,
    )?;
    let primary_operation_id = primary_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN PRIMARY KEY operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN PRIMARY KEY operation ID")?;
    let primary_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        48,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{primary_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(primary_create.contains("\"outcome\":\"committed\""));
    assert!(primary_create.contains("\"index_id\":"));
    let primary_constraint_id_wire = primary_create
        .split("\"constraint_id\":\"")
        .nth(1)
        .ok_or("missing created PRIMARY KEY ID")?
        .get(..32)
        .ok_or("invalid created PRIMARY KEY ID")?;
    let unique_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        49,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.create_unique\",\"arguments\":{{\"table_id\":\"{unique_table_id_wire}\",\"column_ids\":[\"{unique_column_id_wire}\"]}}}}"
        ),
        60_000_000,
    )?;
    let unique_operation_id = unique_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN UNIQUE operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN UNIQUE operation ID")?;
    let unique_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        50,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{unique_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(unique_create.contains("\"outcome\":\"committed\""));
    assert!(unique_create.contains("\"constraint_id\":"));
    let unique_constraint_id_wire = unique_create
        .split("\"constraint_id\":\"")
        .nth(1)
        .ok_or("missing created UNIQUE ID")?
        .get(..32)
        .ok_or("invalid created UNIQUE ID")?;
    let check_eir = prdb_engine::expression::encode_expression(
        &prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(true))),
    )?;
    let check_eir_hex = check_eir
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let check_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        51,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.create_check\",\"arguments\":{{\"table_id\":\"{unique_table_id_wire}\",\"expression_eir1\":\"{check_eir_hex}\",\"dependency_column_ids\":[]}}}}"
        ),
        60_000_000,
    )?;
    let check_operation_id = check_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN CHECK operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN CHECK operation ID")?;
    let check_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        52,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{check_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(check_create.contains("\"outcome\":\"committed\""));
    let check_constraint_id_wire = check_create
        .split("\"constraint_id\":\"")
        .nth(1)
        .ok_or("missing created CHECK constraint ID")?
        .get(..32)
        .ok_or("invalid created CHECK constraint ID")?;
    let check_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        53,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.drop_check\",\"arguments\":{{\"constraint_id\":\"{check_constraint_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let check_drop_operation_id = check_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN CHECK-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN CHECK-drop operation ID")?;
    let check_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        54,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{check_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(check_drop.contains("\"outcome\":\"committed\""));
    let foreign_key_create_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        55,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"foreign_keys.create\",\"arguments\":{{\"source_table_id\":\"{source_table_id_wire}\",\"source_column_ids\":[\"{source_column_id_wire}\"],\"target_constraint_id\":\"{target_constraint_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let foreign_key_create_operation_id = foreign_key_create_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN foreign-key-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN foreign-key-create operation ID")?;
    let foreign_key_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        56,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{foreign_key_create_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(foreign_key_create.contains("\"outcome\":\"committed\""));
    let foreign_key_id_wire = foreign_key_create
        .split("\"foreign_key_id\":\"")
        .nth(1)
        .ok_or("missing created foreign-key ID")?
        .get(..32)
        .ok_or("invalid created foreign-key ID")?;
    let foreign_key_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        57,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"foreign_keys.drop\",\"arguments\":{{\"foreign_key_id\":\"{foreign_key_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let foreign_key_drop_operation_id = foreign_key_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN foreign-key-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN foreign-key-drop operation ID")?;
    let foreign_key_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        58,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{foreign_key_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(foreign_key_drop.contains("\"outcome\":\"committed\""));
    assert!(foreign_key_drop.contains(&format!("\"foreign_key_id\":\"{foreign_key_id_wire}\"")));
    let unique_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        59,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.drop_unique\",\"arguments\":{{\"constraint_id\":\"{unique_constraint_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let unique_drop_operation_id = unique_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN UNIQUE-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN UNIQUE-drop operation ID")?;
    let unique_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        60,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{unique_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(unique_drop.contains("\"outcome\":\"committed\""));
    let index_create_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        63,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"indexes.create\",\"arguments\":{{\"table_id\":\"{unique_table_id_wire}\",\"name\":\"protocol_lookup\",\"column_ids\":[\"{unique_column_id_wire}\"],\"unique\":false}}}}"
        ),
        60_000_000,
    )?;
    let index_create_operation_id = index_create_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN index-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN index-create operation ID")?;
    let index_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        64,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{index_create_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(index_create.contains("\"outcome\":\"committed\""));
    let index_id_wire = index_create
        .split("\"index_id\":\"")
        .nth(1)
        .ok_or("missing created ordinary index ID")?
        .get(..32)
        .ok_or("invalid created ordinary index ID")?;
    let index_rebuild_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        65,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"indexes.rebuild\",\"arguments\":{{\"index_id\":\"{index_id_wire}\",\"column_ids\":[\"{unique_column_id_wire}\"],\"unique\":true}}}}"
        ),
        60_000_000,
    )?;
    let index_rebuild_operation_id = index_rebuild_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN index-rebuild operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN index-rebuild operation ID")?;
    let index_rebuild = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        66,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{index_rebuild_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(index_rebuild.contains("\"outcome\":\"committed\""));
    let index_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        67,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"indexes.drop\",\"arguments\":{{\"index_id\":\"{index_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let index_drop_operation_id = index_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN index-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN index-drop operation ID")?;
    let index_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        68,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{index_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(index_drop.contains("\"outcome\":\"committed\""));
    assert!(index_drop.contains(&format!("\"index_id\":\"{index_id_wire}\"")));
    let primary_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        69,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.drop_primary_key\",\"arguments\":{{\"constraint_id\":\"{primary_constraint_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let primary_drop_operation_id = primary_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN PRIMARY KEY-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN PRIMARY KEY-drop operation ID")?;
    let primary_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        70,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{primary_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(primary_drop.contains("\"outcome\":\"committed\""));
    prdb_engine::startup::validate_persisted_records(&database)?;

    assert!(storage.list_tokens(admin_id, created_user_id)?[0].revoked);
    assert!(
        storage
            .list_tokens(admin_id, user_manager_id)?
            .iter()
            .any(|token| token.token_id == id(240) && token.revoked)
    );
    assert!(matches!(
        storage.get_administrative_invocation_status(admin_id, id(233), now_us)?,
        Some(snapshot) if matches!(snapshot.status,
            InvocationStatus::Active { cancellation_requested: true, .. })
    ));

    let revocation_cursor = storage
        .list_runtime_invocations(admin_id, now_us, 1, None)?
        .continuation
        .unwrap();
    let membership = membership_write_with_grantor(admin_id, admin_role, admin_role);
    let dependency = dependency_write(admin_id, admin_role, 5);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let membership_value = catalog
        .get(membership.key.as_slice())?
        .ok_or("missing admin membership")?
        .value()
        .to_vec();
    let dependency_value = catalog
        .get(dependency.key.as_slice())?
        .ok_or("missing admin dependency")?
        .value()
        .to_vec();
    drop(catalog);
    drop(read);
    let removals = [
        catalog_delete(membership, membership_value),
        catalog_delete(dependency, dependency_value),
    ];
    let mut admin_epoch = principal_write_with_name(admin_id, 1, 2, "runtime_admin");
    let read = database.begin_read()?;
    admin_epoch.expected_value = Some(
        read.open_table(CATALOG)?
            .get(id_catalog_key(10, admin_id).as_slice())?
            .ok_or("missing admin principal")?
            .value()
            .to_vec(),
    );
    drop(read);
    let mut revoke_audit = audit_event(217);
    revoke_audit.actor_id = Some(backup_admin_id);
    revoke_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_authorized_membership_ddl(
            id(217),
            backup_admin_id,
            9_999,
            &[removals[0].clone(), removals[1].clone(), admin_epoch],
            &[revoke_audit],
        )
        .map_err(|error| format!("admin revocation commit: {error:?}"))?;
    assert!(matches!(
        storage.list_runtime_invocations(admin_id, now_us, 1, Some(&revocation_cursor)),
        Err(AdministrativeInvocationListError::Unauthorized)
    ));

    second.abort()?;
    first.abort()?;
    Ok(())
}

#[test]
fn storage_manager_audits_cancellation_before_commit_and_preserves_terminal_outcomes()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::audit::{AuditEvent, AuditResult};
    use prdb_engine::storage_manager::{
        AdministrativeCancellationError, AdministrativeCancellationResult, InvocationStatus,
    };
    use prdb_engine::transaction::TransactionError;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            ddl_fence_timeout: Duration::from_millis(100),
            max_active_invocations: 1,
            max_invocation_age: Duration::from_secs(15 * 60),
            max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 2,
        },
    )?;
    let admin_id = id(240);
    let owner_id = id(241);
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(admin_id, 1, 1, "cancel_admin"),
        object_write_with_name(admin_id, 8, "cancel_admin"),
        principal_write_with_name(owner_id, 1, 1, "cancel_owner"),
        object_write_with_name(owner_id, 8, "cancel_owner"),
        membership_write_with_grantor(admin_id, admin_role, admin_role),
        dependency_write(admin_id, admin_role, 5),
        credential_verifier_write(id(242), admin_id, &[0x6b; 32], 1, i64::MAX, None, 1),
    ];
    let mut setup_audit = audit_event(238);
    setup_audit.actor_id = Some(admin_role);
    storage.commit_catalog_and_audit(id(238), admin_role, 9_999, &records, &[setup_audit])?;

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;
    let cancellation_audit = |event_id, actor_id, target_id| AuditEvent {
        event_id: id(event_id),
        actor_id: Some(actor_id),
        correlation_id: id(event_id.wrapping_add(1)),
        occurred_at_us: now_us,
        action: AuditAction::AdministrativeCancellation,
        result: AuditResult::Denied,
        target_ids: vec![target_id],
    };

    let mut pending = storage.begin_invocation(id(243), owner_id, retention_deadline_us, 2, 64)?;
    let cancelled_row_key = table_row_key(id(250), id(251));
    pending.stage_put(
        prdb_engine::overlay::RowId {
            table: id(250),
            row: id(251),
        },
        b"must be discarded",
    )?;
    assert!(matches!(
        storage
            .clone()
            .begin_invocation(id(252), owner_id, retention_deadline_us, 2, 64),
        Err(TransactionError::AdmissionLimitExceeded)
    ));
    assert!(matches!(
        storage.cancel_invocation(
            owner_id,
            id(243),
            7,
            now_us,
            cancellation_audit(244, owner_id, id(243)),
        ),
        Err(AdministrativeCancellationError::Unauthorized)
    ));
    assert!(matches!(
        storage
            .get_invocation_status(id(243), owner_id, now_us)?
            .unwrap()
            .status,
        InvocationStatus::Active {
            cancellation_requested: false,
            ..
        }
    ));
    assert_eq!(
        storage.cancel_invocation(
            admin_id,
            id(243),
            7,
            now_us,
            cancellation_audit(245, admin_id, id(243)),
        )?,
        AdministrativeCancellationResult::Accepted
    );
    let administrative_page = storage.list_runtime_invocations(admin_id, now_us, 1, None)?;
    assert_eq!(
        administrative_page.invocations[0].cancellation_reason_code,
        Some(7)
    );
    assert!(matches!(
        storage
            .get_invocation_status(id(243), owner_id, now_us)?
            .unwrap()
            .status,
        InvocationStatus::Active {
            cancellation_requested: true,
            ..
        }
    ));
    assert!(matches!(
        pending.check_deadline(),
        Err(TransactionError::CancellationRequested)
    ));
    assert_eq!(
        lookup_outcome(&database, id(243), owner_id, now_us)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert!(row(&database, &cancelled_row_key)?.is_none());
    let mut after_cancel =
        storage.begin_invocation(id(253), owner_id, retention_deadline_us, 2, 64)?;
    after_cancel.abort()?;
    let audit_read = database.begin_read()?;
    let audit_table = audit_read.open_table(schema::AUDIT)?;
    let audit = audit_table
        .get(id(245).as_slice())?
        .ok_or("missing accepted cancellation audit")?;
    assert_eq!(
        AuditEvent::decode(audit.value())?.result,
        AuditResult::Accepted
    );
    drop(audit);
    drop(audit_table);
    drop(audit_read);

    let committed = storage.begin_invocation(id(246), owner_id, retention_deadline_us, 2, 64)?;
    let commit_sequence = committed.commit()?;
    assert!(matches!(
        storage.cancel_invocation(
            admin_id,
            id(246),
            8,
            now_us,
            cancellation_audit(247, admin_id, id(246)),
        )?,
        AdministrativeCancellationResult::Terminal(InvocationOutcome {
            status: OutcomeStatus::Committed,
            commit_sequence: committed_sequence,
            ..
        }) if committed_sequence == commit_sequence
    ));
    assert!(matches!(
        storage.cancel_invocation(
            admin_id,
            id(248),
            9,
            now_us,
            cancellation_audit(249, admin_id, id(248)),
        ),
        Err(AdministrativeCancellationError::RateLimited)
    ));
    Ok(())
}

#[test]
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn admin_cancellation_sync_failure_quarantines_until_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    struct AcceptMetadata;
    impl prdb_engine::supervisor::SubmissionMetadataValidator for AcceptMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &prdb_engine::supervisor::SubmissionMetadata) -> bool {
            true
        }
    }

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_ADMIN_CANCEL_SYNC_FAILURE_DATABASE";
    const ADMIN_ID: [u8; 16] = [0xf0; 16];
    const OWNER_ID: [u8; 16] = [0xf1; 16];
    const INVOCATION_ID: [u8; 16] = [0xf2; 16];
    const AUDIT_ID: [u8; 16] = [0xf3; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let fail_next_sync = Arc::new(AtomicBool::new(false));
        let database = Database::builder().create_with_backend(FailNextSyncBackend {
            inner: redb::backends::FileBackend::new(file)?,
            fail_next_sync: fail_next_sync.clone(),
        })?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let _pending = storage.begin_invocation(INVOCATION_ID, OWNER_ID, i64::MAX, 1, 64)?;
        let now_us = 1_700_000_000_000_000;
        let cancellation_audit = AuditEvent {
            event_id: AUDIT_ID,
            actor_id: Some(ADMIN_ID),
            correlation_id: [0xf4; 16],
            occurred_at_us: now_us,
            action: AuditAction::AdministrativeCancellation,
            result: AuditResult::Denied,
            target_ids: vec![INVOCATION_ID],
        };

        fail_next_sync.store(true, Ordering::SeqCst);
        assert!(matches!(
            storage.cancel_invocation(ADMIN_ID, INVOCATION_ID, 7, now_us, cancellation_audit,),
            Err(AdministrativeCancellationError::Storage)
        ));
        assert!(!fail_next_sync.load(Ordering::SeqCst));
        assert!(storage.requires_reopen_after_commit_error());
        assert!(matches!(
            storage.begin_invocation([0xf5; 16], OWNER_ID, i64::MAX, 1, 64),
            Err(prdb_engine::transaction::TransactionError::Storage(_))
        ));
        assert!(matches!(
            storage.get_invocation_status(INVOCATION_ID, OWNER_ID, now_us),
            Err(prdb_engine::storage_manager::InvocationStatusError::Storage)
        ));
        let socket_path =
            std::path::PathBuf::from(format!("{}.quarantine.sock", database_path.display()));
        let listener = prdb_engine::supervisor_ipc::bind_gateway_service_socket(&socket_path)?;
        let staging = prdb_engine::supervisor::StagingStore::new(
            prdb_engine::supervisor::StagingLimits::default(),
            Arc::new(AcceptMetadata),
        )?;
        let runtime = prdb_engine::supervisor_ipc::GatewayServiceRuntimeConfig {
            admission: prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
                retention_period_us: i64::MAX,
                max_rows: 1,
                max_bytes: 64,
            },
            executor: prdb_engine::supervisor_ipc::GatewayExecutorConfig {
                cgroup_parent: std::path::Path::new(&database_path)
                    .parent()
                    .ok_or("database fixture has no parent")?
                    .to_path_buf(),
                resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
                    cpu_quota_micros: 100_000,
                    cpu_period_micros: 100_000,
                    memory_max_bytes: 64 * 1024 * 1024,
                    max_processes: 16,
                },
                launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
                    executor_path: std::env::current_exe()?,
                    scratch_parent: std::env::temp_dir(),
                    runtime_read_execute: Vec::new(),
                    runtime_read_only: Vec::new(),
                    cleanup_timeout: Duration::from_millis(100),
                },
            },
            listener: prdb_engine::supervisor_ipc::GatewayServiceListenerConfig {
                expected_gateway_uid: unsafe { libc::geteuid() },
                max_connections: 1,
                idle_timeout: Duration::from_secs(1),
                accept_poll_interval: Duration::from_millis(1),
                outcome_maintenance_interval: Duration::from_secs(60),
                outcome_maintenance_batch: prdb_engine::outcome::MAX_OUTCOME_PRUNE_BATCH,
            },
        };
        assert!(matches!(
            prdb_engine::supervisor_ipc::serve_gateway_service_listener(
                listener,
                &storage,
                &staging,
                runtime,
                Arc::new(AtomicBool::new(false)),
                |_manager, _request_id, _stream, _invocation, _submission, _executor| {},
            ),
            Err(prdb_engine::supervisor_ipc::GatewayServiceListenerError::StorageReopenRequired)
        ));
        std::fs::remove_file(socket_path)?;
        // Let process teardown close redb and discard its process-local quarantine.
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    let database = initialize(&database_path)?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(ADMIN_ID, 1, 1, "sync_admin"),
        object_write_with_name(ADMIN_ID, 8, "sync_admin"),
        principal_write_with_name(OWNER_ID, 1, 1, "sync_owner"),
        object_write_with_name(OWNER_ID, 8, "sync_owner"),
        membership_write_with_grantor(ADMIN_ID, admin_role, admin_role),
        dependency_write(ADMIN_ID, admin_role, 5),
        credential_verifier_write([0xf6; 16], ADMIN_ID, &[0x71; 32], 1, i64::MAX, None, 1),
    ];
    let mut setup_audit = audit_event(0xf7);
    setup_audit.actor_id = Some(admin_role);
    storage.commit_catalog_and_audit(id(0xf7), admin_role, i64::MAX, &records, &[setup_audit])?;
    drop(storage);
    drop(database);

    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("admin_cancellation_sync_failure_quarantines_until_reopen")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .status()?;
    assert!(
        child_status.success(),
        "cancellation fault-injection child exited with {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    assert!(!storage.requires_reopen_after_commit_error());
    let mut resumed = storage.begin_invocation([0xf8; 16], OWNER_ID, i64::MAX, 1, 64)?;
    resumed.abort()?;
    let read = reopened.begin_read()?;
    let audit_table = read.open_table(AUDIT)?;
    if let Some(audit) = audit_table.get(&AUDIT_ID[..])? {
        assert_eq!(
            AuditEvent::decode(audit.value())?.result,
            AuditResult::Accepted
        );
    }
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
    let table_epoch_before_build = {
        let read = database.begin_read()?;
        let record = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(5, id(16)).as_slice())?
                .ok_or("table record missing before index build")?
                .value(),
        )?;
        u64::from_le_bytes(
            record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table write epoch missing before index build")?
                .payload
                .as_slice()
                .try_into()?,
        )
    };
    let stale_writer_invocation = id(0x7c);
    let stale_row_key = table_row_key(id(16), id(0x7d));
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, id(9), 9_999, 1, 64)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(16))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    let stale_payload = bytea_tuple(b"staged before index build");
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(16),
            row: id(0x7d),
        },
        &stale_payload,
    )?;
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
    let table_epoch_after_build = {
        let read = database.begin_read()?;
        let record = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(5, id(16)).as_slice())?
                .ok_or("table record missing after index build")?
                .value(),
        )?;
        u64::from_le_bytes(
            record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table write epoch missing after index build")?
                .payload
                .as_slice()
                .try_into()?,
        )
    };
    assert_eq!(table_epoch_after_build, table_epoch_before_build + 1);
    let stale_result = stale_writer.commit();
    assert!(
        matches!(
            stale_result,
            Err(prdb_engine::transaction::TransactionError::Commit(
                CommitError::Conflict
            ))
        ),
        "writer pinned before index publication returned {stale_result:?}"
    );
    assert!(row(&database, &stale_row_key)?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, id(9), 1)?
            .ok_or("missing stale index-build writer outcome")?
            .status,
        OutcomeStatus::Aborted
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
    let table_epoch_after_drop = {
        let read = database.begin_read()?;
        let record = decode_registered_envelope(
            read.open_table(CATALOG)?
                .get(id_catalog_key(5, id(16)).as_slice())?
                .ok_or("table record missing after index drop")?
                .value(),
        )?;
        u64::from_le_bytes(
            record
                .fields
                .iter()
                .find(|field| field.tag == 3)
                .ok_or("table write epoch missing after index drop")?
                .payload
                .as_slice()
                .try_into()?,
        )
    };
    assert_eq!(table_epoch_after_drop, table_epoch_after_build + 1);
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
    let index_admin = id(0x78);
    let new_column_object = object_write(id(0x67), 4);
    let new_column = column_write_at_ordinal(id(0x67), id(16), 1, "object_103");
    {
        let setup = database.begin_write()?;
        for record in [
            new_column_object,
            new_column,
            principal_write_with_name(index_admin, 1, 1, "index_rebuild_admin"),
            object_write_with_name(index_admin, 8, "index_rebuild_admin"),
            membership_write(index_admin, builtin_owner_id()),
            dependency_write(index_admin, builtin_owner_id(), 5),
        ] {
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
    let stale_writer_invocation = id(0x79);
    let stale_row = id(0x7a);
    let mut stale_writer =
        storage.begin_invocation(stale_writer_invocation, id(9), 9_999, 1, 64)?;
    let stale_schema = stale_writer.load_catalog_table_schema(id(16))?;
    stale_writer.load_catalog_indexes(&stale_schema)?;
    stale_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: id(16),
            row: stale_row,
        },
        &two_bytea_tuple(b"staged", b"before rebuild"),
    )?;
    let mut rebuild_audit = audit_event(205);
    rebuild_audit.actor_id = Some(index_admin);
    rebuild_audit.correlation_id = id(205);
    let rebuilt_result = storage
        .rebuild_index(prdb_engine::storage_manager::RebuildIndexRequest {
            invocation_id: id(205),
            authenticated_actor_id: index_admin,
            index_id: id(0x44),
            column_ids: vec![id(0x67)],
            unique: true,
            retention_deadline_us: 9_999,
            audit_event: rebuild_audit,
        })
        .map_err(|error| format!("typed index rebuild failed: {error:?}"))?;
    assert_eq!(rebuilt_result.commit_sequence, 2);
    let stale_result = stale_writer.commit();
    assert!(
        matches!(
            stale_result,
            Err(prdb_engine::transaction::TransactionError::Commit(
                CommitError::Conflict
            ))
        ),
        "writer pinned before index rebuild returned {stale_result:?}"
    );
    assert!(row(&database, &table_row_key(id(16), stale_row))?.is_none());
    assert_eq!(
        lookup_outcome(&database, stale_writer_invocation, id(9), 1)?
            .ok_or("missing stale index-rebuild writer outcome")?
            .status,
        OutcomeStatus::Aborted
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
        lookup_outcome(&database, id(205), index_admin, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    drop(current);
    drop(old_snapshot);
    let mut repeat_rebuild_audit = audit_event(206);
    repeat_rebuild_audit.actor_id = Some(index_admin);
    repeat_rebuild_audit.correlation_id = id(206);
    let repeat_rebuild = storage
        .rebuild_index(prdb_engine::storage_manager::RebuildIndexRequest {
            invocation_id: id(206),
            authenticated_actor_id: index_admin,
            index_id: id(0x44),
            column_ids: vec![id(0x67)],
            unique: true,
            retention_deadline_us: 9_999,
            audit_event: repeat_rebuild_audit,
        })
        .map_err(|error| format!("idempotent typed index rebuild failed: {error:?}"))?;
    assert_eq!(repeat_rebuild.commit_sequence, 3);
    assert_eq!(
        lookup_outcome(&database, id(206), index_admin, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
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
    let (narrow_guard_index_key, narrow_guard_index_definition) =
        index_definition(id(0x8f), id(16));
    let mut narrow_guard_audit = audit_event(215);
    narrow_guard_audit.actor_id = Some(authorized_user);
    assert_eq!(
        storage.commit_authorized_index_ddl(
            id(215),
            authorized_user,
            9_999,
            &[
                CatalogWrite {
                    key: narrow_guard_index_key.clone(),
                    expected_value: None,
                    payload: Some(narrow_guard_index_definition),
                },
                object_write(id(0x8f), 7),
                dependency_write(id(0x8b), id(16), 1),
            ],
            &[narrow_guard_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        lookup_outcome(&database, id(215), authorized_user, 1)?
            .ok_or("missing rejected unrelated index ownership-edge outcome")?
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
            .get(narrow_guard_index_key.as_slice())?
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
fn index_ddl_racing_table_owner_transfer_rechecks_authority_at_commit()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, SchemaLifecycleError, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let owner = id(0xb1);
    let administrator = id(0xb2);
    let successor = id(0xb3);
    let mut setup_audit = identity_audit_event(0xb4, admin_role, owner);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xb4),
        admin_role,
        9_999,
        &[
            principal_write_with_name(owner, 1, 1, "index_race_owner"),
            object_write_with_name(owner, 8, "index_race_owner"),
            principal_write_with_name(administrator, 1, 1, "index_race_admin"),
            object_write_with_name(administrator, 8, "index_race_admin"),
            membership_write(administrator, admin_role),
            dependency_write(administrator, admin_role, 5),
            principal_write_with_name(successor, 1, 1, "index_race_successor"),
            object_write_with_name(successor, 8, "index_race_successor"),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0xb5);
    let mut schema_audit = identity_audit_event(0xb6, administrator, owner);
    schema_audit.correlation_id = schema_invocation;
    let schema = storage
        .create_schema(
            schema_invocation,
            administrator,
            "index_race_schema",
            owner,
            9_999,
            schema_audit,
        )
        .map_err(|error: SchemaLifecycleError| format!("schema setup failed: {error:?}"))?;
    let table_invocation = id(0xb7);
    let mut table_audit = identity_audit_event(0xb8, owner, schema.schema_id);
    table_audit.correlation_id = table_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: owner,
        schema_id: schema.schema_id,
        name: "indexed_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Bytea,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: table_audit,
    })?;

    let index_id = id(0xb9);
    let (index_key, index_payload) =
        index_definition_with_columns(index_id, table.table_id, &table.column_ids);
    let index_key_for_commit = index_key.clone();
    let index_invocation = id(0xba);
    let transfer_invocation = id(0xbb);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (index_result, transfer_result) = std::thread::scope(|scope| {
        let index_barrier = barrier.clone();
        let index_storage = &storage;
        let index = scope.spawn(move || {
            let mut audit = identity_audit_event(0xbc, owner, table.table_id);
            audit.correlation_id = index_invocation;
            index_barrier.wait();
            index_storage.commit_authorized_index_ddl(
                index_invocation,
                owner,
                9_999,
                &[
                    CatalogWrite {
                        key: index_key_for_commit,
                        expected_value: None,
                        payload: Some(index_payload),
                    },
                    object_write_with_owner(index_id, 7, owner),
                ],
                &[audit],
            )
        });
        let transfer_barrier = barrier.clone();
        let transfer_storage = &storage;
        let transfer = scope.spawn(move || {
            let mut audit = identity_audit_event(0xbd, administrator, table.table_id);
            audit.correlation_id = transfer_invocation;
            transfer_barrier.wait();
            transfer_storage.set_table_owner(
                transfer_invocation,
                administrator,
                table.table_id,
                successor,
                9_999,
                audit,
            )
        });
        barrier.wait();
        (
            index.join().expect("index DDL thread panicked"),
            transfer.join().expect("owner transfer thread panicked"),
        )
    });

    let (index_status, transfer_status) = match (index_result, transfer_result) {
        (Ok(_), Ok(_)) => (OutcomeStatus::Committed, OutcomeStatus::Committed),
        (Ok(_), Err(TableLifecycleError::Commit(CommitError::Conflict))) => {
            (OutcomeStatus::Committed, OutcomeStatus::Aborted)
        }
        (Err(CommitError::DdlAuthorizationDenied), Ok(_)) => {
            (OutcomeStatus::Aborted, OutcomeStatus::Committed)
        }
        (index, transfer) => {
            panic!("unexpected index/owner-transfer race: index={index:?}, transfer={transfer:?}")
        }
    };
    assert_eq!(
        commit_sequence(&database)?,
        before_race
            + u64::from(index_status == OutcomeStatus::Committed)
            + u64::from(transfer_status == OutcomeStatus::Committed)
    );
    assert_eq!(
        lookup_outcome(&database, index_invocation, owner, 1)?
            .ok_or("missing raced index outcome")?
            .status,
        index_status
    );
    assert_eq!(
        lookup_outcome(&database, transfer_invocation, administrator, 1)?
            .ok_or("missing raced owner-transfer outcome")?
            .status,
        transfer_status
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    assert_eq!(
        catalog.get(index_key.as_slice())?.is_some(),
        index_status == OutcomeStatus::Committed
    );
    let table_object = decode_registered_envelope(
        catalog
            .get(id_catalog_key(2, table.table_id).as_slice())?
            .ok_or("table object missing after owner race")?
            .value(),
    )?;
    let final_owner = table_object
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("table owner missing after owner race")?
        .payload
        .as_slice();
    assert_eq!(
        final_owner,
        if transfer_status == OutcomeStatus::Committed {
            successor.as_slice()
        } else {
            owner.as_slice()
        }
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn policy_ddl_racing_table_owner_transfer_rechecks_authority_at_commit()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, SchemaLifecycleError, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let owner = id(0xc1);
    let administrator = id(0xc2);
    let successor = id(0xc3);
    let mut setup_audit = identity_audit_event(0xc4, admin_role, owner);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xc4),
        admin_role,
        9_999,
        &[
            principal_write_with_name(owner, 1, 1, "policy_race_owner"),
            object_write_with_name(owner, 8, "policy_race_owner"),
            principal_write_with_name(administrator, 1, 1, "policy_race_admin"),
            object_write_with_name(administrator, 8, "policy_race_admin"),
            membership_write(administrator, admin_role),
            dependency_write(administrator, admin_role, 5),
            principal_write_with_name(successor, 1, 1, "policy_race_successor"),
            object_write_with_name(successor, 8, "policy_race_successor"),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0xc5);
    let mut schema_audit = identity_audit_event(0xc6, administrator, owner);
    schema_audit.correlation_id = schema_invocation;
    let schema = storage
        .create_schema(
            schema_invocation,
            administrator,
            "policy_race_schema",
            owner,
            9_999,
            schema_audit,
        )
        .map_err(|error: SchemaLifecycleError| format!("schema setup failed: {error:?}"))?;
    let table_invocation = id(0xc7);
    let mut table_audit = identity_audit_event(0xc8, owner, schema.schema_id);
    table_audit.correlation_id = table_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: owner,
        schema_id: schema.schema_id,
        name: "policy_race_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Bytea,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: table_audit,
    })?;

    let policy_invocation = id(0xc9);
    let transfer_invocation = id(0xca);
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(3));
    let (policy_result, transfer_result) = std::thread::scope(|scope| {
        let policy_barrier = barrier.clone();
        let policy_storage = &storage;
        let policy = scope.spawn(move || {
            let mut audit = identity_audit_event(0xcb, owner, table.table_id);
            audit.correlation_id = policy_invocation;
            policy_barrier.wait();
            policy_storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
                invocation_id: policy_invocation,
                authenticated_actor_id: owner,
                table_id: table.table_id,
                name: "owner_policy_race".to_owned(),
                kind: prdb_engine::storage_manager::PolicyKind::Row,
                operation: prdb_engine::authorization::Operation::Select,
                effect: prdb_engine::authorization::Effect::Allow,
                recipients: vec![prdb_engine::authorization::Subject::Principal(owner)],
                target_column_ids: Vec::new(),
                using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                    ScalarValue::Boolean(true),
                ))),
                with_check_expression: None,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
        });
        let transfer_barrier = barrier.clone();
        let transfer_storage = &storage;
        let transfer = scope.spawn(move || {
            let mut audit = identity_audit_event(0xcc, administrator, table.table_id);
            audit.correlation_id = transfer_invocation;
            transfer_barrier.wait();
            transfer_storage.set_table_owner(
                transfer_invocation,
                administrator,
                table.table_id,
                successor,
                9_999,
                audit,
            )
        });
        barrier.wait();
        (
            policy.join().expect("policy DDL thread panicked"),
            transfer.join().expect("owner-transfer thread panicked"),
        )
    });

    let (policy_status, policy_id, transfer_status) = match (policy_result, transfer_result) {
        (Ok(created), Ok(_)) => (
            OutcomeStatus::Committed,
            Some(created.policy_id),
            OutcomeStatus::Committed,
        ),
        (Ok(created), Err(TableLifecycleError::Commit(CommitError::Conflict))) => (
            OutcomeStatus::Committed,
            Some(created.policy_id),
            OutcomeStatus::Aborted,
        ),
        (
            Err(
                TableLifecycleError::Unauthorized
                | TableLifecycleError::Commit(CommitError::Conflict),
            ),
            Ok(_),
        ) => (OutcomeStatus::Aborted, None, OutcomeStatus::Committed),
        (policy, transfer) => {
            panic!(
                "unexpected policy/owner-transfer race: policy={policy:?}, transfer={transfer:?}"
            )
        }
    };
    assert_eq!(
        commit_sequence(&database)?,
        before_race
            + u64::from(policy_status == OutcomeStatus::Committed)
            + u64::from(transfer_status == OutcomeStatus::Committed)
    );
    assert_eq!(
        lookup_outcome(&database, policy_invocation, owner, 1)?
            .ok_or("missing raced policy outcome")?
            .status,
        policy_status
    );
    assert_eq!(
        lookup_outcome(&database, transfer_invocation, administrator, 1)?
            .ok_or("missing raced owner-transfer outcome")?
            .status,
        transfer_status
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    if let Some(policy_id) = policy_id {
        assert!(
            catalog
                .get(id_catalog_key(13, policy_id).as_slice())?
                .is_some()
        );
    }
    let table_object = decode_registered_envelope(
        catalog
            .get(id_catalog_key(2, table.table_id).as_slice())?
            .ok_or("table object missing after policy race")?
            .value(),
    )?;
    let final_owner = table_object
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("table owner missing after policy race")?
        .payload
        .as_slice();
    assert_eq!(
        final_owner,
        if transfer_status == OutcomeStatus::Committed {
            successor.as_slice()
        } else {
            owner.as_slice()
        }
    );
    drop(catalog);
    drop(read);
    prdb_engine::startup::validate_persisted_records(&database)?;
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
    let mut drop_user_audit = identity_audit_event(0xd5, admin_user, dropped_user);
    drop_user_audit.correlation_id = id(0xd5);
    let drop_result =
        storage.drop_identity(id(0xd5), admin_user, dropped_user, 9_999, drop_user_audit);
    assert_eq!(drop_result, Ok(2));
    let mut drop_role_audit = identity_audit_event(0xd8, admin_user, dropped_role);
    drop_role_audit.correlation_id = id(0xd8);
    assert_eq!(
        storage.drop_identity(id(0xd8), admin_user, dropped_role, 9_999, drop_role_audit)?,
        3
    );
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
    drop(catalog);
    drop(read);
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0xd9),
            dropped_token,
            &[0xd4; 32],
            2,
            2,
            64,
        ),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));
    assert_eq!(lookup_outcome(&database, id(0xd9), dropped_user, 2)?, None);
    assert_eq!(commit_sequence(&database)?, 3);
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

#[test]
fn token_revocation_racing_a_staged_write_linearizes() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(500))?;
    let admin_role = builtin_owner_id();
    let admin_user = id(0xfa);
    let user_id = id(0xfb);
    let token_id = id(0xfc);
    let secret = [0xa5; 32];
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_micros(),
    )?;
    let retention_deadline_us = now + 60_000_000;
    let mut setup_audit = identity_audit_event(0xf6, admin_role, admin_user);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0xf6),
        admin_role,
        retention_deadline_us,
        &[
            principal_write_with_name(admin_user, 1, 1, "token_race_admin"),
            object_write_with_name(admin_user, 8, "token_race_admin"),
            membership_write(admin_user, admin_role),
            dependency_write(admin_user, admin_role, 5),
            credential_verifier_write(
                id(0xfd),
                admin_user,
                &[0x5a; 32],
                now - 1,
                now + 60_000_000,
                None,
                1,
            ),
            principal_write_with_name(user_id, 1, 1, "token_race_user"),
            object_write_with_name(user_id, 8, "token_race_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[setup_audit],
    )?;

    let writer_invocation = id(0xfe);
    let (mut writer, roles) = storage.begin_token_authenticated_invocation(
        writer_invocation,
        token_id,
        &secret,
        retention_deadline_us,
        2,
        64,
    )?;
    assert!(roles.is_empty());
    let row_schema = writer.load_catalog_table_schema(id(16))?;
    writer.load_catalog_indexes(&row_schema)?;
    writer.load_catalog_check_constraints(&row_schema)?;
    let row_id = prdb_engine::overlay::RowId {
        table: id(16),
        row: id(0xa6),
    };
    writer.stage_put(row_id, &bytea_tuple(b"committed before token revoke"))?;

    let revoke_invocation = id(0xa7);
    let barrier = Arc::new(Barrier::new(2));
    let (write_result, revoke_result) = std::thread::scope(|scope| {
        let revoke_barrier = barrier.clone();
        let revoke_storage = &storage;
        let revoke = scope.spawn(move || {
            let mut audit = audit_event(0xa8);
            audit.actor_id = Some(admin_user);
            audit.correlation_id = revoke_invocation;
            audit.action = AuditAction::TokenLifecycle;
            audit.target_ids = vec![token_id];
            revoke_barrier.wait();
            revoke_storage.revoke_token(
                revoke_invocation,
                admin_user,
                token_id,
                retention_deadline_us,
                audit,
            )
        });
        barrier.wait();
        let write = writer.commit();
        (
            write,
            revoke.join().expect("token revocation thread panicked"),
        )
    });

    let write_result_debug = format!("{write_result:?}");
    let write_status = match write_result {
        Ok(sequence) => {
            assert!(sequence >= 2);
            OutcomeStatus::Committed
        }
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        Err(error) => panic!("unexpected staged-write result in token race: {error:?}"),
    };
    assert!(
        revoke_result.is_ok(),
        "token revocation failed: {revoke_result:?}"
    );
    let revoke_sequence = revoke_result.unwrap();
    assert_eq!(commit_sequence(&database)?, revoke_sequence);
    assert_eq!(
        revoke_sequence,
        if write_status == OutcomeStatus::Committed {
            3
        } else {
            2
        }
    );
    let writer_outcome = lookup_outcome(&database, writer_invocation, user_id, now)?;
    assert!(
        writer_outcome.is_some(),
        "missing staged-writer race outcome: result={write_result_debug}, revoke_sequence={revoke_sequence}, current_sequence={:?}",
        commit_sequence(&database)?
    );
    assert_eq!(writer_outcome.unwrap().status, write_status);
    assert_eq!(
        lookup_outcome(&database, revoke_invocation, admin_user, now)?
            .ok_or("missing token-revocation race outcome")?
            .status,
        OutcomeStatus::Committed
    );
    let row_key = table_row_key(row_id.table, row_id.row);
    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    assert_eq!(
        rows.get(row_key.as_slice())?.map(|value| {
            RowRecord::decode(value.value())
                .expect("committed row record is valid")
                .payload
        }),
        (write_status == OutcomeStatus::Committed)
            .then(|| bytea_tuple(b"committed before token revoke"))
    );
    drop(rows);
    drop(read);
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0xa9),
            token_id,
            &secret,
            retention_deadline_us,
            2,
            64,
        ),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));
    assert_eq!(lookup_outcome(&database, id(0xa9), user_id, now)?, None);
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
    let grant = grant_write(dropped_user, database_id, 12, 1, database_id, admin_role);
    let mut drop_audit = identity_audit_event(0xe5, admin_user, dropped_user);
    drop_audit.correlation_id = id(0xe5);
    assert_eq!(
        storage.drop_identity(id(0xe5), admin_user, dropped_user, 9_999, drop_audit,),
        Err(prdb_engine::storage_manager::IdentityLifecycleError::Unauthorized)
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
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

    let mut rename_audit = identity_audit_event(0x83, user_manager, created_user);
    rename_audit.correlation_id = id(0x83);
    assert_eq!(
        storage.rename_identity(
            id(0x83),
            user_manager,
            created_user,
            "renamed_user",
            9_999,
            rename_audit,
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

    let mut disable_audit = identity_audit_event(0x8c, user_manager, created_user);
    disable_audit.correlation_id = id(0x8c);
    assert_eq!(
        storage.set_user_login_enabled(
            id(0x8c),
            user_manager,
            created_user,
            false,
            9_999,
            disable_audit,
        )?,
        7
    );
    assert!(storage.list_tokens(user_manager, created_user)?[0].revoked);

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
    let mut rename_role_audit = identity_audit_event(0x9a, admin_user, created_role);
    rename_role_audit.correlation_id = id(0x9a);
    assert_eq!(
        storage.rename_identity(
            id(0x9a),
            admin_user,
            created_role,
            "renamed_created_role",
            9_999,
            rename_role_audit,
        )?,
        9
    );
    assert_eq!(commit_sequence(&database)?, 9);
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
fn storage_manager_authorizes_object_grants_and_linearizes_revocation_races()
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

    use prdb_engine::authorization::{
        AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Effect,
        Expr, Grant, Operation, Policy, Subject,
    };
    use prdb_engine::tuple::FieldPatch;

    let row_id = id(0x81);
    let row_key = table_row_key(id(16), row_id);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[0x82; 16]];
    commit_rows(
        &database,
        id(0x80),
        manager_grantee,
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text("before revoke".to_owned()))],
            )?),
        }],
    )
    .map_err(|error| format!("seed grant-race row: {error:?}"))?;

    let grant_bytes = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(manager_update.key.as_slice())?
        .ok_or("missing update grant before staged write")?
        .value()
        .to_vec();
    let context = AuthorizationContext {
        principal_id: manager_grantee,
        role_ids: Default::default(),
        scope: [
            (Operation::Usage, AccessTarget::Schema(id(16))),
            (Operation::Select, AccessTarget::Table(id(16))),
            (Operation::Update, AccessTarget::Table(id(16))),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: id(16),
                    column_id: column_ids[0],
                },
            ),
            (
                Operation::Update,
                AccessTarget::Column {
                    table_id: id(16),
                    column_id: column_ids[0],
                },
            ),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Usage,
            target: AccessTarget::Schema(id(16)),
        },
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Select,
            target: AccessTarget::Table(id(16)),
        },
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Update,
            target: AccessTarget::Table(id(16)),
        },
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
        },
        Grant {
            subject: Subject::Principal(manager_grantee),
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
        },
    ];
    let mut policies = Vec::new();
    for operation in [Operation::Select, Operation::Update] {
        let policy = Policy {
            subjects: vec![Subject::Principal(manager_grantee)],
            operation,
            target: AccessTarget::Table(id(16)),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: (operation == Operation::Update)
                .then_some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        };
        policies.push(policy.clone());
        policies.push(Policy {
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
            ..policy
        });
    }
    let dependency = AuthorityDependency {
        key: manager_update.key.clone(),
        expected_value: Some(grant_bytes),
    };
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: id(16),
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: std::slice::from_ref(&dependency),
    };
    let mut writer = storage.begin_invocation(id(0x83), manager_grantee, 9_999, 1, 1024)?;
    writer
        .stage_patch_authorized(
            prdb_engine::overlay::RowId {
                table: id(16),
                row: row_id,
            },
            &projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text("after grant race".to_owned())),
            }],
        )
        .map_err(|error| format!("stage grant-race update: {error:?}"))?;

    let mut race_revoke_audit = audit_event(0x84);
    race_revoke_audit.actor_id = Some(grant_manager);
    race_revoke_audit.action = AuditAction::AuthorityChange;
    let before_race = commit_sequence(&database)?;
    let barrier = Arc::new(Barrier::new(2));
    let (write_result, revoke_result) = std::thread::scope(|scope| {
        let write_barrier = barrier.clone();
        let write_thread = scope.spawn(move || {
            write_barrier.wait();
            writer.commit()
        });
        barrier.wait();
        let revoke_result = storage.commit_authorized_object_grant_ddl(
            id(0x84),
            grant_manager,
            9_999,
            &[
                remove_catalog_record(manager_update.clone())?,
                remove_catalog_record(dependency_write(manager_grantee, id(16), 5))?,
                bump_grantee_epoch(manager_grantee, 3, "manager_grantee")?,
            ],
            &[race_revoke_audit],
        );
        let write_result = write_thread.join().expect("grant writer thread panicked");
        Ok::<_, Box<dyn std::error::Error>>((write_result, revoke_result))
    })?;
    let write_status = match write_result {
        Ok(_) => OutcomeStatus::Committed,
        Err(prdb_engine::transaction::TransactionError::Commit(CommitError::Conflict)) => {
            OutcomeStatus::Aborted
        }
        Err(error) => panic!("unexpected grant-writer race result: {error:?}"),
    };
    let revoke_sequence =
        revoke_result.map_err(|error| format!("revoke grant for staged writer: {error:?}"))?;
    assert_eq!(commit_sequence(&database)?, revoke_sequence);
    assert_eq!(
        revoke_sequence,
        before_race + u64::from(write_status == OutcomeStatus::Committed) + 1
    );
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &RowRecord::decode(
                database
                    .begin_read()?
                    .open_table(ROWS)?
                    .get(&row_key[..])?
                    .ok_or("row disappeared after grant revoke")?
                    .value(),
            )?
            .payload,
        )?
        .values,
        [Some(ScalarValue::Text(
            if write_status == OutcomeStatus::Committed {
                "after grant race"
            } else {
                "before revoke"
            }
            .to_owned()
        ))]
    );
    assert_eq!(
        lookup_outcome(&database, id(0x83), manager_grantee, 1)?
            .ok_or("missing stale grant writer outcome")?
            .status,
        write_status
    );

    assert_eq!(commit_sequence(&database)?, revoke_sequence);
    for (invocation_id, principal_id, status) in [
        (id(101), table_owner, OutcomeStatus::Committed),
        (id(102), grant_manager, OutcomeStatus::Committed),
        (id(103), denied_user, OutcomeStatus::Aborted),
        (id(104), table_owner, OutcomeStatus::Committed),
        (id(0x80), manager_grantee, OutcomeStatus::Committed),
        (id(0x84), grant_manager, OutcomeStatus::Committed),
        (id(0x83), manager_grantee, write_status),
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
    assert!(catalog.get(manager_update.key.as_slice())?.is_none());
    assert!(catalog.get(denied_insert.key.as_slice())?.is_none());
    assert!(
        catalog
            .get(dependency_write(owner_grantee, id(16), 5).key.as_slice())?
            .is_none()
    );
    Ok(())
}

#[test]
fn role_membership_revocation_aborts_a_staged_role_authorized_writer()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{
        AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Effect,
        Expr, Grant, Operation, Policy, Subject,
    };
    use prdb_engine::storage_manager::RoleMembershipChange;
    use prdb_engine::tuple::FieldPatch;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let user_id = id(0x91);
    let role_id = id(0x92);
    let invocation_id = id(0x93);
    let membership_invocation_id = id(0x94);
    let admin_role = builtin_owner_id();
    let row_id = id(0x95);
    let row_key = table_row_key(id(16), row_id);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[0x96; 16]];

    let mut setup_audit = audit_event(0x97);
    setup_audit.actor_id = Some(admin_role);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x97),
        admin_role,
        9_999,
        &[
            principal_write_with_name(user_id, 1, 1, "membership_writer"),
            object_write_with_name(user_id, 8, "membership_writer"),
            credential_verifier_write(id(0x9a), user_id, &[0x9a; 32], 1, i64::MAX, None, 1),
            membership_write(user_id, admin_role),
            dependency_write(user_id, admin_role, 5),
            principal_write_with_name(role_id, 2, 1, "membership_editor"),
            object_write_with_name(role_id, 9, "membership_editor"),
            membership_write(user_id, role_id),
            dependency_write(user_id, role_id, 5),
            grant_write(role_id, id(16), 4, 1, id(16), admin_role),
            dependency_write(role_id, id(16), 5),
        ],
        &[setup_audit],
    )?;

    commit_rows(
        &database,
        id(0x98),
        user_id,
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text(
                    "before membership revoke".to_owned(),
                ))],
            )?),
        }],
    )?;

    let grant_key = grant_write(role_id, id(16), 4, 1, id(16), admin_role).key;
    let grant_bytes = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(grant_key.as_slice())?
        .ok_or("missing role UPDATE grant")?
        .value()
        .to_vec();
    let (mut writer, role_ids) = storage
        .begin_authenticated_invocation(invocation_id, user_id, 9_999, 1, 1024)
        .map_err(|error| format!("admit membership writer: {error:?}"))?;
    assert!(role_ids.contains(&role_id));
    assert!(role_ids.contains(&admin_role));
    let context = AuthorizationContext {
        principal_id: user_id,
        role_ids,
        scope: [
            (Operation::Usage, AccessTarget::Schema(id(16))),
            (Operation::Select, AccessTarget::Table(id(16))),
            (Operation::Update, AccessTarget::Table(id(16))),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: id(16),
                    column_id: column_ids[0],
                },
            ),
            (
                Operation::Update,
                AccessTarget::Column {
                    table_id: id(16),
                    column_id: column_ids[0],
                },
            ),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Usage,
            target: AccessTarget::Schema(id(16)),
        },
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Select,
            target: AccessTarget::Table(id(16)),
        },
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Update,
            target: AccessTarget::Table(id(16)),
        },
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
        },
        Grant {
            subject: Subject::Role(role_id),
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role(role_id)],
            operation: Operation::Select,
            target: AccessTarget::Table(id(16)),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role(role_id)],
            operation: Operation::Update,
            target: AccessTarget::Table(id(16)),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Role(role_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role(role_id)],
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: id(16),
                column_id: column_ids[0],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: id(16),
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[AuthorityDependency {
            key: grant_key,
            expected_value: Some(grant_bytes),
        }],
    };
    writer
        .stage_patch_authorized(
            prdb_engine::overlay::RowId {
                table: id(16),
                row: row_id,
            },
            &projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text(
                    "stale after membership revoke".to_owned(),
                )),
            }],
        )
        .map_err(|error| format!("stage membership-authorized writer: {error:?}"))?;

    let mut revoke_audit = audit_event(0x99);
    revoke_audit.actor_id = Some(user_id);
    revoke_audit.correlation_id = membership_invocation_id;
    revoke_audit.action = AuditAction::AuthorityChange;
    assert_eq!(
        storage
            .set_role_membership(
                membership_invocation_id,
                user_id,
                RoleMembershipChange {
                    member_id: user_id,
                    role_id,
                    grant: false
                },
                9_999,
                revoke_audit,
            )
            .map_err(|error| format!("revoke role membership: {error:?}"))?,
        3,
    );
    assert!(matches!(
        writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));

    let row = database
        .begin_read()?
        .open_table(ROWS)?
        .get(&row_key[..])?
        .ok_or("row disappeared after membership revoke")?
        .value()
        .to_vec();
    assert_eq!(
        decode_tuple(&columns, 1, &RowRecord::decode(&row)?.payload)?.values,
        [Some(ScalarValue::Text(
            "before membership revoke".to_owned()
        ))]
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, user_id, 1)?
            .ok_or("missing aborted writer outcome")?
            .status,
        OutcomeStatus::Aborted,
    );
    assert_eq!(
        lookup_outcome(&database, membership_invocation_id, user_id, 1)?
            .ok_or("missing membership revoke outcome")?
            .status,
        OutcomeStatus::Committed,
    );
    assert_eq!(commit_sequence(&database)?, 3);
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
    drop(catalog);
    drop(read);
    let mut create_policy_audit = policy_audit(0x87, table_owner);
    create_policy_audit.correlation_id = id(0x87);
    let created_policy =
        storage.create_policy(prdb_engine::storage_manager::CreatePolicyRequest {
            invocation_id: id(0x87),
            authenticated_actor_id: table_owner,
            table_id,
            name: "typed_policy".to_owned(),
            kind: prdb_engine::storage_manager::PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients: vec![prdb_engine::authorization::Subject::Role(manager_role)],
            target_column_ids: Vec::new(),
            using_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: create_policy_audit,
        })?;
    assert_ne!(created_policy.policy_id, [0; 16]);
    assert!(created_policy.using_expression_id.is_some());
    assert!(created_policy.with_check_expression_id.is_none());
    assert_eq!(created_policy.commit_sequence, 6);
    assert_eq!(commit_sequence(&database)?, 6);
    assert_eq!(
        lookup_outcome(&database, id(0x87), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    let mut drop_policy_audit = policy_audit(0x88, table_owner);
    drop_policy_audit.correlation_id = id(0x88);
    let drop_sequence = storage.drop_policy(prdb_engine::storage_manager::DropPolicyRequest {
        invocation_id: id(0x88),
        authenticated_actor_id: table_owner,
        policy_id: created_policy.policy_id,
        retention_deadline_us: 9_999,
        audit_event: drop_policy_audit,
    })?;
    assert_eq!(drop_sequence, 7);
    assert_eq!(commit_sequence(&database)?, 7);
    assert_eq!(
        lookup_outcome(&database, id(0x88), table_owner, 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
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
    let ddl_admin = id(0x7e);
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
        principal_write_with_name(ddl_admin, 1, 1, "column_drop_dba"),
        object_write_with_name(ddl_admin, 8, "column_drop_dba"),
        membership_write(ddl_admin, admin_role),
        dependency_write(ddl_admin, admin_role, 5),
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
    let stale_source_invocation = id(0x76);
    let stale_source_row = id(0x78);
    let mut stale_source_writer = storage.begin_invocation(
        stale_source_invocation,
        alter_and_reference_user,
        9_999,
        1,
        1024,
    )?;
    let stale_source_schema = stale_source_writer.load_catalog_table_schema(source_table_id)?;
    stale_source_writer.load_catalog_indexes(&stale_source_schema)?;
    let stale_source_tuple = encode_tuple(
        &stale_source_schema.columns,
        stale_source_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"before foreign key".to_vec()))],
    )?;
    stale_source_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: source_table_id,
            row: stale_source_row,
        },
        &stale_source_tuple,
    )?;
    let stale_target_invocation = id(0x77);
    let stale_target_row = id(0x79);
    let mut stale_target_writer = storage.begin_invocation(
        stale_target_invocation,
        alter_and_reference_user,
        9_999,
        1,
        1024,
    )?;
    let stale_target_schema = stale_target_writer.load_catalog_table_schema(target_table_id)?;
    stale_target_writer.load_catalog_indexes(&stale_target_schema)?;
    let stale_target_tuple = encode_tuple(
        &stale_target_schema.columns,
        stale_target_schema.schema_version,
        &[Some(ScalarValue::Bytea(b"before foreign key".to_vec()))],
    )?;
    stale_target_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: target_table_id,
            row: stale_target_row,
        },
        &stale_target_tuple,
    )?;
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
    assert!(matches!(
        stale_source_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(source_table_id, stale_source_row))?.is_none());
    assert_eq!(
        lookup_outcome(
            &database,
            stale_source_invocation,
            alter_and_reference_user,
            1
        )?
        .ok_or("missing stale source-table FK writer outcome")?
        .status,
        OutcomeStatus::Aborted
    );
    assert!(matches!(
        stale_target_writer.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(row(&database, &table_row_key(target_table_id, stale_target_row))?.is_none());
    assert_eq!(
        lookup_outcome(
            &database,
            stale_target_invocation,
            alter_and_reference_user,
            1
        )?
        .ok_or("missing target-table FK writer outcome")?
        .status,
        OutcomeStatus::Aborted
    );

    for (invocation_id, column_id) in [(id(0x7a), target_column_id), (id(0x7c), id(84))] {
        let mut drop_audit = identity_audit_event(invocation_id[0], ddl_admin, column_id);
        drop_audit.correlation_id = invocation_id;
        assert_eq!(
            storage.drop_column(
                invocation_id,
                ddl_admin,
                if column_id == target_column_id {
                    target_table_id
                } else {
                    source_table_id
                },
                column_id,
                9_999,
                drop_audit,
            ),
            Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
                CommitError::ColumnHasDependencies
            ))
        );
        assert_eq!(
            lookup_outcome(&database, invocation_id, ddl_admin, 1)?
                .ok_or("missing FK-restricted column-drop outcome")?
                .status,
            OutcomeStatus::Aborted
        );
    }
    let restricted_table_drop = id(0x7f);
    let mut restricted_table_drop_audit = identity_audit_event(0x7f, ddl_admin, target_table_id);
    restricted_table_drop_audit.correlation_id = restricted_table_drop;
    assert_eq!(
        storage.drop_table(
            restricted_table_drop,
            ddl_admin,
            target_table_id,
            9_999,
            restricted_table_drop_audit,
        ),
        Err(prdb_engine::storage_manager::TableLifecycleError::Commit(
            CommitError::TableHasDependencies
        ))
    );
    assert_eq!(
        lookup_outcome(&database, restricted_table_drop, ddl_admin, 1)?
            .ok_or("missing FK-restricted table-drop outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        assert!(catalog.get(foreign_key_key.as_slice())?.is_some());
        assert!(
            catalog
                .get(id_catalog_key(5, target_table_id).as_slice())?
                .is_some()
        );
    }

    let stale_source_drop_invocation = id(0x80);
    let stale_source_drop_row = id(0x82);
    let mut stale_source_drop_writer = storage.begin_invocation(
        stale_source_drop_invocation,
        alter_and_reference_user,
        9_999,
        1,
        1024,
    )?;
    let stale_source_drop_schema =
        stale_source_drop_writer.load_catalog_table_schema(source_table_id)?;
    stale_source_drop_writer.load_catalog_indexes(&stale_source_drop_schema)?;
    let stale_source_drop_tuple = encode_tuple(
        &stale_source_drop_schema.columns,
        stale_source_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"before foreign key drop".to_vec(),
        ))],
    )?;
    stale_source_drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: source_table_id,
            row: stale_source_drop_row,
        },
        &stale_source_drop_tuple,
    )?;
    let stale_target_drop_invocation = id(0x81);
    let stale_target_drop_row = id(0x83);
    let mut stale_target_drop_writer = storage.begin_invocation(
        stale_target_drop_invocation,
        alter_and_reference_user,
        9_999,
        1,
        1024,
    )?;
    let stale_target_drop_schema =
        stale_target_drop_writer.load_catalog_table_schema(target_table_id)?;
    stale_target_drop_writer.load_catalog_indexes(&stale_target_drop_schema)?;
    let stale_target_drop_tuple = encode_tuple(
        &stale_target_drop_schema.columns,
        stale_target_drop_schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"before foreign key drop".to_vec(),
        ))],
    )?;
    stale_target_drop_writer.stage_put(
        prdb_engine::overlay::RowId {
            table: target_table_id,
            row: stale_target_drop_row,
        },
        &stale_target_drop_tuple,
    )?;

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
    for (writer, invocation_id, table_id, row_id, label) in [
        (
            stale_source_drop_writer,
            stale_source_drop_invocation,
            source_table_id,
            stale_source_drop_row,
            "source",
        ),
        (
            stale_target_drop_writer,
            stale_target_drop_invocation,
            target_table_id,
            stale_target_drop_row,
            "target",
        ),
    ] {
        assert!(matches!(
            writer.commit(),
            Err(prdb_engine::transaction::TransactionError::Commit(
                CommitError::Conflict
            ))
        ));
        assert!(row(&database, &table_row_key(table_id, row_id))?.is_none());
        assert_eq!(
            lookup_outcome(&database, invocation_id, alter_and_reference_user, 1)?
                .ok_or_else(|| format!("missing stale {label}-table FK-drop outcome"))?
                .status,
            OutcomeStatus::Aborted
        );
    }

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
        (
            stale_source_drop_invocation,
            alter_and_reference_user,
            OutcomeStatus::Aborted,
        ),
        (
            stale_target_drop_invocation,
            alter_and_reference_user,
            OutcomeStatus::Aborted,
        ),
        (restricted_table_drop, ddl_admin, OutcomeStatus::Aborted),
        (
            stale_source_invocation,
            alter_and_reference_user,
            OutcomeStatus::Aborted,
        ),
        (
            stale_target_invocation,
            alter_and_reference_user,
            OutcomeStatus::Aborted,
        ),
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
fn storage_manager_add_column_row_limit_aborts_atomically() -> Result<(), Box<dyn std::error::Error>>
{
    use prdb_engine::storage_manager::{
        AddTableColumnRequest, CreateTableRequest, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x8b);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x8c, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x8c),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "column_limit_owner"),
            object_write_with_name(actor, 8, "column_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x8d);
    let mut schema_audit = identity_audit_event(0x8e, actor, id(0x8f));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_limit_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x90);
    let mut create_audit = identity_audit_event(0x91, actor, id(0x92));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "bounded_table".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let old_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        }],
        1,
        &[Some(ScalarValue::Integer(1))],
    )?;
    let rows = (1u64..=65_537)
        .map(|row_number| {
            let mut row_suffix = [0; 16];
            row_suffix[8..].copy_from_slice(&row_number.to_be_bytes());
            RowWrite {
                key: table_row_key(table.table_id, row_suffix),
                expected_version: None,
                payload: Some(old_tuple.clone()),
            }
        })
        .collect::<Vec<_>>();
    let rows_invocation = id(0x93);
    let before_add = commit_rows(&database, rows_invocation, actor, 9_999, &rows)?;
    let add_invocation = id(0x94);
    let mut add_audit = identity_audit_event(0x95, actor, id(0x96));
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
            .ok_or("missing add-column limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    let table_record = decode_registered_envelope(
        read.open_table(CATALOG)?
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing after add-column limit")?
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
    let mut scanned_columns = 0;
    for entry in read
        .open_table(CATALOG)?
        .range(6u16.to_be_bytes().as_slice()..)?
    {
        let (key, value) = entry?;
        let key = key.value();
        if !key.starts_with(&6u16.to_be_bytes()) {
            break;
        }
        let record = decode_registered_envelope(value.value())?;
        if record
            .fields
            .iter()
            .any(|field| field.tag == 1 && field.payload.as_slice() == table.table_id)
        {
            scanned_columns += 1;
        }
    }
    assert_eq!(scanned_columns, 1);
    drop(read);

    let mut last_row_suffix = [0; 16];
    last_row_suffix[8..].copy_from_slice(&65_537u64.to_be_bytes());
    let last_row_key = table_row_key(table.table_id, last_row_suffix);
    let remove_last_row_invocation = id(0x97);
    assert_eq!(
        commit_rows(
            &database,
            remove_last_row_invocation,
            actor,
            9_999,
            &[RowWrite {
                key: last_row_key,
                expected_version: Some(before_add),
                payload: None,
            }],
        )?,
        before_add + 1
    );
    let boundary_invocation = id(0x98);
    let mut boundary_audit = identity_audit_event(0x99, actor, id(0x9a));
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
            default: None,
        },
        retention_deadline_us: 9_999,
        audit_event: boundary_audit,
    })?;
    assert_eq!(boundary_add.schema_version, 2);
    assert_eq!(boundary_add.commit_sequence, before_add + 2);
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact-bound add-column outcome")?
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_drop_column_row_limit_aborts_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        CreateTableRequest, TableColumnDefinition, TableLifecycleError,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0x8b);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x8c, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(
        id(0x8c),
        admin_role,
        9_999,
        &[
            principal_write_with_name(actor, 1, 1, "column_drop_limit_owner"),
            object_write_with_name(actor, 8, "column_drop_limit_owner"),
            membership_write(actor, admin_role),
            dependency_write(actor, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let schema_invocation = id(0x8d);
    let mut schema_audit = identity_audit_event(0x8e, actor, id(0x8f));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        actor,
        "column_drop_limit_schema",
        actor,
        9_999,
        schema_audit,
    )?;
    let create_invocation = id(0x90);
    let mut create_audit = identity_audit_event(0x91, actor, id(0x92));
    create_audit.correlation_id = create_invocation;
    let table = storage.create_table(CreateTableRequest {
        invocation_id: create_invocation,
        authenticated_actor_id: actor,
        schema_id: schema.schema_id,
        name: "bounded_drop_table".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "removed".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "retained".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: 9_999,
        audit_event: create_audit,
    })?;
    let old_tuple = encode_tuple(
        &[
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: false,
            },
        ],
        1,
        &[Some(ScalarValue::Integer(1)), Some(ScalarValue::Integer(2))],
    )?;
    let rows = (1u64..=65_537)
        .map(|row_number| {
            let mut row_suffix = [0; 16];
            row_suffix[8..].copy_from_slice(&row_number.to_be_bytes());
            RowWrite {
                key: table_row_key(table.table_id, row_suffix),
                expected_version: None,
                payload: Some(old_tuple.clone()),
            }
        })
        .collect::<Vec<_>>();
    let rows_invocation = id(0x93);
    let before_drop = commit_rows(&database, rows_invocation, actor, 9_999, &rows)?;
    let drop_invocation = id(0x94);
    let mut drop_audit = identity_audit_event(0x95, actor, id(0x96));
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
            .ok_or("missing DROP COLUMN limit outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    let mut last_row_suffix = [0; 16];
    last_row_suffix[8..].copy_from_slice(&65_537u64.to_be_bytes());
    let last_row_key = table_row_key(table.table_id, last_row_suffix);
    assert_eq!(
        commit_rows(
            &database,
            id(0x97),
            actor,
            9_999,
            &[RowWrite {
                key: last_row_key,
                expected_version: Some(before_drop),
                payload: None,
            }],
        )?,
        before_drop + 1
    );
    let boundary_invocation = id(0x98);
    let mut boundary_audit = identity_audit_event(0x99, actor, id(0x9a));
    boundary_audit.correlation_id = boundary_invocation;
    let dropped = storage.drop_column(
        boundary_invocation,
        actor,
        table.table_id,
        table.column_ids[0],
        9_999,
        boundary_audit,
    )?;
    assert_eq!(dropped.column_id, table.column_ids[0]);
    assert_eq!(dropped.commit_sequence, before_drop + 2);
    let mut first_row_suffix = [0; 16];
    first_row_suffix[8..].copy_from_slice(&1u64.to_be_bytes());
    let first_row_key = table_row_key(table.table_id, first_row_suffix);
    let remaining_row =
        row(&database, &first_row_key)?.ok_or("row missing after exact-bound DROP COLUMN")?;
    let decoded = decode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        }],
        2,
        &remaining_row.payload,
    )?;
    assert_eq!(decoded.values, vec![Some(ScalarValue::Integer(2))]);
    let table_record = decode_registered_envelope(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, table.table_id).as_slice())?
            .ok_or("table record missing after exact-bound DROP COLUMN")?
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
        2
    );
    assert_eq!(
        lookup_outcome(&database, boundary_invocation, actor, 1)?
            .ok_or("missing exact-bound DROP COLUMN outcome")?
            .status,
        OutcomeStatus::Committed
    );
    StorageManager::new(&database, Duration::from_millis(100))?;
    Ok(())
}

#[test]
fn storage_manager_drop_column_byte_limit_aborts_atomically()
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

#[test]
fn storage_manager_add_column_byte_limit_aborts_atomically()
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
    use prdb_engine::authorization::{
        AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Effect,
        Expr, Grant, Operation, Policy, Subject,
    };
    use prdb_engine::transaction::TransactionError;
    use prdb_engine::tuple::FieldPatch;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(70);
    let admin_user_id = id(0x82);
    let admin_role = builtin_owner_id();
    let active_secret = [0x31; 32];
    let expired_secret = [0x32; 32];
    let revoked_secret = [0x33; 32];
    let login_disable_secret = [0x35; 32];
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
    let live_admin_credential = credential_verifier_write(
        id(0x83),
        admin_user_id,
        &[0x34; 32],
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    let login_disable_credential = credential_verifier_write(
        id(0x85),
        user_id,
        &login_disable_secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    commit_catalog_and_audit(
        &database,
        id(74),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(admin_user_id, 1, 1, "credential_admin"),
            object_write_with_name(admin_user_id, 8, "credential_admin"),
            membership_write(admin_user_id, admin_role),
            dependency_write(admin_user_id, admin_role, 5),
            principal_write_with_name(user_id, 1, 1, "credential_user"),
            object_write_with_name(user_id, 8, "credential_user"),
            grant_write(user_id, id(16), 2, 1, id(16), admin_role),
            grant_write(user_id, id(16), 4, 1, id(16), admin_role),
            dependency_write(user_id, id(16), 5),
            active,
            expired,
            revoked,
            live_admin_credential,
            login_disable_credential,
        ],
        &[audit_event(74)],
    )
    .map_err(|error| format!("seed credential records: {error:?}"))?;

    let storage = StorageManager::new(&database, Duration::from_millis(10))
        .map_err(|error| format!("validate seeded credential records: {error:?}"))?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: false,
    }];
    let row_id = id(0x7a);
    let row_key = table_row_key(id(16), row_id);
    let original_value = ScalarValue::Bytea(b"before token revoke".to_vec());
    let original_payload = encode_tuple(&columns, 1, &[Some(original_value.clone())])?;
    let original_index = typed_bytea_index_key([0x44; 16], &row_key, &original_payload);
    let original_index_key = original_index.key.clone();
    commit_rows_and_indexes(
        &database,
        id(0x84),
        user_id,
        now + 3_600_000_000,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(original_payload),
        }],
        &[IndexWrite {
            key: original_index.key,
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: original_index.has_null,
        }],
    )?;
    let discovered = storage.discover_token_authorized_schemas(id(71), &active_secret)?;
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].name, "information_schema");
    assert!(matches!(
        storage.discover_token_authorized_schemas(id(71), &[0xFF; 32]),
        Err(TransactionError::AuthenticationFailed)
    ));
    let (mut admitted, roles) = storage.begin_token_authenticated_invocation(
        id(75),
        id(71),
        &active_secret,
        now + 3_600_000_000,
        2,
        1024,
    )?;
    assert!(roles.is_empty());
    let schema = admitted.load_catalog_table_schema(id(16))?;
    admitted.load_catalog_indexes(&schema)?;
    let select_grant_key = grant_write(user_id, id(16), 2, 1, id(16), admin_role).key;
    let update_grant_key = grant_write(user_id, id(16), 4, 1, id(16), admin_role).key;
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let select_grant_value = catalog
        .get(select_grant_key.as_slice())?
        .ok_or("missing persisted SELECT grant")?
        .value()
        .to_vec();
    let update_grant_value = catalog
        .get(update_grant_key.as_slice())?
        .ok_or("missing persisted UPDATE grant")?
        .value()
        .to_vec();
    drop(catalog);
    drop(read);
    let column_id = schema.column_ids[0];
    let context = AuthorizationContext {
        principal_id: user_id,
        role_ids: roles,
        scope: [
            (Operation::Usage, AccessTarget::Schema(schema.schema_id)),
            (Operation::Select, AccessTarget::Table(schema.table_id)),
            (Operation::Update, AccessTarget::Table(schema.table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: schema.table_id,
                    column_id,
                },
            ),
            (
                Operation::Update,
                AccessTarget::Column {
                    table_id: schema.table_id,
                    column_id,
                },
            ),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema.schema_id),
        },
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Select,
            target: AccessTarget::Table(schema.table_id),
        },
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Update,
            target: AccessTarget::Table(schema.table_id),
        },
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: schema.table_id,
                column_id,
            },
        },
        Grant {
            subject: Subject::Principal(user_id),
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: schema.table_id,
                column_id,
            },
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal(user_id)],
            operation: Operation::Select,
            target: AccessTarget::Table(schema.table_id),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(user_id)],
            operation: Operation::Update,
            target: AccessTarget::Table(schema.table_id),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Principal(user_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: schema.table_id,
                column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(user_id)],
            operation: Operation::Update,
            target: AccessTarget::Column {
                table_id: schema.table_id,
                column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let dependencies = [
        AuthorityDependency {
            key: select_grant_key,
            expected_value: Some(select_grant_value),
        },
        AuthorityDependency {
            key: update_grant_key,
            expected_value: Some(update_grant_value),
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
        dependencies: &dependencies,
    };
    admitted.stage_patch_authorized(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Bytea(
                b"must not publish after token revoke".to_vec(),
            )),
        }],
    )?;
    let updated_payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"must not publish after token revoke".to_vec(),
        ))],
    )?;
    let updated_index = typed_bytea_index_key([0x44; 16], &row_key, &updated_payload);
    let updated_index_key = updated_index.key.clone();
    let original_index_key_for_assert = original_index_key.clone();
    let row_handle = prdb_engine::overlay::RowId {
        table: schema.table_id,
        row: row_id,
    };
    admitted.stage_index_membership(row_handle, original_index_key, false)?;
    admitted.stage_index_membership(row_handle, updated_index.key, true)?;

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
            lookup_outcome(&database, invocation_id, user_id, now)?,
            None
        );
    }

    let mut revoke_event = audit_event(79);
    revoke_event.actor_id = Some(admin_user_id);
    revoke_event.correlation_id = id(79);
    revoke_event.action = AuditAction::TokenLifecycle;
    revoke_event.target_ids = vec![id(71)];
    storage.revoke_token(
        id(79),
        admin_user_id,
        id(71),
        now + 3_600_000_000,
        revoke_event,
    )?;
    let stale_commit = admitted.commit();
    assert!(
        matches!(
            &stale_commit,
            Err(TransactionError::Commit(CommitError::Conflict))
        ),
        "revoked token allowed a stale commit or produced the wrong error: {stale_commit:?}"
    );
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &RowRecord::decode(
                database
                    .begin_read()?
                    .open_table(ROWS)?
                    .get(&row_key[..])?
                    .ok_or("row disappeared after token revocation")?
                    .value(),
            )?
            .payload,
        )?
        .values,
        [Some(original_value)],
        "revoked token's authorized update was published"
    );
    let read = database.begin_read()?;
    let indexes = read.open_table(schema::INDEXES)?;
    assert!(
        indexes
            .get(original_index_key_for_assert.as_slice())?
            .is_some(),
        "revoked token removed the existing index membership"
    );
    assert!(
        indexes.get(updated_index_key.as_slice())?.is_none(),
        "revoked token published the replacement index membership"
    );
    drop(indexes);
    drop(read);
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
    assert_eq!(lookup_outcome(&database, id(80), user_id, now)?, None);

    let (mut rename_writer, rename_roles) = storage.begin_token_authenticated_invocation(
        id(0x8a),
        id(0x85),
        &login_disable_secret,
        now + 3_600_000_000,
        2,
        1024,
    )?;
    assert!(rename_roles.is_empty());
    rename_writer.stage_patch_authorized(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Bytea(
                b"must not publish after identity rename".to_vec(),
            )),
        }],
    )?;
    let renamed_payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"must not publish after identity rename".to_vec(),
        ))],
    )?;
    let renamed_index = typed_bytea_index_key([0x44; 16], &row_key, &renamed_payload);
    let renamed_index_key = renamed_index.key.clone();
    rename_writer.stage_index_membership(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        original_index_key_for_assert.clone(),
        false,
    )?;
    rename_writer.stage_index_membership(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        renamed_index.key,
        true,
    )?;
    let rename_invocation = id(0x8b);
    let mut rename_audit = identity_audit_event(0x8c, admin_user_id, user_id);
    rename_audit.correlation_id = rename_invocation;
    assert_eq!(
        storage.rename_identity(
            rename_invocation,
            admin_user_id,
            user_id,
            "renamed_credential_user",
            now + 3_600_000_000,
            rename_audit,
        )?,
        4,
    );
    let rename_result = rename_writer.commit();
    assert!(
        matches!(
            &rename_result,
            Err(TransactionError::Commit(CommitError::Conflict))
        ),
        "renamed identity committed a stale update: {rename_result:?}"
    );
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &RowRecord::decode(
                database
                    .begin_read()?
                    .open_table(ROWS)?
                    .get(&row_key[..])?
                    .ok_or("row disappeared after identity rename")?
                    .value(),
            )?
            .payload,
        )?
        .values,
        [Some(ScalarValue::Bytea(b"before token revoke".to_vec()))],
        "identity rename's stale update was published"
    );
    let read = database.begin_read()?;
    let indexes = read.open_table(schema::INDEXES)?;
    assert!(
        indexes
            .get(original_index_key_for_assert.as_slice())?
            .is_some()
    );
    assert!(indexes.get(renamed_index_key.as_slice())?.is_none());
    drop(indexes);
    drop(read);
    assert_eq!(
        lookup_outcome(&database, id(0x8a), user_id, now)?
            .ok_or("missing identity-rename stale writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let (mut login_disable_writer, login_roles) = storage.begin_token_authenticated_invocation(
        id(0x88),
        id(0x85),
        &login_disable_secret,
        now + 3_600_000_000,
        2,
        1024,
    )?;
    assert!(login_roles.is_empty());
    login_disable_writer.stage_patch_authorized(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Bytea(
                b"must not publish after LOGIN disable".to_vec(),
            )),
        }],
    )?;
    let login_disabled_payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(ScalarValue::Bytea(
            b"must not publish after LOGIN disable".to_vec(),
        ))],
    )?;
    let login_disabled_index = typed_bytea_index_key([0x44; 16], &row_key, &login_disabled_payload);
    login_disable_writer.stage_index_membership(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        original_index_key_for_assert.clone(),
        false,
    )?;
    login_disable_writer.stage_index_membership(
        prdb_engine::overlay::RowId {
            table: schema.table_id,
            row: row_id,
        },
        login_disabled_index.key.clone(),
        true,
    )?;

    let login_disable_invocation = id(0x87);
    let mut login_disable_audit = identity_audit_event(0x86, admin_user_id, user_id);
    login_disable_audit.correlation_id = login_disable_invocation;
    assert_eq!(
        storage.set_user_login_enabled(
            login_disable_invocation,
            admin_user_id,
            user_id,
            false,
            now + 3_600_000_000,
            login_disable_audit,
        )?,
        5,
    );
    let login_disable_result = login_disable_writer.commit();
    assert!(
        matches!(
            &login_disable_result,
            Err(TransactionError::Commit(CommitError::Conflict))
        ),
        "disabled LOGIN user committed a stale update: {login_disable_result:?}"
    );
    assert_eq!(
        decode_tuple(
            &columns,
            1,
            &RowRecord::decode(
                database
                    .begin_read()?
                    .open_table(ROWS)?
                    .get(&row_key[..])?
                    .ok_or("row disappeared after LOGIN disable")?
                    .value(),
            )?
            .payload,
        )?
        .values,
        [Some(ScalarValue::Bytea(b"before token revoke".to_vec()))],
        "disabled user's staged update was published"
    );
    let read = database.begin_read()?;
    let indexes = read.open_table(schema::INDEXES)?;
    assert!(
        indexes
            .get(original_index_key_for_assert.as_slice())?
            .is_some()
    );
    assert!(indexes.get(login_disabled_index.key.as_slice())?.is_none());
    drop(indexes);
    drop(read);
    assert_eq!(
        lookup_outcome(&database, id(0x88), user_id, now)?
            .ok_or("missing LOGIN-disable stale writer outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0x89),
            id(0x85),
            &login_disable_secret,
            now + 3_600_000_000,
            2,
            64,
        ),
        Err(TransactionError::AuthenticationFailed)
    ));
    assert_eq!(lookup_outcome(&database, id(0x89), user_id, now)?, None);
    assert!(
        storage
            .list_tokens(admin_user_id, user_id)?
            .iter()
            .any(|token| token.token_id == id(0x85) && token.revoked)
    );
    assert!(prdb_engine::startup::validate_persisted_records(&database).is_ok());
    Ok(())
}

#[test]
fn client_session_authenticates_and_revalidates_before_operations()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientMode, ClientSessionError};
    use prdb_engine::transaction::TransactionError;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    let credential = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    let credential_value = credential.payload.clone().unwrap();
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "session_user"),
            object_write_with_name(user_id, 8, "session_user"),
            credential,
        ],
        &[audit_event(0x72)],
    )?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;

    let mut session = ClientConnectionSession::default();
    assert!(session.permits_request_kind(1));
    assert!(!session.permits_request_kind(2));
    session.accept_hello(ClientMode::Query, "integration-test")?;
    assert!(session.permits_request_kind(2));
    assert!(!session.permits_request_kind(3));
    let authenticated = session.authenticate(&storage, token_id, &secret)?;
    let session_id = authenticated.connection_id();
    assert_ne!(session_id, [0; 16]);
    assert_eq!(authenticated.principal_id(), user_id);
    assert!(authenticated.role_ids().is_empty());
    assert!(session.staging_owner().is_ok());
    assert!(session.permits_request_kind(3));
    session.authorize_request(&storage, 3)?;
    assert_eq!(session.session_id()?, session_id);

    let mut revoked = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        Some(now),
        2,
    );
    revoked.expected_value = Some(credential_value);
    storage.commit_catalog_and_audit(
        id(0x73),
        id(9),
        now + 3_600_000_000,
        &[revoked],
        &[audit_event(0x73)],
    )?;
    assert!(matches!(
        session.authorize_request(&storage, 8),
        Err(ClientSessionError::Storage(
            TransactionError::AuthenticationFailed
        ))
    ));
    assert_eq!(session.session_id()?, session_id);
    Ok(())
}

#[test]
fn client_handshake_requires_hello_first_and_sequences_responses()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientSessionError};
    use prdb_engine::supervisor::{
        StagingLimits, StagingStore, SubmissionMetadata, SubmissionMetadataValidator,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayTerminalDecision,
        finalize_gateway_invocation_and_send_terminal, send_gateway_result_data_event,
    };
    use sha2::{Digest, Sha256};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
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
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "handshake_user"),
            object_write_with_name(user_id, 8, "handshake_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut session = ClientConnectionSession::default();
    assert!(matches!(
        session.handle_handshake_request(
            &storage,
            ClientFrame {
                kind: 11,
                request_id: 1,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            },
        ),
        Err(ClientSessionError::UnsupportedRequest)
    ));
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptUpload))?;
    let admission = GatewayAdmissionConfig {
        retention_period_us: 3_600_000_000,
        max_rows: 2,
        max_bytes: 64,
    };

    let token_id_hex = token_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let token_secret_base64 = "A".repeat(43) + "=";
    let auth_payload =
        format!("{{\"token_id\":\"{token_id_hex}\",\"token_secret\":\"{token_secret_base64}\"}}");
    let artifact = b"upload";
    let artifact_hash = Sha256::digest(artifact)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let empty_hash = Sha256::digest([])
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (mut service, mut ipc_gateway) = UnixStream::pair()?;
    service.set_read_timeout(Some(Duration::from_secs(10)))?;
    ipc_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    let tcp_listener = TcpListener::bind(("127.0.0.1", 0))?;
    let mut gateway = TcpStream::connect(tcp_listener.local_addr()?)?;
    let (mut gateway_tcp, _) = tcp_listener.accept()?;
    gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    gateway_tcp.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut session = std::thread::scope(|scope| {
        let storage_ref = &storage;
        let staging_ref = &staging;
        let service_thread = scope.spawn(move || {
            let mut service_session = session;
            let mut started = false;
            loop {
                match prdb_engine::supervisor_ipc::serve_gateway_client_session(
                    &mut service,
                    storage_ref,
                    staging_ref,
                    &mut service_session,
                    admission,
                )
                .map_err(|error| format!("gateway service failed: {error:?}"))?
                {
                    prdb_engine::supervisor_ipc::GatewayClientServiceEvent::Started {
                        request_id,
                        invocation,
                        submission,
                        executor: None,
                    } => {
                        assert_eq!(submission.artifact(), artifact);
                        let owner = service_session
                            .staging_owner()
                            .map_err(|error| format!("session owner unavailable: {error:?}"))?;
                        assert_eq!(
                            staging_ref
                                .status(owner, invocation.invocation_id())
                                .map_err(|error| format!("staging status failed: {error:?}"))?,
                            prdb_engine::supervisor::SubmissionState::Started
                        );
                        let mut invocation = invocation;
                        let mut worker = std::process::Command::new("true")
                            .spawn()
                            .map_err(|error| format!("worker spawn failed: {error:?}"))?;
                        invocation
                            .observe_worker_exit(&mut worker)
                            .map_err(|error| format!("worker exit failed: {error:?}"))?;
                        send_gateway_result_data_event(&mut service, request_id, b"rows".to_vec())
                            .map_err(|error| format!("result event failed: {error:?}"))?;
                        finalize_gateway_invocation_and_send_terminal(
                            &mut invocation,
                            &mut service,
                            request_id,
                            GatewayTerminalDecision::Commit,
                        )
                        .map_err(|error| format!("terminal relay failed: {error:?}"))?;
                        staging_ref
                            .finish_started(owner, invocation.invocation_id())
                            .map_err(|error| format!("staging cleanup failed: {error:?}"))?;
                        started = true;
                    }
                    prdb_engine::supervisor_ipc::GatewayClientServiceEvent::Started {
                        executor: Some(_),
                        ..
                    } => unreachable!("legacy session unexpectedly launched an executor"),
                    prdb_engine::supervisor_ipc::GatewayClientServiceEvent::Disconnected => {
                        if !started {
                            return Err("disconnect before START".to_owned());
                        }
                        return Ok::<_, String>(service_session);
                    }
                }
            }
        });

        let bridge_thread = scope.spawn(move || {
            prdb_engine::gateway::serve_tcp_client_connection(&mut gateway_tcp, &mut ipc_gateway)
                .map_err(|error| format!("TCP gateway bridge failed: {error:?}"))
        });

        let mut response_decoder = prdb_engine::client_protocol::ClientFrameDecoder::default();
        let mut response_buffer = [0u8; 8192];
        let mut pending_responses = std::collections::VecDeque::new();

        macro_rules! next_response {
            () => {{
                loop {
                    if let Some(response) = pending_responses.pop_front() {
                        break response;
                    }
                    let count = std::io::Read::read(&mut gateway, &mut response_buffer)?;
                    if count == 0 {
                        return Err("missing gateway response".into());
                    }
                    pending_responses.extend(response_decoder.push(&response_buffer[..count])?);
                }
            }};
        }

        macro_rules! round_trip {
            ($kind:expr, $request_id:expr, $payload:expr) => {{
                let encoded = ClientFrame {
                    kind: $kind,
                    request_id: $request_id,
                    response_sequence: 0,
                    payload: $payload,
                }
                .encode()?;
                std::io::Write::write_all(&mut gateway, &encoded)?;
                next_response!()
            }};
        }

        let hello = round_trip!(1, 2, br#"{"mode":"query","client_name":"test"}"#.to_vec());
        assert_eq!(hello.kind, 100);
        assert!(std::str::from_utf8(&hello.payload)?.contains("\"mode\":\"query\""));
        let auth = round_trip!(2, 3, auth_payload.into_bytes());
        assert_eq!(auth.kind, 100);
        assert!(std::str::from_utf8(&auth.payload)?.contains("\"authenticated\":true"));
        let auth_response = std::str::from_utf8(&auth.payload)?;
        let session_id = auth_response
            .split("\"session_id\":\"")
            .nth(1)
            .and_then(|value| value.get(..32))
            .ok_or("AUTH response omitted its session ID")?;
        assert_eq!(session_id.len(), 32);

        let prepare = round_trip!(
            3,
            4,
            format!("{{\"manifest_version\":1,\"manifest\":\"bQ==\",\"artifact_bytes\":\"6\",\"artifact_sha256\":\"{artifact_hash}\",\"parameter_bytes\":\"0\",\"parameter_sha256\":\"{empty_hash}\",\"scope_version\":1,\"requested_scope\":\"UFNDMQABAgIBAQEBAQEBAQEBAQEBAQEB\",\"deadline_ms\":\"30000\",\"workload_class\":\"operational\"}}").into_bytes()
        );
        let prepare_response = std::str::from_utf8(&prepare.payload)?;
        let invocation_hex = prepare_response
            .split("\"invocation_id\":\"")
            .nth(1)
            .and_then(|value| value.get(..32))
            .ok_or("PREPARE response omitted invocation ID")?;
        let invocation_id = prdb_engine::client_protocol::decode_client_seal(
            format!("{{\"invocation_id\":\"{invocation_hex}\"}}").as_bytes(),
        )?;
        let mut chunk_payload = invocation_id.to_vec();
        chunk_payload.extend_from_slice(&0u64.to_le_bytes());
        chunk_payload.extend_from_slice(artifact);
        let chunk = round_trip!(4, 5, chunk_payload);
        assert_eq!(chunk.payload, b"{\"next_offset\":\"6\"}");
        let seal = round_trip!(
            6,
            6,
            format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes()
        );
        assert_eq!(seal.payload, b"{\"state\":\"prepared\"}");
        let status = round_trip!(
            8,
            7,
            format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes()
        );
        assert_eq!(status.payload, b"{\"state\":\"prepared\"}");
        let accepted = round_trip!(
            7,
            8,
            format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes()
        );
        assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");
        let result_data = next_response!();
        assert_eq!(result_data.kind, 102);
        assert_eq!(result_data.request_id, 8);
        assert_eq!(result_data.response_sequence, 1);
        assert_eq!(result_data.payload, b"rows");
        let terminal = next_response!();
        assert_eq!(terminal.kind, 103);
        assert_eq!(terminal.request_id, 8);
        assert_eq!(terminal.response_sequence, 2);
        assert!(std::str::from_utf8(&terminal.payload)?.contains("\"outcome\":\"committed\""));
        gateway.shutdown(std::net::Shutdown::Write)?;
        bridge_thread
            .join()
            .map_err(|_| std::io::Error::other("gateway bridge thread panicked"))?
            .map_err(std::io::Error::other)?;
        let session = service_thread
            .join()
            .map_err(|_| std::io::Error::other("gateway service thread panicked"))?
            .map_err(std::io::Error::other)?;
        Ok::<_, Box<dyn std::error::Error>>(session)
    })?;

    let ping = session.handle_handshake_request(
        &storage,
        ClientFrame {
            kind: 11,
            request_id: 9,
            response_sequence: 0,
            payload: b"{}".to_vec(),
        },
    )?;
    assert_eq!(ping.payload, b"{\"alive\":true}");
    assert!(matches!(
        session.handle_handshake_request(
            &storage,
            ClientFrame {
                kind: 1,
                request_id: 10,
                response_sequence: 0,
                payload: br#"{"mode":"query","client_name":"again"}"#.to_vec(),
            },
        ),
        Err(ClientSessionError::UnsupportedRequest)
    ));
    Ok(())
}

#[test]
fn gateway_ipc_status_and_cancel_keep_unknown_ids_non_enumerating()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{IpcFrame, Operation, ServiceRole};
    use prdb_engine::supervisor_ipc::{
        SUPERVISOR_PAYLOAD_VERSION, dispatch_client_control_request,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut payload = SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec();
    payload.extend_from_slice(&id(0x61));
    let status = dispatch_client_control_request(
        ServiceRole::Gateway,
        &storage,
        id(0x62),
        &IpcFrame {
            operation: Operation::ClientGetInvocationStatus as u16,
            request_id: 1,
            payload: payload.clone(),
        },
    )?;
    assert_eq!(
        status.payload,
        [SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().as_slice(), &[0]].concat()
    );
    let cancellation = dispatch_client_control_request(
        ServiceRole::Gateway,
        &storage,
        id(0x62),
        &IpcFrame {
            operation: Operation::ClientCancelInvocation as u16,
            request_id: 2,
            payload: payload.clone(),
        },
    )?;
    assert_eq!(
        cancellation.payload,
        [SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().as_slice(), &[3]].concat()
    );
    assert!(
        dispatch_client_control_request(
            ServiceRole::Executor,
            &storage,
            id(0x62),
            &IpcFrame {
                operation: Operation::ClientGetInvocationStatus as u16,
                request_id: 3,
                payload,
            },
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn gateway_control_socket_serves_bounded_status_requests() -> Result<(), Box<dyn std::error::Error>>
{
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use prdb_engine::supervisor_ipc::{SUPERVISOR_PAYLOAD_VERSION, serve_client_control};
    use std::os::unix::net::UnixStream;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let (mut service, mut gateway) = UnixStream::pair()?;
    let mut payload = SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec();
    payload.extend_from_slice(&id(0x63));
    write_frame(
        &mut gateway,
        &IpcFrame {
            operation: Operation::ClientGetInvocationStatus as u16,
            request_id: 1,
            payload,
        },
    )?;
    gateway.shutdown(std::net::Shutdown::Write)?;
    serve_client_control(&mut service, &storage, id(0x64)).unwrap();
    let response = read_frame(&mut gateway)?.ok_or("missing gateway status response")?;
    assert_eq!(
        response.operation,
        Operation::ClientGetInvocationStatus as u16
    );
    assert_eq!(response.request_id, 1);
    assert_eq!(
        response.payload,
        [SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().as_slice(), &[0]].concat()
    );
    Ok(())
}

#[test]
fn gateway_service_listener_binds_peer_and_serves_hello() -> Result<(), Box<dyn std::error::Error>>
{
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use prdb_engine::supervisor::{
        StagingLimits, StagingStore, SubmissionMetadata, SubmissionMetadataValidator,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayServiceListenerConfig, bind_gateway_service_socket,
        serve_gateway_service_listener,
    };
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptUpload))?;
    let socket_path =
        std::path::PathBuf::from(format!("{}.gateway.sock", fixture.path().display()));
    let listener = bind_gateway_service_socket(&socket_path)?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let admission = GatewayAdmissionConfig {
        retention_period_us: i64::MAX,
        max_rows: 2,
        max_bytes: 64,
    };
    let executor_path = std::env::current_exe()?;
    let cgroup_parent = fixture.path().parent().unwrap().to_path_buf();
    std::thread::scope(|scope| {
        let shutdown_server = Arc::clone(&shutdown);
        let server = scope.spawn(move || {
            serve_gateway_service_listener(
                listener,
                &storage,
                &staging,
                prdb_engine::supervisor_ipc::GatewayServiceRuntimeConfig {
                    admission,
                    executor: prdb_engine::supervisor_ipc::GatewayExecutorConfig {
                        cgroup_parent: cgroup_parent.clone(),
                        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
                            cpu_quota_micros: 100_000,
                            cpu_period_micros: 100_000,
                            memory_max_bytes: 64 * 1024 * 1024,
                            max_processes: 16,
                        },
                        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
                            executor_path: executor_path.clone(),
                            scratch_parent: std::env::temp_dir(),
                            runtime_read_execute: Vec::new(),
                            runtime_read_only: Vec::new(),
                            cleanup_timeout: Duration::from_millis(100),
                        },
                    },
                    listener: GatewayServiceListenerConfig {
                        expected_gateway_uid: unsafe { libc::geteuid() },
                        max_connections: 2,
                        idle_timeout: Duration::from_secs(5),
                        accept_poll_interval: Duration::from_millis(5),
                        outcome_maintenance_interval: Duration::from_secs(60),
                        outcome_maintenance_batch: prdb_engine::outcome::MAX_OUTCOME_PRUNE_BATCH,
                    },
                },
                shutdown_server,
                |_manager, _request_id, _stream, _invocation, _submission, _executor| {},
            )
        });

        let mut gateway = UnixStream::connect(&socket_path)?;
        write_frame(
            &mut gateway,
            &IpcFrame {
                operation: Operation::OpenClientSession as u16,
                request_id: 1,
                payload: br#"{"mode":"query","client_name":"listener"}"#.to_vec(),
            },
        )?;
        let response = read_frame(&mut gateway)?.ok_or("missing HELLO response")?;
        assert_eq!(response.operation, Operation::OpenClientSession as u16);
        assert_eq!(response.request_id, 1);
        assert!(std::str::from_utf8(&response.payload)?.contains("\"mode\":\"query\""));
        drop(gateway);
        shutdown.store(true, Ordering::Release);
        server
            .join()
            .map_err(|_| std::io::Error::other("gateway service listener panicked"))??;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    std::fs::remove_file(socket_path)?;
    Ok(())
}

#[test]
fn gateway_disconnect_after_storage_commit_is_reconcilable()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::InvocationStatus;
    use prdb_engine::supervisor_ipc::{
        GatewayAbortReason, GatewayTerminalDecision, finalize_gateway_invocation_and_send_terminal,
        send_gateway_result_data_event, send_gateway_terminal_for_confirmed_outcome,
    };
    use std::os::unix::net::UnixStream;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "terminal_user"),
            object_write_with_name(user_id, 8, "terminal_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let mut invocation = storage.admit_supervisor_invocation(
        id(0x73),
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;
    let mut worker = std::process::Command::new("true").spawn()?;
    invocation.observe_worker_exit(&mut worker)?;
    let (mut service, mut gateway) = UnixStream::pair()?;
    send_gateway_result_data_event(&mut service, 9, b"provisional".to_vec())?;
    let data = read_frame(&mut gateway)?.ok_or("missing provisional event")?;
    assert_eq!(data.operation, Operation::GatewayResultData as u16);
    assert_eq!(data.request_id, 9);
    assert_eq!(data.payload, b"provisional");
    drop(gateway);
    assert!(
        finalize_gateway_invocation_and_send_terminal(
            &mut invocation,
            &mut service,
            9,
            GatewayTerminalDecision::Commit,
        )
        .is_err()
    );
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert!(matches!(
        storage
            .get_invocation_status(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(outcome) if outcome.status == OutcomeStatus::Committed
    ));
    let (mut replay_service, mut replay_gateway) = UnixStream::pair()?;
    assert_eq!(
        send_gateway_terminal_for_confirmed_outcome(
            &storage,
            &invocation,
            &mut replay_service,
            9,
            GatewayAbortReason::InternalError,
        )?,
        GatewayTerminalDecision::Commit
    );
    let reconciled_terminal = read_frame(&mut replay_gateway)?.ok_or("missing terminal replay")?;
    assert_eq!(
        reconciled_terminal.operation,
        Operation::GatewayTerminal as u16
    );
    assert!(String::from_utf8(reconciled_terminal.payload)?.contains("\"outcome\":\"committed\""));
    Ok(())
}

#[test]
fn process_exit_after_durable_commit_preserves_reconcilable_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::outcome::OutcomeStatus;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_AFTER_COMMIT_DATABASE";
    const CHILD_INVOCATION_ID: [u8; 16] = [0xa1; 16];
    const PRINCIPAL_ID: [u8; 16] = [9; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database = Database::open(database_path)?;
        let retention_deadline_us = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_micros()
                .saturating_add(60_000_000),
        )?;
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: CHILD_INVOCATION_ID,
                principal_id: PRINCIPAL_ID,
                retention_deadline_us,
                rows: &[],
                indexes: &[],
                foreign_keys: &[],
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        )?;
        // Simulate losing the supervisor reply and all process-local cleanup
        // immediately after redb reports durable success.
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    drop(initialize(&database_path)?);
    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("process_exit_after_durable_commit_preserves_reconcilable_outcome")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .status()?;
    assert!(
        child_status.success(),
        "commit child exited with {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    let now_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let outcome = storage
        .lookup_invocation_outcome(CHILD_INVOCATION_ID, PRINCIPAL_ID, now_us)?
        .ok_or("committed outcome was not recoverable after process exit")?;
    assert_eq!(outcome.status, OutcomeStatus::Committed);
    assert!(outcome.commit_sequence > 0);
    Ok(())
}

#[test]
fn process_exit_after_atomic_row_index_commit_preserves_publication()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_AFTER_ROW_INDEX_COMMIT_DATABASE";
    const INVOCATION_ID: [u8; 16] = [0xa2; 16];
    const PRINCIPAL_ID: [u8; 16] = [9; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database = Database::open(database_path)?;
        let row_key = key(7);
        let retention_deadline_us = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_micros()
                .saturating_add(60_000_000),
        )?;
        let (catalog_key, catalog_payload) = dependency_record([6; 16], [7; 16], 1);
        let indexed_key = index_key(&row_key);
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: INVOCATION_ID,
                principal_id: PRINCIPAL_ID,
                retention_deadline_us,
                rows: &[RowWrite {
                    key: row_key,
                    expected_version: None,
                    payload: Some(b"durable row".to_vec()),
                }],
                indexes: &[IndexWrite {
                    key: indexed_key,
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
                        key: catalog_key,
                        expected_value: None,
                        payload: Some(catalog_payload),
                    },
                ],
                audit: &[audit_event(0xa3)],
            },
        )?;
        // Simulate process loss after durable publication and before a reply.
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    drop(initialize(&fixture.path())?);
    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("process_exit_after_atomic_row_index_commit_preserves_publication")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, fixture.path())
        .status()?;
    assert!(
        child_status.success(),
        "row/index commit child exited with {child_status}"
    );

    let reopened = Database::open(fixture.path())?;
    let row_key = key(7);
    assert_eq!(row(&reopened, &row_key)?.unwrap().payload, b"durable row");
    let indexed_key = index_key(&row_key);
    let (catalog_key, _) = dependency_record([6; 16], [7; 16], 1);
    let read = reopened.begin_read()?;
    let indexes = read.open_table(schema::INDEXES)?;
    let locator = indexes
        .get(indexed_key.as_slice())?
        .ok_or("index locator was not recovered after process exit")?
        .value()
        .to_vec();
    assert_eq!(
        IndexLocator::decode_for_key(&indexed_key, &locator)?.row_version,
        1
    );
    assert!(
        read.open_table(CATALOG)?
            .get(catalog_key.as_slice())?
            .is_some()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[0xa3; 16][..])?
            .is_some()
    );
    let outcome = InvocationOutcome::decode(
        read.open_table(schema::OUTCOMES)?
            .get(&INVOCATION_ID[..])?
            .ok_or("committed outcome was not recovered after process exit")?
            .value(),
    )?;
    assert_eq!(outcome.status, OutcomeStatus::Committed);
    assert_eq!(outcome.commit_sequence, 1);
    assert_eq!(commit_sequence(&reopened)?, 1);
    Ok(())
}

#[test]
fn supervisor_drains_both_worker_channels_before_durable_finalize()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{IpcFrame, Operation, encode_frame, read_frame};
    use prdb_engine::storage_manager::SupervisorInvocationError;
    use prdb_engine::supervisor_ipc::SUPERVISOR_PAYLOAD_VERSION;
    use std::io::Cursor;
    use std::os::unix::net::UnixStream;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "worker_user"),
            object_write_with_name(user_id, 8, "worker_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let mut invocation = storage.admit_supervisor_invocation(
        id(0x73),
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;

    let (planner_worker, planner_storage) = UnixStream::pair()?;
    let (executor_worker, executor_storage) = UnixStream::pair()?;
    drop(planner_worker);
    drop(executor_worker);
    invocation.serve_worker_channels(planner_storage, executor_storage)?;

    assert!(matches!(
        invocation.serve_terminal(&mut Cursor::new(Vec::new())),
        Err(SupervisorInvocationError::WorkerExitNotObserved)
    ));
    let mut worker = Command::new("true").spawn()?;
    assert!(invocation.observe_worker_exit(&mut worker)?.success());
    let (_executor_worker, mut executor_storage) = UnixStream::pair()?;
    assert!(matches!(
        invocation.serve_executor(&mut executor_storage),
        Err(SupervisorInvocationError::Closed)
    ));

    let terminal = IpcFrame {
        operation: Operation::FinalizeTransaction as u16,
        request_id: 1,
        payload: SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec(),
    };
    let request_bytes = encode_frame(&terminal)?;
    let response_start = request_bytes.len() as u64;
    let mut terminal_stream = Cursor::new(request_bytes);
    invocation.serve_terminal(&mut terminal_stream)?;
    terminal_stream.set_position(response_start);
    let response = read_frame(&mut terminal_stream)?.ok_or("missing terminal response")?;
    assert_eq!(response.operation, Operation::FinalizeTransaction as u16);
    assert_eq!(response.request_id, 1);
    assert_eq!(response.payload[2], 0);
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    Ok(())
}

#[test]
fn supervisor_worker_exit_failure_is_reaped_and_durably_aborted()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::SupervisorInvocationError;
    use prdb_engine::supervisor_ipc::{
        GatewayAbortReason, GatewayTerminalDecision, send_gateway_result_data_event,
        send_gateway_terminal_for_confirmed_outcome,
    };
    use std::io::Cursor;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "worker_exit_user"),
            object_write_with_name(user_id, 8, "worker_exit_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let invocation = storage.admit_supervisor_invocation(
        id(0x73),
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;
    let mut terminal_stream = Cursor::new(Vec::new());
    assert!(
        send_gateway_terminal_for_confirmed_outcome(
            &storage,
            &invocation,
            &mut terminal_stream,
            9,
            GatewayAbortReason::WorkerFailure,
        )
        .is_err()
    );
    assert!(terminal_stream.get_ref().is_empty());

    let mut result_and_terminal_stream = Cursor::new(Vec::new());
    send_gateway_result_data_event(
        &mut result_and_terminal_stream,
        9,
        b"partial SQL output".to_vec(),
    )?;

    let mut worker = Command::new("sh").args(["-c", "exit 7"]).spawn()?;
    assert!(matches!(
        invocation.observe_worker_exit(&mut worker),
        Err(SupervisorInvocationError::WorkerExited(_))
    ));
    assert_eq!(worker.try_wait()?.unwrap().code(), Some(7));
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        send_gateway_terminal_for_confirmed_outcome(
            &storage,
            &invocation,
            &mut result_and_terminal_stream,
            9,
            GatewayAbortReason::WorkerFailure,
        )?,
        GatewayTerminalDecision::Abort(GatewayAbortReason::WorkerFailure)
    );
    result_and_terminal_stream.set_position(0);
    let provisional =
        read_frame(&mut result_and_terminal_stream)?.ok_or("missing provisional result data")?;
    assert_eq!(provisional.operation, Operation::GatewayResultData as u16);
    assert_eq!(provisional.request_id, 9);
    assert_eq!(provisional.payload, b"partial SQL output");
    let terminal =
        read_frame(&mut result_and_terminal_stream)?.ok_or("missing aborted terminal")?;
    assert_eq!(terminal.operation, Operation::GatewayTerminal as u16);
    assert_eq!(terminal.request_id, 9);
    assert!(String::from_utf8(terminal.payload)?.contains("\"outcome\":\"aborted\""));
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
#[ignore = "requires a delegated cgroup and the built TinyCC v3 query fixture"]
fn sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientMode};
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::supervisor::{
        ChunkKind, PrepareSubmission, StagingLimits, StagingStore, SubmissionMetadata,
        SubmissionMetadataValidator, WorkloadClass,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayExecutorConfig, GatewayTerminalDecision,
        observe_executor_relay_and_finalize_gateway,
    };
    use sha2::{Digest, Sha256};
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct AcceptFixture;
    impl SubmissionMetadataValidator for AcceptFixture {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            metadata.manifest == b"m" && metadata.requested_scope == requested_scope_fixture()
        }
    }

    fn required_paths(name: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name)
            .ok_or_else(|| format!("{name} must list the sandbox runtime paths"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    let artifact_path = std::env::var_os("PRDB_TEST_QUERY_ARTIFACT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_QUERY_ARTIFACT must name sql-v3-caught-error.so")?;
    if !cgroup_parent.is_absolute() || !artifact_path.is_absolute() {
        return Err("sandbox test paths must be absolute".into());
    }
    let artifact = std::fs::read(&artifact_path)?;
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "sandbox_sql_user"),
            object_write_with_name(user_id, 8, "sandbox_sql_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut client = ClientConnectionSession::default();
    client.accept_hello(ClientMode::Query, "sandbox-sql-v3")?;
    client.authenticate(&storage, token_id, &secret)?;
    let owner = client.staging_owner()?;
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptFixture))?;
    let reservation = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: b"m".to_vec(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope_fixture(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    staging.upload_chunk(
        owner,
        reservation.invocation_id,
        ChunkKind::Artifact,
        0,
        &artifact,
    )?;
    staging.seal(owner, reservation.invocation_id)?;

    let paths_hex = |value: [u8; 16]| {
        value
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let runtime = GatewayExecutorConfig {
        cgroup_parent,
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 500_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 32,
        },
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-executor")),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?,
            runtime_read_only: required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?,
            cleanup_timeout: Duration::from_secs(2),
        },
    };
    let (accepted, mut invocation, mut executor) = client.handle_start_request_with_executor(
        &storage,
        &staging,
        ClientFrame {
            kind: 7,
            request_id: 1,
            response_sequence: 0,
            payload: format!(
                "{{\"invocation_id\":\"{}\"}}",
                paths_hex(reservation.invocation_id)
            )
            .into_bytes(),
        },
        GatewayAdmissionConfig {
            retention_period_us: 3_600_000_000,
            max_rows: 2,
            max_bytes: 1024,
        },
        &runtime,
    )?;
    assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");

    let (mut service, mut gateway) = UnixStream::pair()?;
    gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    assert_eq!(
        observe_executor_relay_and_finalize_gateway(
            &storage,
            &mut invocation,
            &mut executor,
            &mut service,
            accepted.request_id,
        )?,
        GatewayTerminalDecision::Abort(
            prdb_engine::supervisor_ipc::GatewayAbortReason::WorkerFailure
        )
    );
    staging.finish_started(owner, reservation.invocation_id)?;

    let provisional = read_frame(&mut gateway)?.ok_or("missing provisional SQL output")?;
    assert_eq!(provisional.operation, Operation::GatewayResultData as u16);
    assert_eq!(provisional.request_id, accepted.request_id);
    assert_eq!(provisional.payload, b"partial SQL output");
    let terminal = read_frame(&mut gateway)?.ok_or("missing aborted terminal event")?;
    assert_eq!(terminal.operation, Operation::GatewayTerminal as u16);
    assert_eq!(terminal.request_id, accepted.request_id);
    assert!(String::from_utf8(terminal.payload)?.contains("\"outcome\":\"aborted\""));
    assert_eq!(
        storage
            .lookup_invocation_outcome(reservation.invocation_id, user_id, now + 1)?
            .ok_or("missing durable invocation outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
#[ignore = "requires a delegated cgroup and the built TinyCC restricted metadata fixture"]
fn sandboxed_v3_metadata_submission_commits_over_tcp() -> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_submit::{ClientResultEvent, SubmissionClient, TerminalOutcome};
    use prdb_engine::gateway::serve_tcp_client_connection;
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use prdb_engine::submission_manifest::{
        ArtifactCompatibilityProfile, VersionedSubmissionMetadataValidator, encode_manifest_v1,
        manifest_v1_from_elf,
    };
    use prdb_engine::supervisor::{StagingLimits, StagingStore, WorkloadClass};
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayClientServiceEvent, GatewayExecutorConfig,
        GatewayTerminalDecision, observe_executor_relay_and_finalize_gateway,
        serve_gateway_client_session_with_executor,
    };
    use prdb_engine::tuple::ScalarType;
    use sha2::{Digest, Sha256};
    use std::collections::HashSet;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn required_paths(name: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name)
            .ok_or_else(|| format!("{name} must list the sandbox runtime paths"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    let artifact_path = std::env::var_os("PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT must name the TinyCC fixture")?;
    if !cgroup_parent.is_absolute() || !artifact_path.is_absolute() {
        return Err("sandbox test paths must be absolute".into());
    }
    let artifact = std::fs::read(artifact_path)?;
    let manifest = manifest_v1_from_elf(
        &artifact,
        3,
        1,
        "x86_64-v1".to_owned(),
        "test-runtime".to_owned(),
        "TinyCC integration test".to_owned(),
    )
    .map_err(|error| format!("manifest inspection failed: {error:?}"))?;
    let manifest_bytes = encode_manifest_v1(&manifest)
        .map_err(|error| format!("manifest encoding failed: {error:?}"))?;
    let validator = VersionedSubmissionMetadataValidator::new(
        ArtifactCompatibilityProfile::new(
            manifest.cpu_baseline.clone(),
            manifest.runtime_profile.clone(),
            manifest.sdk_release,
            manifest
                .dependencies
                .iter()
                .cloned()
                .collect::<HashSet<_>>(),
        )
        .map_err(|error| format!("compatibility profile failed: {error:?}"))?,
    );

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let user_id = id(0xc0);
    let token_id = id(0xc1);
    let secret = [0x53; 32];
    let restricted_user_id = id(0xcd);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xc2, admin_role, user_id);
    setup_audit.action = AuditAction::AuthorityChange;
    setup_audit.correlation_id = id(0xc2);
    storage
        .commit_catalog_and_audit(
            id(0xc2),
            admin_role,
            now + 3_600_000_000,
            &[
                principal_write_with_name(user_id, 1, 1, "tcp_submission_admin"),
                object_write_with_name(user_id, 8, "tcp_submission_admin"),
                principal_write_with_name(restricted_user_id, 1, 1, "tcp_submission_user"),
                object_write_with_name(restricted_user_id, 8, "tcp_submission_user"),
                credential_verifier_write(
                    token_id,
                    restricted_user_id,
                    &secret,
                    now - 1_000_000,
                    now + 60_000_000,
                    None,
                    1,
                ),
                membership_write(user_id, admin_role),
                dependency_write(user_id, admin_role, 5),
            ],
            &[setup_audit],
        )
        .map_err(|error| format!("TCP identity setup failed: {error:?}"))?;

    let empty_schema_invocation = id(0xc3);
    let mut empty_schema_audit = identity_audit_event(0xc4, user_id, id(0xc5));
    empty_schema_audit.correlation_id = empty_schema_invocation;
    let empty_schema = storage
        .create_schema(
            empty_schema_invocation,
            user_id,
            "empty_roundtrip",
            user_id,
            now + 3_600_000_000,
            empty_schema_audit,
        )
        .map_err(|error| format!("TCP empty schema creation failed: {error:?}"))?;
    let hidden_schema_invocation = id(0xc6);
    let mut hidden_schema_audit = identity_audit_event(0xc7, user_id, id(0xc8));
    hidden_schema_audit.correlation_id = hidden_schema_invocation;
    let hidden_schema = storage
        .create_schema(
            hidden_schema_invocation,
            user_id,
            "sql_roundtrip",
            user_id,
            now + 3_600_000_000,
            hidden_schema_audit,
        )
        .map_err(|error| format!("TCP hidden schema creation failed: {error:?}"))?;
    let hidden_table_invocation = id(0xcb);
    let mut hidden_table_audit = identity_audit_event(0xcc, user_id, hidden_schema.schema_id);
    hidden_table_audit.correlation_id = hidden_table_invocation;
    storage.create_table(CreateTableRequest {
        invocation_id: hidden_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: hidden_schema.schema_id,
        name: "records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: hidden_table_audit,
    })?;
    let grant_invocation = id(0xc9);
    let mut grant_audit = identity_audit_event(0xca, user_id, empty_schema.schema_id);
    grant_audit.action = AuditAction::AuthorityChange;
    grant_audit.correlation_id = grant_invocation;
    let (old_user_record, old_token_record) = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        (
            catalog
                .get(id_catalog_key(10, restricted_user_id).as_slice())?
                .ok_or("TCP restricted submission user record missing")?
                .value()
                .to_vec(),
            catalog
                .get(id_catalog_key(16, token_id).as_slice())?
                .ok_or("TCP submission token record missing")?
                .value()
                .to_vec(),
        )
    };
    let mut authority_writes = vec![
        grant_write(
            restricted_user_id,
            empty_schema.schema_id,
            1,
            1,
            empty_schema.schema_id,
            user_id,
        ),
        dependency_write(restricted_user_id, empty_schema.schema_id, 5),
    ];
    let mut bumped_user =
        principal_write_with_name(restricted_user_id, 1, 2, "tcp_submission_user");
    bumped_user.expected_value = Some(old_user_record);
    authority_writes.push(bumped_user);
    let mut refreshed_token = credential_verifier_write(
        token_id,
        restricted_user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        2,
    );
    refreshed_token.expected_value = Some(old_token_record);
    authority_writes.push(refreshed_token);
    storage
        .commit_catalog_and_audit(
            grant_invocation,
            user_id,
            now + 3_600_000_000,
            &authority_writes,
            &[grant_audit],
        )
        .map_err(|error| format!("TCP schema grant failed: {error:?}"))?;

    let requested_scope = [
        b"PSC1\0\x01".as_slice(),
        &[1, 1],
        empty_schema.schema_id.as_slice(),
    ]
    .concat();
    let package = fixture.0.join("package");
    std::fs::create_dir(&package)?;
    std::fs::set_permissions(&package, std::fs::Permissions::from_mode(0o700))?;
    std::fs::write(package.join("query.so"), &artifact)?;
    std::fs::write(package.join("manifest.pmf1"), &manifest_bytes)?;
    std::fs::write(package.join("requested_scope.psc1"), &requested_scope)?;
    let artifact_digest = Sha256::digest(&artifact)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        + "\n";
    std::fs::write(package.join("artifact.sha256"), artifact_digest)?;
    for name in [
        "query.so",
        "manifest.pmf1",
        "requested_scope.psc1",
        "artifact.sha256",
    ] {
        std::fs::set_permissions(package.join(name), std::fs::Permissions::from_mode(0o600))?;
    }
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(validator))?;
    let runtime = GatewayExecutorConfig {
        cgroup_parent,
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 500_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 32,
        },
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-executor")),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?,
            runtime_read_only: required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?,
            cleanup_timeout: Duration::from_secs(2),
        },
    };
    let admission = GatewayAdmissionConfig {
        retention_period_us: 3_600_000_000,
        max_rows: 8,
        max_bytes: 4096,
    };
    let (mut gateway_service, mut supervisor_service) = UnixStream::pair()?;
    let tcp_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = tcp_listener.local_addr()?;
    let receipt = fixture.0.join("invocation.receipt");

    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let gateway = scope.spawn(move || -> Result<(), String> {
            let (mut client_stream, _) = tcp_listener
                .accept()
                .map_err(|error| format!("TCP accept failed: {error}"))?;
            serve_tcp_client_connection(&mut client_stream, &mut gateway_service)
                .map_err(|error| format!("TCP gateway failed: {error:?}"))?;
            Ok(())
        });
        let supervisor = scope.spawn(|| -> Result<(), String> {
            let mut session = prdb_engine::client_protocol::ClientConnectionSession::default();
            loop {
                match serve_gateway_client_session_with_executor(
                    &mut supervisor_service,
                    &storage,
                    &staging,
                    &mut session,
                    admission,
                    &runtime,
                )
                .map_err(|error| format!("supervisor session failed: {error:?}"))?
                {
                    GatewayClientServiceEvent::Disconnected => return Ok(()),
                    GatewayClientServiceEvent::Started {
                        request_id,
                        mut invocation,
                        executor: Some(mut executor),
                        ..
                    } => {
                        let schemas = invocation
                            .scan_information_schema_schemata(128)
                            .map_err(|error| format!("TCP metadata API scan failed: {error:?}"))?
                            .rows;
                        if !schemas
                            .iter()
                            .any(|row| row.schema_name == "empty_roundtrip")
                            || schemas.iter().any(|row| row.schema_name == "sql_roundtrip")
                        {
                            return Err("TCP invocation metadata scope was not enforced".into());
                        }
                        let mut tables = Vec::new();
                        let mut table_cursor = None;
                        loop {
                            let page = invocation
                                .scan_information_schema_tables(128, table_cursor)
                                .map_err(|error| {
                                    format!("TCP metadata table API scan failed: {error:?}")
                                })?;
                            tables.extend(page.rows);
                            table_cursor = page.continuation;
                            if table_cursor.is_none() {
                                break;
                            }
                        }
                        if !tables
                            .iter()
                            .any(|row| row.table_schema == "information_schema")
                            || tables.iter().any(|row| row.table_schema == "sql_roundtrip")
                        {
                            return Err("TCP metadata table scope was not enforced".into());
                        }
                        let mut columns = Vec::new();
                        let mut column_cursor = None;
                        loop {
                            let page = invocation
                                .scan_information_schema_columns(128, column_cursor)
                                .map_err(|error| {
                                    format!("TCP metadata column API scan failed: {error:?}")
                                })?;
                            columns.extend(page.rows);
                            column_cursor = page.continuation;
                            if column_cursor.is_none() {
                                break;
                            }
                        }
                        if !columns
                            .iter()
                            .any(|row| row.table_schema == "information_schema")
                            || columns
                                .iter()
                                .any(|row| row.table_schema == "sql_roundtrip")
                        {
                            return Err("TCP metadata column scope was not enforced".into());
                        }
                        let decision = observe_executor_relay_and_finalize_gateway(
                            &storage,
                            &mut invocation,
                            &mut executor,
                            &mut supervisor_service,
                            request_id,
                        )
                        .map_err(|error| format!("executor relay failed: {error:?}"))?;
                        if decision != GatewayTerminalDecision::Commit {
                            return Err(format!("TCP submission aborted: {decision:?}"));
                        }
                    }
                    GatewayClientServiceEvent::Started { executor: None, .. } => {
                        return Err("supervisor did not launch the sandbox executor".into());
                    }
                }
            }
        });

        let mut client_secret = secret;
        let mut client =
            SubmissionClient::connect_authenticated(address, token_id, &mut client_secret)?;
        assert_eq!(client_secret, [0; 32]);
        let invocation_id =
            client.upload_and_seal(&package, &[], 30_000, WorkloadClass::Operational)?;
        {
            let mut started = client.start(invocation_id, &receipt)?;
            assert_eq!(
                started.next_event()?,
                Some(ClientResultEvent::Data(b"empty_roundtrip".to_vec()))
            );
            assert!(matches!(
                started.next_event()?,
                Some(ClientResultEvent::Terminal(terminal))
                    if terminal.invocation_id == invocation_id
                        && terminal.outcome == TerminalOutcome::Committed
                        && terminal.reason_code == "success"
            ));
            assert_eq!(started.next_event()?, None);
        }
        assert_eq!(
            client.status(invocation_id)?,
            prdb_engine::client_submit::InvocationStatus::Committed
        );
        drop(client);

        gateway
            .join()
            .map_err(|_| std::io::Error::other("TCP gateway thread panicked"))?
            .map_err(std::io::Error::other)?;
        supervisor
            .join()
            .map_err(|_| std::io::Error::other("supervisor thread panicked"))?
            .map_err(std::io::Error::other)?;
        assert_eq!(
            storage
                .lookup_invocation_outcome(invocation_id, restricted_user_id, now + 1)?
                .ok_or("TCP submission outcome missing from storage")?
                .status,
            OutcomeStatus::Committed
        );
        Ok(())
    })?;
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
#[ignore = "requires a delegated cgroup and the built TinyCC SQL write/read metadata fixture"]
fn sandboxed_v3_sql_dml_sequence_is_visible_and_commits() -> Result<(), Box<dyn std::error::Error>>
{
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientMode};
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use prdb_engine::supervisor::{
        ChunkKind, PrepareSubmission, StagingLimits, StagingStore, SubmissionMetadata,
        SubmissionMetadataValidator, WorkloadClass,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayExecutorConfig, GatewayTerminalDecision,
        finalize_gateway_invocation_and_send_terminal, observe_executor_and_relay_results,
        observe_executor_relay_and_finalize_gateway,
    };
    use prdb_engine::tuple::ScalarType;
    use sha2::{Digest, Sha256};
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct AcceptFixture {
        requested_scope: Vec<u8>,
    }
    impl SubmissionMetadataValidator for AcceptFixture {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            metadata.manifest == b"m" && metadata.requested_scope == self.requested_scope
        }
    }

    fn required_paths(name: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name)
            .ok_or_else(|| format!("{name} must list the sandbox runtime paths"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    let artifact_path = std::env::var_os("PRDB_TEST_WRITE_READ_QUERY_ARTIFACT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_WRITE_READ_QUERY_ARTIFACT must name sql-v3-write-read.so")?;
    let restricted_artifact_path = std::env::var_os("PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT")
        .map(PathBuf::from)
        .ok_or(
            "PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT must name sql-v3-restricted-metadata.so",
        )?;
    if !cgroup_parent.is_absolute()
        || !artifact_path.is_absolute()
        || !restricted_artifact_path.is_absolute()
    {
        return Err("sandbox test paths must be absolute".into());
    }
    let artifact = std::fs::read(&artifact_path)?;
    let restricted_artifact = std::fs::read(&restricted_artifact_path)?;
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x7a);
    let token_id = id(0x7b);
    let secret = [0x42; 32];
    let restricted_user_id = id(0xb0);
    let restricted_token_id = id(0xb1);
    let restricted_secret = [0x43; 32];
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x7c, admin_role, user_id);
    setup_audit.action = AuditAction::AuthorityChange;
    setup_audit.correlation_id = id(0x7c);
    let setup_invocation = id(0x7c);
    StorageManager::new(&database, Duration::from_millis(100))?.commit_catalog_and_audit(
        setup_invocation,
        admin_role,
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "sandbox_sql_roundtrip"),
            object_write_with_name(user_id, 8, "sandbox_sql_roundtrip"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
            principal_write_with_name(restricted_user_id, 1, 1, "sandbox_sql_restricted"),
            object_write_with_name(restricted_user_id, 8, "sandbox_sql_restricted"),
            credential_verifier_write(
                restricted_token_id,
                restricted_user_id,
                &restricted_secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
            membership_write(user_id, admin_role),
            dependency_write(user_id, admin_role, 5),
        ],
        &[setup_audit],
    )?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let schema_invocation = id(0x7d);
    let mut schema_audit = identity_audit_event(0x7e, user_id, id(0x7f));
    schema_audit.correlation_id = schema_invocation;
    let schema = storage.create_schema(
        schema_invocation,
        user_id,
        "sql_roundtrip",
        user_id,
        now + 3_600_000_000,
        schema_audit,
    )?;
    let empty_schema_invocation = id(0xa2);
    let mut empty_schema_audit = identity_audit_event(0xa3, user_id, id(0xa4));
    empty_schema_audit.correlation_id = empty_schema_invocation;
    let empty_schema = storage.create_schema(
        empty_schema_invocation,
        user_id,
        "empty_roundtrip",
        user_id,
        now + 3_600_000_000,
        empty_schema_audit,
    )?;
    let hidden_schema_invocation = id(0xa5);
    let mut hidden_schema_audit = identity_audit_event(0xa6, user_id, id(0xa7));
    hidden_schema_audit.correlation_id = hidden_schema_invocation;
    let hidden_schema = storage.create_schema(
        hidden_schema_invocation,
        user_id,
        "hidden_roundtrip",
        user_id,
        now + 3_600_000_000,
        hidden_schema_audit,
    )?;
    let table_invocation = id(0x80);
    let mut table_audit = identity_audit_event(0x81, user_id, id(0x82));
    table_audit.correlation_id = table_invocation;
    let records_table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: table_audit,
    })?;
    let joined_table_invocation = id(0xc1);
    let mut joined_table_audit = identity_audit_event(0xc2, user_id, id(0xc3));
    joined_table_audit.correlation_id = joined_table_invocation;
    let joined_table = storage.create_table(CreateTableRequest {
        invocation_id: joined_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "linked_records".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "id".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "match_key".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: joined_table_audit,
    })?;
    let composite_table_invocation = id(0xd0);
    let mut composite_table_audit = identity_audit_event(0xd1, user_id, schema.schema_id);
    composite_table_audit.correlation_id = composite_table_invocation;
    let composite_table = storage.create_table(CreateTableRequest {
        invocation_id: composite_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: schema.schema_id,
        name: "grouped_records".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "left_key".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "right_key".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: composite_table_audit,
    })?;
    let composite_schema = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
    ];
    let composite_rows = [(0xd3, 1, 7), (0xd4, 1, 7), (0xd5, 2, 9)]
        .into_iter()
        .map(|(row_id, left, right)| {
            Ok(RowWrite {
                key: table_row_key(composite_table.table_id, id(row_id)),
                expected_version: None,
                payload: Some(encode_tuple(
                    &composite_schema,
                    1,
                    &[
                        Some(ScalarValue::Integer(left)),
                        Some(ScalarValue::Integer(right)),
                    ],
                )?),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    commit_rows(
        &database,
        id(0xd6),
        builtin_owner_id(),
        now + 3_600_000_000,
        &composite_rows,
    )?;
    for row_id in [0xd3, 0xd4, 0xd5] {
        assert!(
            row(
                &database,
                &table_row_key(composite_table.table_id, id(row_id))
            )?
            .is_some()
        );
    }
    let joined_schema = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: true,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: true,
        },
    ];
    let joined_rows = [(0xc5, 42, 7), (0xca, 1, 7)]
        .into_iter()
        .map(|(row_id, id_value, match_key)| {
            Ok(RowWrite {
                key: table_row_key(joined_table.table_id, id(row_id)),
                expected_version: None,
                payload: Some(encode_tuple(
                    &joined_schema,
                    1,
                    &[
                        Some(ScalarValue::Integer(id_value)),
                        Some(ScalarValue::Integer(match_key)),
                    ],
                )?),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    commit_rows(
        &database,
        id(0xc4),
        builtin_owner_id(),
        now + 3_600_000_000,
        &joined_rows,
    )?;
    let private_table_invocation = id(0xa0);
    let mut private_table_audit = identity_audit_event(0xa1, user_id, schema.schema_id);
    private_table_audit.correlation_id = private_table_invocation;
    let _private_table = storage
        .create_table(CreateTableRequest {
            invocation_id: private_table_invocation,
            authenticated_actor_id: user_id,
            schema_id: schema.schema_id,
            name: "private_records".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "secret".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: now + 3_600_000_000,
            audit_event: private_table_audit,
        })
        .map_err(|error| format!("private metadata fixture table creation failed: {error:?}"))?;
    let hidden_table_invocation = id(0xa8);
    let mut hidden_table_audit = identity_audit_event(0xa9, user_id, hidden_schema.schema_id);
    hidden_table_audit.correlation_id = hidden_table_invocation;
    let _hidden_table = storage.create_table(CreateTableRequest {
        invocation_id: hidden_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: hidden_schema.schema_id,
        name: "hidden_records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "secret".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: hidden_table_audit,
    })?;

    let authority_invocation = id(0x83);
    let mut authority_audit = identity_audit_event(0x84, user_id, user_id);
    authority_audit.action = AuditAction::AuthorityChange;
    authority_audit.correlation_id = authority_invocation;
    let mut authority_writes = vec![
        grant_write(user_id, schema.schema_id, 1, 1, schema.schema_id, user_id),
        grant_write(
            user_id,
            empty_schema.schema_id,
            1,
            1,
            empty_schema.schema_id,
            user_id,
        ),
        grant_write(
            restricted_user_id,
            empty_schema.schema_id,
            1,
            1,
            empty_schema.schema_id,
            user_id,
        ),
        dependency_write(user_id, schema.schema_id, 5),
        dependency_write(user_id, empty_schema.schema_id, 5),
        dependency_write(restricted_user_id, empty_schema.schema_id, 5),
        dependency_write(user_id, records_table.table_id, 5),
        dependency_write(user_id, records_table.column_ids[0], 5),
        dependency_write(user_id, joined_table.table_id, 5),
        dependency_write(user_id, joined_table.column_ids[0], 5),
        dependency_write(user_id, joined_table.column_ids[1], 5),
        dependency_write(user_id, composite_table.table_id, 5),
        dependency_write(user_id, composite_table.column_ids[0], 5),
        dependency_write(user_id, composite_table.column_ids[1], 5),
    ];
    authority_writes.extend((2..=5).map(|privilege| {
        grant_write(
            user_id,
            records_table.table_id,
            privilege,
            1,
            records_table.table_id,
            user_id,
        )
    }));
    authority_writes.extend((2..=4).map(|privilege| {
        grant_write(
            user_id,
            records_table.table_id,
            privilege,
            2,
            records_table.column_ids[0],
            user_id,
        )
    }));
    authority_writes.extend([
        grant_write(
            user_id,
            joined_table.table_id,
            2,
            1,
            joined_table.table_id,
            user_id,
        ),
        grant_write(
            user_id,
            joined_table.table_id,
            2,
            2,
            joined_table.column_ids[0],
            user_id,
        ),
        grant_write(
            user_id,
            joined_table.table_id,
            2,
            2,
            joined_table.column_ids[1],
            user_id,
        ),
        grant_write(
            user_id,
            composite_table.table_id,
            2,
            1,
            composite_table.table_id,
            user_id,
        ),
    ]);
    authority_writes.extend(composite_table.column_ids.iter().map(|column_id| {
        grant_write(user_id, composite_table.table_id, 2, 2, *column_id, user_id)
    }));
    let (
        old_user_record,
        old_token_record,
        old_restricted_user_record,
        old_restricted_token_record,
    ) = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        (
            catalog
                .get(id_catalog_key(10, user_id).as_slice())?
                .ok_or("sandbox SQL user record missing")?
                .value()
                .to_vec(),
            catalog
                .get(id_catalog_key(16, token_id).as_slice())?
                .ok_or("sandbox SQL token record missing")?
                .value()
                .to_vec(),
            catalog
                .get(id_catalog_key(10, restricted_user_id).as_slice())?
                .ok_or("restricted sandbox SQL user record missing")?
                .value()
                .to_vec(),
            catalog
                .get(id_catalog_key(16, restricted_token_id).as_slice())?
                .ok_or("restricted sandbox SQL token record missing")?
                .value()
                .to_vec(),
        )
    };
    let mut bumped_user = principal_write_with_name(user_id, 1, 2, "sandbox_sql_roundtrip");
    bumped_user.expected_value = Some(old_user_record);
    authority_writes.push(bumped_user);
    let mut refreshed_token = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        2,
    );
    refreshed_token.expected_value = Some(old_token_record);
    authority_writes.push(refreshed_token);
    let mut bumped_restricted_user =
        principal_write_with_name(restricted_user_id, 1, 2, "sandbox_sql_restricted");
    bumped_restricted_user.expected_value = Some(old_restricted_user_record);
    authority_writes.push(bumped_restricted_user);
    let mut refreshed_restricted_token = credential_verifier_write(
        restricted_token_id,
        restricted_user_id,
        &restricted_secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        2,
    );
    refreshed_restricted_token.expected_value = Some(old_restricted_token_record);
    authority_writes.push(refreshed_restricted_token);
    storage
        .commit_catalog_and_audit(
            authority_invocation,
            user_id,
            now + 3_600_000_000,
            &authority_writes,
            &[authority_audit],
        )
        .map_err(|error| format!("metadata fixture grants failed: {error:?}"))?;

    let insert_policy_id = id(0x85);
    let insert_policy_expression_id = id(0x86);
    let insert_cell_policy_id = id(0x89);
    let insert_policy_key = id_catalog_key(13, insert_policy_id);
    let insert_policy_payload = encode_envelope(
        &family_schema(13).unwrap(),
        &insert_policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: records_table.table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 2u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Id,
                payload: insert_policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let policy_target_payload =
        prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
            policy_id: insert_policy_id,
            kind: 1,
            target_id: user_id,
        })?;
    let cell_policy_key = id_catalog_key(13, insert_cell_policy_id);
    let cell_policy_payload = encode_envelope(
        &family_schema(13).unwrap(),
        &cell_policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: records_table.table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 2u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 2u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Id,
                payload: insert_policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let cell_policy_recipient =
        prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
            policy_id: insert_cell_policy_id,
            kind: 1,
            target_id: user_id,
        })?;
    let cell_policy_column =
        prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
            policy_id: insert_cell_policy_id,
            kind: 3,
            target_id: records_table.column_ids[0],
        })?;
    let insert_policy_expression_key = id_catalog_key(7, insert_policy_expression_id);
    let insert_policy_expression_ir =
        prdb_engine::expression::encode_expression(&prdb_engine::authorization::Expr::Literal(
            Some(prdb_engine::tuple::ScalarValue::Boolean(true)),
        ))?;
    let insert_policy_expression = CatalogWrite {
        key: insert_policy_expression_key.clone(),
        expected_value: None,
        payload: Some(encode_envelope(
            &family_schema(7).unwrap(),
            &insert_policy_expression_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 4u16.to_le_bytes().to_vec(),
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
                    payload: insert_policy_expression_ir,
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Ids,
                    payload: 0u32.to_le_bytes().to_vec(),
                },
            ],
        )?),
    };
    let mut insert_policy_table_epoch =
        table_write_with_versions(records_table.table_id, schema.schema_id, 1, 2);
    insert_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, records_table.table_id).as_slice())?
            .ok_or("sandbox SQL table record missing")?
            .value()
            .to_vec(),
    );
    let mut joined_policy_table_epoch =
        table_write_with_versions(joined_table.table_id, schema.schema_id, 1, 2);
    joined_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, joined_table.table_id).as_slice())?
            .ok_or("sandbox joined table record missing")?
            .value()
            .to_vec(),
    );
    let mut composite_policy_table_epoch =
        table_write_with_versions(composite_table.table_id, schema.schema_id, 1, 2);
    composite_policy_table_epoch.expected_value = Some(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(id_catalog_key(5, composite_table.table_id).as_slice())?
            .ok_or("sandbox composite table record missing")?
            .value()
            .to_vec(),
    );
    let select_using_id = id(0x8a);
    let update_using_id = id(0x8b);
    let mut policy_writes = vec![
        CatalogWrite {
            key: insert_policy_key,
            expected_value: None,
            payload: Some(insert_policy_payload),
        },
        CatalogWrite {
            key: cell_policy_key,
            expected_value: None,
            payload: Some(cell_policy_payload),
        },
        object_write_with_parent_and_owner_and_name(
            insert_policy_id,
            10,
            None,
            user_id,
            "sandbox_insert_policy",
        ),
        object_write_with_parent_and_owner_and_name(
            insert_cell_policy_id,
            10,
            None,
            user_id,
            "sandbox_insert_cell_policy",
        ),
        insert_policy_expression,
        object_write_with_parent_and_owner_and_name(
            insert_policy_expression_id,
            5,
            None,
            user_id,
            "sandbox_insert_policy_check",
        ),
        CatalogWrite {
            key: policy_target_payload[32..].to_vec(),
            expected_value: None,
            payload: Some(policy_target_payload),
        },
        CatalogWrite {
            key: cell_policy_recipient[32..].to_vec(),
            expected_value: None,
            payload: Some(cell_policy_recipient),
        },
        CatalogWrite {
            key: cell_policy_column[32..].to_vec(),
            expected_value: None,
            payload: Some(cell_policy_column),
        },
        dependency_write(insert_policy_id, insert_policy_expression_id, 2),
        dependency_write(insert_policy_id, user_id, 5),
        dependency_write(insert_cell_policy_id, insert_policy_expression_id, 2),
        dependency_write(insert_cell_policy_id, user_id, 5),
        insert_policy_table_epoch,
        joined_policy_table_epoch,
        composite_policy_table_epoch,
    ];
    let make_using_expression = |expression_id: [u8; 16], name: &str| {
        let key = id_catalog_key(7, expression_id);
        let ir = prdb_engine::expression::encode_expression(
            &prdb_engine::authorization::Expr::Literal(Some(
                prdb_engine::tuple::ScalarValue::Boolean(true),
            )),
        )?;
        let expression = CatalogWrite {
            key: key.clone(),
            expected_value: None,
            payload: Some(encode_envelope(
                &family_schema(7).unwrap(),
                &key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: 3u16.to_le_bytes().to_vec(),
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
                        payload: ir,
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: 0u32.to_le_bytes().to_vec(),
                    },
                ],
            )?),
        };
        Ok::<_, Box<dyn std::error::Error>>((
            expression,
            object_write_with_parent_and_owner_and_name(expression_id, 5, None, user_id, name),
        ))
    };
    let (select_using, select_using_object) =
        make_using_expression(select_using_id, "sandbox_select_using")?;
    let (update_using, update_using_object) =
        make_using_expression(update_using_id, "sandbox_update_using")?;
    policy_writes.extend([
        select_using,
        select_using_object,
        update_using,
        update_using_object,
    ]);

    let mut add_table_policy = |table_id: [u8; 16],
                                column_id: [u8; 16],
                                policy_id: [u8; 16],
                                operation: u16,
                                using_id: Option<[u8; 16]>,
                                check_id: Option<[u8; 16]>,
                                column_target: bool,
                                name: &str|
     -> Result<(), Box<dyn std::error::Error>> {
        let key = id_catalog_key(13, policy_id);
        let mut fields = vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: if column_target { 2u16 } else { 1u16 }
                    .to_le_bytes()
                    .to_vec(),
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
        ];
        if let Some(expression_id) = using_id {
            fields.push(CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        if let Some(expression_id) = check_id {
            fields.push(CatalogField {
                tag: 6,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        fields.push(CatalogField {
            tag: 7,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        });
        policy_writes.extend([
            CatalogWrite {
                key: key.clone(),
                expected_value: None,
                payload: Some(encode_envelope(&family_schema(13).unwrap(), &key, &fields)?),
            },
            object_write_with_parent_and_owner_and_name(policy_id, 10, None, user_id, name),
        ]);
        let recipient =
            prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
                policy_id,
                kind: 1,
                target_id: user_id,
            })?;
        policy_writes.push(CatalogWrite {
            key: recipient[32..].to_vec(),
            expected_value: None,
            payload: Some(recipient),
        });
        if column_target {
            let column =
                prdb_engine::catalog::encode_policy_target(&prdb_engine::catalog::PolicyTarget {
                    policy_id,
                    kind: 3,
                    target_id: column_id,
                })?;
            policy_writes.push(CatalogWrite {
                key: column[32..].to_vec(),
                expected_value: None,
                payload: Some(column),
            });
        }
        if let Some(expression_id) = using_id {
            policy_writes.push(dependency_write(policy_id, expression_id, 2));
        }
        if let Some(expression_id) = check_id {
            policy_writes.push(dependency_write(policy_id, expression_id, 2));
        }
        policy_writes.push(dependency_write(policy_id, user_id, 5));
        Ok(())
    };
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x8c),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_select_policy",
    )?;
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x8d),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_select_cell_policy",
    )?;
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x8e),
        3,
        Some(update_using_id),
        Some(insert_policy_expression_id),
        false,
        "sandbox_update_policy",
    )?;
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x8f),
        3,
        Some(update_using_id),
        Some(insert_policy_expression_id),
        true,
        "sandbox_update_cell_policy",
    )?;
    add_table_policy(
        records_table.table_id,
        records_table.column_ids[0],
        id(0x90),
        4,
        Some(select_using_id),
        None,
        false,
        "sandbox_delete_policy",
    )?;
    add_table_policy(
        joined_table.table_id,
        joined_table.column_ids[0],
        id(0xc6),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_join_select_policy",
    )?;
    add_table_policy(
        joined_table.table_id,
        joined_table.column_ids[0],
        id(0xc7),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_join_select_cell_policy",
    )?;
    add_table_policy(
        joined_table.table_id,
        joined_table.column_ids[1],
        id(0xcb),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_join_match_key_select_cell_policy",
    )?;
    add_table_policy(
        composite_table.table_id,
        composite_table.column_ids[0],
        id(0xd7),
        1,
        Some(select_using_id),
        None,
        false,
        "sandbox_composite_select_policy",
    )?;
    add_table_policy(
        composite_table.table_id,
        composite_table.column_ids[0],
        id(0xd8),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_composite_left_select_policy",
    )?;
    add_table_policy(
        composite_table.table_id,
        composite_table.column_ids[1],
        id(0xd9),
        1,
        Some(select_using_id),
        None,
        true,
        "sandbox_composite_right_select_policy",
    )?;
    let policy_invocation = id(0x87);
    let mut policy_audit = identity_audit_event(0x88, user_id, insert_policy_id);
    policy_audit.correlation_id = policy_invocation;
    storage
        .commit_authorized_policy_ddl(
            policy_invocation,
            user_id,
            now + 3_600_000_000,
            &policy_writes,
            &[policy_audit],
        )
        .map_err(|error| format!("metadata fixture policies failed: {error:?}"))?;
    let mut requested_scope_records = Vec::<Vec<u8>>::new();
    for schema_id in [schema.schema_id, empty_schema.schema_id] {
        let mut record = vec![1, 1];
        record.extend_from_slice(&schema_id);
        requested_scope_records.push(record);
    }
    for operation in 2u8..=5 {
        let mut tables = if operation == 2 {
            vec![&records_table, &joined_table, &composite_table]
        } else {
            vec![&records_table]
        };
        tables.sort_by_key(|table| table.table_id);
        for table in &tables {
            let mut record = vec![operation, 2];
            record.extend_from_slice(&table.table_id);
            requested_scope_records.push(record);
        }
        if operation <= 4 {
            for table in &tables {
                for column_id in &table.column_ids {
                    let mut record = vec![operation, 3];
                    record.extend_from_slice(&table.table_id);
                    record.extend_from_slice(column_id);
                    requested_scope_records.push(record);
                }
            }
        }
    }
    requested_scope_records.sort();
    requested_scope_records.dedup();
    let scope_count = u16::try_from(requested_scope_records.len())?;
    let mut requested_scope = b"PSC1".to_vec();
    requested_scope.extend_from_slice(&scope_count.to_be_bytes());
    for record in requested_scope_records {
        requested_scope.extend_from_slice(&record);
    }

    let mut client = ClientConnectionSession::default();
    client.accept_hello(ClientMode::Query, "sandbox-sql-write-read")?;
    client.authenticate(&storage, token_id, &secret)?;
    let owner = client.staging_owner()?;
    let staging = StagingStore::new(
        StagingLimits::default(),
        Arc::new(AcceptFixture {
            requested_scope: requested_scope.clone(),
        }),
    )?;
    let reservation = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: b"m".to_vec(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope,
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    staging.upload_chunk(
        owner,
        reservation.invocation_id,
        ChunkKind::Artifact,
        0,
        &artifact,
    )?;
    staging.seal(owner, reservation.invocation_id)?;

    let invocation_hex = reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let runtime = GatewayExecutorConfig {
        cgroup_parent,
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 500_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 32,
        },
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-executor")),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?,
            runtime_read_only: required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?,
            cleanup_timeout: Duration::from_secs(2),
        },
    };
    let (accepted, mut invocation, mut executor) = client.handle_start_request_with_executor(
        &storage,
        &staging,
        ClientFrame {
            kind: 7,
            request_id: 1,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes(),
        },
        GatewayAdmissionConfig {
            retention_period_us: 3_600_000_000,
            max_rows: 8,
            max_bytes: 4096,
        },
        &runtime,
    )?;
    assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");

    let schema_rows = invocation.scan_information_schema_schemata(128)?.rows;
    let mut table_rows = Vec::new();
    let mut table_cursor = None;
    loop {
        let page = invocation.scan_information_schema_tables(128, table_cursor)?;
        table_rows.extend(page.rows);
        table_cursor = page.continuation;
        if table_cursor.is_none() {
            break;
        }
    }
    let mut column_rows = Vec::new();
    let mut column_cursor = None;
    loop {
        let page = invocation.scan_information_schema_columns(128, column_cursor)?;
        column_rows.extend(page.rows);
        column_cursor = page.continuation;
        if column_cursor.is_none() {
            break;
        }
    }
    assert!(
        !table_rows
            .iter()
            .any(|row| row.table_name == "private_records")
    );
    assert!(
        !column_rows
            .iter()
            .any(|row| row.table_name == "private_records")
    );
    assert!(
        !schema_rows
            .iter()
            .any(|row| row.schema_name == "hidden_roundtrip")
    );
    assert!(
        !schema_rows
            .iter()
            .any(|row| row.schema_name == "missing_roundtrip")
    );
    assert!(
        !table_rows
            .iter()
            .any(|row| row.table_schema == "hidden_roundtrip")
    );
    assert!(
        !table_rows
            .iter()
            .any(|row| row.table_schema == "missing_roundtrip")
    );
    assert!(
        !column_rows
            .iter()
            .any(|row| row.table_schema == "hidden_roundtrip")
    );
    assert!(
        !column_rows
            .iter()
            .any(|row| row.table_schema == "missing_roundtrip")
    );
    assert!(
        !table_rows
            .iter()
            .any(|row| row.table_schema == "empty_roundtrip")
    );
    assert!(
        !column_rows
            .iter()
            .any(|row| row.table_schema == "empty_roundtrip")
    );
    let expected_schema = schema_rows
        .iter()
        .find(|row| row.schema_name == "information_schema")
        .ok_or("metadata API omitted the public information_schema schema")?
        .schema_name
        .as_bytes()
        .to_vec();
    let expected_table = table_rows
        .iter()
        .find(|row| row.table_schema == "information_schema")
        .ok_or("metadata API omitted the public information_schema tables")?
        .table_name
        .as_bytes()
        .to_vec();
    let expected_column = column_rows
        .iter()
        .find(|row| row.table_schema == "information_schema" && row.table_name == "columns")
        .ok_or("metadata API omitted information_schema.columns metadata")?
        .column_name
        .as_bytes()
        .to_vec();
    let expected_user_schema = schema_rows
        .iter()
        .find(|row| row.schema_name == "sql_roundtrip")
        .ok_or("metadata API omitted the admitted user schema")?
        .schema_name
        .as_bytes()
        .to_vec();
    let expected_empty_schema = schema_rows
        .iter()
        .find(|row| row.schema_name == "empty_roundtrip")
        .ok_or("metadata API omitted the authorized empty schema")?
        .schema_name
        .as_bytes()
        .to_vec();
    let expected_user_column = column_rows
        .iter()
        .find(|row| row.table_schema == "sql_roundtrip" && row.table_name == "records")
        .ok_or("metadata API omitted the admitted user column")?
        .column_name
        .as_bytes()
        .to_vec();

    let (mut service, mut gateway) = UnixStream::pair()?;
    gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    let worker_status_result = observe_executor_and_relay_results(
        &mut executor,
        &invocation,
        &mut service,
        accepted.request_id,
    );
    let worker_status = match worker_status_result {
        Ok(status) => status,
        Err(error) => {
            gateway.set_read_timeout(Some(Duration::from_millis(100)))?;
            let mut relayed_payloads = Vec::new();
            while let Ok(Some(frame)) = read_frame(&mut gateway) {
                relayed_payloads.push(frame.payload);
            }
            return Err(format!("{error:?}; relayed payloads: {relayed_payloads:?}").into());
        }
    };
    assert!(
        worker_status.success(),
        "sandbox query worker failed: {worker_status}"
    );
    staging.finish_started(owner, reservation.invocation_id)?;
    let inserted = read_frame(&mut gateway)?.ok_or("missing INSERT visibility result")?;
    assert_eq!(inserted.operation, Operation::GatewayResultData as u16);
    assert_eq!(inserted.request_id, accepted.request_id);
    assert_eq!(inserted.payload, b"insert-visible");
    let metadata = read_frame(&mut gateway)?.ok_or("missing SQL metadata result")?;
    assert_eq!(metadata.operation, Operation::GatewayResultData as u16);
    assert_eq!(metadata.request_id, accepted.request_id);
    assert_eq!(metadata.payload, expected_schema);
    let tables = read_frame(&mut gateway)?.ok_or("missing SQL tables metadata result")?;
    assert_eq!(tables.operation, Operation::GatewayResultData as u16);
    assert_eq!(tables.request_id, accepted.request_id);
    assert_eq!(tables.payload, expected_table);
    let columns = read_frame(&mut gateway)?.ok_or("missing SQL columns metadata result")?;
    assert_eq!(columns.operation, Operation::GatewayResultData as u16);
    assert_eq!(columns.request_id, accepted.request_id);
    assert_eq!(columns.payload, expected_column);
    let user_schema = read_frame(&mut gateway)?.ok_or("missing SQL user schema result")?;
    assert_eq!(user_schema.operation, Operation::GatewayResultData as u16);
    assert_eq!(user_schema.request_id, accepted.request_id);
    assert_eq!(user_schema.payload, expected_user_schema);
    let user_table = read_frame(&mut gateway)?.ok_or("missing SQL user table result")?;
    assert_eq!(user_table.operation, Operation::GatewayResultData as u16);
    assert_eq!(user_table.request_id, accepted.request_id);
    assert!(
        user_table.payload == b"records"
            || user_table.payload == b"linked_records"
            || user_table.payload == b"grouped_records"
    );
    let user_column = read_frame(&mut gateway)?.ok_or("missing SQL user column result")?;
    assert_eq!(user_column.operation, Operation::GatewayResultData as u16);
    assert_eq!(user_column.request_id, accepted.request_id);
    assert_eq!(user_column.payload, expected_user_column);
    let empty_schema_result = read_frame(&mut gateway)?.ok_or("missing empty schema SQL result")?;
    assert_eq!(
        empty_schema_result.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(empty_schema_result.request_id, accepted.request_id);
    assert_eq!(empty_schema_result.payload, expected_empty_schema);
    let updated = read_frame(&mut gateway)?.ok_or("missing UPDATE visibility result")?;
    assert_eq!(updated.operation, Operation::GatewayResultData as u16);
    assert_eq!(updated.request_id, accepted.request_id);
    assert_eq!(updated.payload, b"update-visible");
    let grouped_min = read_frame(&mut gateway)?.ok_or("missing grouped MIN SQLR result")?;
    assert_eq!(grouped_min.operation, Operation::GatewayResultData as u16);
    assert_eq!(grouped_min.request_id, accepted.request_id);
    assert_eq!(
        grouped_min.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 2, b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42, 0, 3,
                b'm', b'i', b'n', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42
            ][..]
        ]
        .concat()
    );
    let grouped_composite_first =
        read_frame(&mut gateway)?.ok_or("missing first composite GROUP BY SQLR result")?;
    assert_eq!(
        grouped_composite_first.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_second =
        read_frame(&mut gateway)?.ok_or("missing second composite GROUP BY SQLR result")?;
    assert_eq!(
        grouped_composite_second.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_order_first =
        read_frame(&mut gateway)?.ok_or("missing first composite ORDER BY SQLR result")?;
    assert_eq!(
        grouped_composite_order_first.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 2, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 9, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1,
            ][..],
        ]
        .concat()
    );
    let grouped_composite_order_second =
        read_frame(&mut gateway)?.ok_or("missing second composite ORDER BY SQLR result")?;
    assert_eq!(
        grouped_composite_order_second.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 3, 0, 8, b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 9, b'r', b'i', b'g', b'h', b't', b'_', b'k', b'e',
                b'y', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 7, 0, 5, b'c', b'o', b'u', b'n',
                b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2,
            ][..],
        ]
        .concat()
    );
    let grouped_sum = read_frame(&mut gateway)?.ok_or("missing grouped SUM SQLR result")?;
    assert_eq!(grouped_sum.operation, Operation::GatewayResultData as u16);
    assert_eq!(grouped_sum.request_id, accepted.request_id);
    assert_eq!(
        grouped_sum.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 2, b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42, 0, 3,
                b's', b'u', b'm', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 42
            ][..]
        ]
        .concat()
    );
    let grouped_avg = read_frame(&mut gateway)?.ok_or("missing grouped AVG SQLR result")?;
    assert_eq!(grouped_avg.operation, Operation::GatewayResultData as u16);
    assert_eq!(grouped_avg.request_id, accepted.request_id);
    assert_eq!(
        grouped_avg.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 2, b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42, 0, 3,
                b'a', b'v', b'g', 0, 0, 0, 5, 38, 6, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 2, 128, 222, 128
            ][..]
        ]
        .concat()
    );
    let grouped_order = read_frame(&mut gateway)?.ok_or("missing grouped ORDER BY SQLR result")?;
    assert_eq!(grouped_order.operation, Operation::GatewayResultData as u16);
    assert_eq!(grouped_order.request_id, accepted.request_id);
    assert_eq!(
        grouped_order.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 2, b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42, 0, 5,
                b'c', b'o', b'u', b'n', b't', 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
                1,
            ][..]
        ]
        .concat()
    );
    let joined = read_frame(&mut gateway)?.ok_or("missing joined SQLR result")?;
    assert_eq!(joined.operation, Operation::GatewayResultData as u16);
    assert_eq!(joined.request_id, accepted.request_id);
    assert_eq!(
        joined.payload,
        [
            b"SQLR".as_slice(),
            &[
                0, 1, 0, 2, 0, 2, b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42, 0, 2,
                b'i', b'd', 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 42,
            ][..]
        ]
        .concat()
    );
    for (expected_left, expected_right) in [(1u32, 7u32), (1u32, 7u32)] {
        let composite_joined =
            read_frame(&mut gateway)?.ok_or("missing composite joined SQLR result")?;
        assert_eq!(
            composite_joined.operation,
            Operation::GatewayResultData as u16
        );
        assert_eq!(composite_joined.request_id, accepted.request_id);
        let integer_field = |name: &[u8], value: u32| {
            [
                &(name.len() as u16).to_be_bytes()[..],
                name,
                &[0, 0, 0, 3],
                &[0, 0, 0],
                &[0, 0, 0, 4],
                &value.to_be_bytes(),
            ]
            .concat()
        };
        let expected_payload = [
            b"SQLR".as_slice(),
            &[0, 1, 0, 4],
            &integer_field(b"left_key", expected_left),
            &integer_field(b"right_key", expected_right),
            &integer_field(b"id", expected_left),
            &integer_field(b"match_key", expected_right),
        ]
        .concat();
        assert_eq!(composite_joined.payload, expected_payload);
    }
    drop(gateway);
    assert!(matches!(
        finalize_gateway_invocation_and_send_terminal(
            &mut invocation,
            &mut service,
            accepted.request_id,
            GatewayTerminalDecision::Commit,
        ),
        Err(prdb_engine::supervisor_ipc::GatewayTerminalRelayError::Frame(_))
    ));
    assert_eq!(
        storage
            .lookup_invocation_outcome(reservation.invocation_id, user_id, now + 1)?
            .ok_or("missing durable invocation outcome")?
            .status,
        OutcomeStatus::Committed
    );

    let restricted_scope = [
        b"PSC1\0\x01".as_slice(),
        &[1, 1],
        empty_schema.schema_id.as_slice(),
    ]
    .concat();
    let mut restricted_client = ClientConnectionSession::default();
    restricted_client.accept_hello(ClientMode::Query, "sandbox-sql-restricted-metadata")?;
    restricted_client.authenticate(&storage, restricted_token_id, &restricted_secret)?;
    let restricted_owner = restricted_client.staging_owner()?;
    let restricted_staging = StagingStore::new(
        StagingLimits::default(),
        Arc::new(AcceptFixture {
            requested_scope: restricted_scope.clone(),
        }),
    )?;
    let restricted_reservation = restricted_staging.prepare(
        restricted_owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: b"m".to_vec(),
            artifact_bytes: u64::try_from(restricted_artifact.len())?,
            artifact_sha256: Sha256::digest(&restricted_artifact).into(),
            scope_version: 1,
            requested_scope: restricted_scope,
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    restricted_staging.upload_chunk(
        restricted_owner,
        restricted_reservation.invocation_id,
        ChunkKind::Artifact,
        0,
        &restricted_artifact,
    )?;
    restricted_staging.seal(restricted_owner, restricted_reservation.invocation_id)?;
    let restricted_invocation_hex = restricted_reservation
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (restricted_accepted, mut restricted_invocation, mut restricted_executor) =
        restricted_client.handle_start_request_with_executor(
            &storage,
            &restricted_staging,
            ClientFrame {
                kind: 7,
                request_id: 2,
                response_sequence: 0,
                payload: format!("{{\"invocation_id\":\"{restricted_invocation_hex}\"}}")
                    .into_bytes(),
            },
            GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 8,
                max_bytes: 4096,
            },
            &runtime,
        )?;
    assert_eq!(restricted_accepted.payload, b"{\"state\":\"accepted\"}");

    let restricted_schemata = restricted_invocation
        .scan_information_schema_schemata(128)?
        .rows;
    assert!(
        restricted_schemata
            .iter()
            .any(|row| row.schema_name == "information_schema")
    );
    assert!(
        restricted_schemata
            .iter()
            .any(|row| row.schema_name == "empty_roundtrip")
    );
    assert!(
        !restricted_schemata
            .iter()
            .any(|row| row.schema_name == "sql_roundtrip")
    );
    let mut restricted_tables = Vec::new();
    let mut restricted_table_cursor = None;
    loop {
        let page =
            restricted_invocation.scan_information_schema_tables(128, restricted_table_cursor)?;
        restricted_tables.extend(page.rows);
        restricted_table_cursor = page.continuation;
        if restricted_table_cursor.is_none() {
            break;
        }
    }
    let mut restricted_columns = Vec::new();
    let mut restricted_column_cursor = None;
    loop {
        let page =
            restricted_invocation.scan_information_schema_columns(128, restricted_column_cursor)?;
        restricted_columns.extend(page.rows);
        restricted_column_cursor = page.continuation;
        if restricted_column_cursor.is_none() {
            break;
        }
    }
    assert!(
        !restricted_tables
            .iter()
            .any(|row| row.table_schema == "sql_roundtrip")
    );
    assert!(
        !restricted_columns
            .iter()
            .any(|row| row.table_schema == "sql_roundtrip")
    );

    let (mut restricted_service, mut restricted_gateway) = UnixStream::pair()?;
    restricted_gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    let restricted_decision = observe_executor_relay_and_finalize_gateway(
        &storage,
        &mut restricted_invocation,
        &mut restricted_executor,
        &mut restricted_service,
        restricted_accepted.request_id,
    )?;
    if restricted_decision != GatewayTerminalDecision::Commit {
        let mut relayed = Vec::new();
        while let Some(frame) = read_frame(&mut restricted_gateway)? {
            let terminal = frame.operation == Operation::GatewayTerminal as u16;
            relayed.push((
                frame.operation,
                String::from_utf8_lossy(&frame.payload).into_owned(),
            ));
            if terminal {
                break;
            }
        }
        panic!(
            "restricted sandbox query aborted: {restricted_decision:?}; relayed frames: {relayed:?}"
        );
    }
    restricted_staging.finish_started(restricted_owner, restricted_reservation.invocation_id)?;
    let restricted_schema_result =
        read_frame(&mut restricted_gateway)?.ok_or("missing restricted SQL metadata result")?;
    assert_eq!(
        restricted_schema_result.operation,
        Operation::GatewayResultData as u16
    );
    assert_eq!(
        restricted_schema_result.request_id,
        restricted_accepted.request_id
    );
    assert_eq!(restricted_schema_result.payload, b"empty_roundtrip");
    let restricted_terminal = read_frame(&mut restricted_gateway)?
        .ok_or("missing restricted committed terminal event")?;
    assert_eq!(
        restricted_terminal.operation,
        Operation::GatewayTerminal as u16
    );
    assert_eq!(
        restricted_terminal.request_id,
        restricted_accepted.request_id
    );
    assert!(String::from_utf8(restricted_terminal.payload)?.contains("\"outcome\":\"committed\""));
    assert_eq!(
        storage
            .lookup_invocation_outcome(
                restricted_reservation.invocation_id,
                restricted_user_id,
                now + 1,
            )?
            .ok_or("missing restricted durable invocation outcome")?
            .status,
        OutcomeStatus::Committed
    );
    Ok(())
}

#[test]
fn supervisor_cancellation_and_deadline_during_worker_wait_abort()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::{
        OwnerCancellationResult, SupervisorAdmissionRequest, SupervisorInvocationError,
    };
    use prdb_engine::transaction::TransactionError;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "worker_cancel_user"),
            object_write_with_name(user_id, 8, "worker_cancel_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let invocation = storage.admit_supervisor_invocation(
        id(0x73),
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;
    let mut worker = Command::new("sleep").arg("30").spawn()?;

    let (observed, cancellation) = std::thread::scope(|scope| {
        let cancel = scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(25));
            storage.cancel_owned_invocation(user_id, id(0x73), now + 1)
        });
        let observed = invocation.observe_worker_exit(&mut worker);
        let cancellation = cancel
            .join()
            .map_err(|_| std::io::Error::other("cancellation thread panicked"))??;
        Ok::<_, Box<dyn std::error::Error>>((observed, cancellation))
    })?;
    assert_eq!(cancellation, OwnerCancellationResult::Accepted);
    assert!(matches!(
        observed,
        Err(SupervisorInvocationError::Deadline(
            TransactionError::CancellationRequested
        ))
    ));
    assert!(worker.try_wait()?.is_some());
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let expired = storage.admit_supervisor_invocation_with_deadline(
        SupervisorAdmissionRequest {
            invocation_id: id(0x74),
            token_id,
            retention_deadline_us: now + 3_600_000_000,
            max_rows: 2,
            max_bytes: 64,
            deadline: Some(std::time::Instant::now() + Duration::from_millis(40)),
        },
        &secret,
    )?;
    let mut deadline_worker = Command::new("sleep").arg("30").spawn()?;
    assert!(matches!(
        expired.observe_worker_exit(&mut deadline_worker),
        Err(SupervisorInvocationError::Deadline(
            TransactionError::Expired
        ))
    ));
    assert!(deadline_worker.try_wait()?.is_some());
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x74), user_id, now + 2)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

#[test]
fn sealed_start_admits_only_current_credentials_and_enforces_request_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{
        ClientConnectionSession, ClientFrame, ClientMode, ClientStartError,
    };
    use prdb_engine::supervisor::{
        ChunkKind, PrepareSubmission, StagingError, StagingLimits, StagingStore,
        SubmissionMetadata, SubmissionMetadataValidator, WorkloadClass,
    };
    use prdb_engine::transaction::TransactionError;
    use sha2::{Digest, Sha256};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct AcceptPrepared;
    impl SubmissionMetadataValidator for AcceptPrepared {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            metadata.manifest == b"m" && metadata.requested_scope == requested_scope_fixture()
        }
    }

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    let credential = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    let credential_value = credential.payload.clone().unwrap();
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "start_user"),
            object_write_with_name(user_id, 8, "start_user"),
            credential,
        ],
        &[audit_event(0x72)],
    )?;
    let mut client = ClientConnectionSession::default();
    client.accept_hello(ClientMode::Query, "start-test")?;
    client.authenticate(&storage, token_id, &secret)?;
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptPrepared))?;
    let owner = client.staging_owner()?;

    let artifact = b"x";
    let first = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: b"m".to_vec(),
            artifact_bytes: artifact.len() as u64,
            artifact_sha256: Sha256::digest(artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope_fixture(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 10_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    staging.upload_chunk(owner, first.invocation_id, ChunkKind::Artifact, 0, artifact)?;
    staging.seal(owner, first.invocation_id)?;
    assert_eq!(
        storage.get_invocation_status(first.invocation_id, user_id, now)?,
        None
    );
    assert_eq!(
        lookup_outcome(&database, first.invocation_id, user_id, now)?,
        None
    );

    let invocation_hex = first
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (accepted, admitted) = client.handle_start_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 7,
            request_id: 1,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes(),
        },
        now + 3_600_000_000,
        2,
        64,
    )?;
    assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");
    assert_eq!(accepted.request_id, 1);
    let started_submission = staging.started_submission(owner, first.invocation_id)?;
    assert_eq!(started_submission.invocation_id(), first.invocation_id);
    assert_eq!(started_submission.artifact(), artifact);
    assert!(started_submission.parameters().is_empty());
    let cancelled = client.handle_cancel_prepared_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 9,
            request_id: 2,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes(),
        },
    )?;
    assert_eq!(cancelled.payload, b"{\"state\":\"accepted\"}");
    assert!(
        storage
            .get_invocation_status(first.invocation_id, user_id, now)?
            .is_some()
    );
    assert!(matches!(
        admitted.check_deadline(),
        Err(
            prdb_engine::storage_manager::SupervisorInvocationError::Deadline(
                TransactionError::CancellationRequested
            )
        )
    ));
    assert_eq!(
        lookup_outcome(&database, first.invocation_id, user_id, now)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    staging.finish_started(owner, first.invocation_id)?;
    drop(admitted);

    let second = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: b"m".to_vec(),
            artifact_bytes: artifact.len() as u64,
            artifact_sha256: Sha256::digest(artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope_fixture(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 10_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    staging.upload_chunk(
        owner,
        second.invocation_id,
        ChunkKind::Artifact,
        0,
        artifact,
    )?;
    staging.seal(owner, second.invocation_id)?;
    let second_invocation_hex = second
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let cgroup_parent =
        std::env::temp_dir().join(format!("prdb-start-cgroup-{}", std::process::id()));
    std::fs::create_dir_all(&cgroup_parent)?;
    let executor = prdb_engine::supervisor_ipc::GatewayExecutorConfig {
        cgroup_parent: cgroup_parent.clone(),
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 100_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 64 * 1024 * 1024,
            max_processes: 16,
        },
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: std::env::current_exe()?,
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: Vec::new(),
            runtime_read_only: Vec::new(),
            cleanup_timeout: Duration::from_millis(100),
        },
    };
    assert!(matches!(
        prdb_engine::supervisor_ipc::dispatch_gateway_client_request_with_executor(
            prdb_engine::ipc::ServiceRole::Gateway,
            &storage,
            &staging,
            &mut client,
            &prdb_engine::ipc::IpcFrame {
                operation: prdb_engine::ipc::Operation::StartSubmission as u16,
                request_id: 3,
                payload: format!("{{\"invocation_id\":\"{second_invocation_hex}\"}}").into_bytes(),
            },
            prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 2,
                max_bytes: 64,
            },
            &executor,
        ),
        Err(prdb_engine::supervisor_ipc::GatewayRequestError::Start(
            ClientStartError::ExecutorStart(prdb_engine::supervisor::StartError::Sandbox(_))
        ))
    ));
    assert_eq!(
        staging.status(owner, second.invocation_id)?,
        prdb_engine::supervisor::SubmissionState::Prepared
    );
    assert_eq!(
        storage.get_invocation_status(second.invocation_id, user_id, now)?,
        None
    );
    assert_eq!(
        lookup_outcome(&database, second.invocation_id, user_id, now)?,
        None
    );
    std::fs::remove_dir_all(cgroup_parent)?;

    let mut revoked = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        Some(now),
        2,
    );
    revoked.expected_value = Some(credential_value);
    storage.commit_catalog_and_audit(
        id(0x73),
        id(9),
        now + 3_600_000_000,
        &[revoked],
        &[audit_event(0x73)],
    )?;
    assert!(matches!(
        client.start_submission(
            &storage,
            &staging,
            second.invocation_id,
            now + 3_600_000_000,
            2,
            64,
        ),
        Err(ClientStartError::Start(
            prdb_engine::supervisor::StartError::Admission(TransactionError::AuthenticationFailed)
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, second.invocation_id, user_id, now)?,
        None
    );
    assert_eq!(
        storage.get_invocation_status(second.invocation_id, user_id, now)?,
        None
    );
    assert_eq!(
        staging.status(owner, second.invocation_id),
        Err(StagingError::Unavailable)
    );
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
        max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
        max_token_lifecycle_requests_per_minute: 5,
        max_admin_cancellation_requests_per_minute: 30,
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
fn oversized_index_key_is_rejected_before_publication() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = key(7);
    let mut index_key = vec![0x44; 16];
    index_key.resize(prdb_engine::index_key::MAX_INDEX_KEY_BYTES + 1 - 16, 0x01);
    index_key.extend_from_slice(&row_key[16..]);

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(6),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(b"row".to_vec()),
            }],
            &[IndexWrite {
                key: index_key,
                row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }],
        ),
        Err(CommitError::InvalidIndex)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert!(row(&database, &row_key)?.is_none());

    let mut boundary_key = vec![0x44; 16];
    boundary_key.resize(prdb_engine::index_key::MAX_INDEX_KEY_BYTES - 16, 0x01);
    boundary_key.extend_from_slice(&row_key[16..]);
    assert_eq!(
        boundary_key.len(),
        prdb_engine::index_key::MAX_INDEX_KEY_BYTES
    );
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(7),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(b"row".to_vec()),
            }],
            &[IndexWrite {
                key: boundary_key,
                row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }],
        )?,
        1
    );
    assert!(row(&database, &row_key)?.is_some());
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
