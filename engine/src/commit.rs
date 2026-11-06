//! Serialized row publication with optimistic row-version validation.
use crate::audit::AuditEvent;
use crate::authorization::{
    MAX_MEMBERSHIP_EDGES, Membership, resolve_role_closure, validate_membership_graph,
};
use crate::catalog::{
    decode_builtin_scalar_type, decode_registered_envelope, validate_builtin_type_profile,
};
use crate::ddl_fence::DdlFenceCoordinator;
use crate::expression::decode_catalog_expression_record;
use crate::index_locator::IndexLocator;
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::outcome::{InvocationOutcome, OutcomeStatus};
use crate::row_record::RowRecord;
use crate::schema::{AUDIT, CATALOG, ENGINE_META, INDEXES, OUTCOMES, ROW_INDEXES, ROWS};
use crate::tuple::{ColumnSchema, decode_tuple, encode_tuple};
use redb::{Database, ReadOnlyTable, ReadableDatabase, ReadableTable, WriteTransaction};
use std::collections::{HashMap, HashSet};

type IndexTableMap = HashMap<[u8; 16], [u8; 16]>;
const MAX_FOREIGN_KEY_ROW_VISITS_PER_COMMIT: usize = 65_536;
const MAX_FOREIGN_KEY_COMPARISON_KEY_BYTES_PER_COMMIT: usize = 16 * 1024 * 1024;
const MAX_INDEX_DDL_ROWS_PER_COMMIT: usize = 65_536;
const MAX_INDEX_DDL_ENTRIES_PER_COMMIT: usize = 65_536;
const MAX_INDEX_DDL_BYTES_PER_COMMIT: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowWrite {
    pub key: [u8; 32],
    /// `None` means the row must not exist; otherwise it is the snapshot row version.
    pub expected_version: Option<u64>,
    /// `None` deletes the row; `Some` replaces its schema-aware payload.
    pub payload: Option<Vec<u8>>,
}

/// One exact logical index-key mutation associated with a row write. For a key
/// change, remove the old key and add the new key in the same commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexWrite {
    pub key: Vec<u8>,
    pub row_key: [u8; 32],
    /// Expected locator version at this key; `None` requires the key to be absent.
    pub expected_version: Option<u64>,
    /// Keep this key and publish a locator at the new commit sequence.
    pub present: bool,
    /// Enforce uniqueness for this indexed SQL tuple when adding membership.
    pub unique: bool,
    /// SQL NULLS DISTINCT: skip tuple collision checks when any indexed value is NULL.
    pub null_exempt: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogWrite {
    pub key: Vec<u8>,
    /// Exact CAT1 bytes observed in the pinned snapshot; `None` requires absence.
    pub expected_value: Option<Vec<u8>>,
    /// `None` deletes the record; `Some` publishes a validated CAT1 record.
    pub payload: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogGuard {
    pub key: Vec<u8>,
    /// Exact CAT1 bytes read by the invocation for schema/authority validation.
    pub expected_value: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct RetainedIndexMutation {
    index_key: Vec<u8>,
    row_index_key: Vec<u8>,
    locator: Option<[u8; crate::index_locator::LOCATOR_LENGTH]>,
}

/// A catalog-validated FK relationship and the typed column projections
/// needed to check its final state inside the serialized redb commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForeignKeyCheck {
    pub source_table_id: [u8; 16],
    pub source_schema_version: u64,
    pub source_schema: Vec<ColumnSchema>,
    pub source_ordinals: Vec<usize>,
    pub target_table_id: [u8; 16],
    pub target_schema_version: u64,
    pub target_schema: Vec<ColumnSchema>,
    pub target_ordinals: Vec<usize>,
}

#[derive(Clone, Debug)]
struct FinalTableSchema {
    schema_version: u64,
    column_ids: Vec<[u8; 16]>,
    columns: Vec<ColumnSchema>,
}

#[derive(Clone, Debug)]
struct IndexDdlPlan {
    index_tables: HashMap<[u8; 16], [u8; 16]>,
    created_indexes: HashSet<[u8; 16]>,
    dropped_indexes: HashSet<[u8; 16]>,
    rebuilt_indexes: HashSet<[u8; 16]>,
    expected_rows: HashMap<[u8; 16], Vec<([u8; 32], u64)>>,
    row_versions: HashMap<[u8; 32], u64>,
    writes: Vec<IndexWrite>,
}

#[derive(Clone, Debug)]
struct IndexDdlDefinition {
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: Vec<[u8; 16]>,
    unique: bool,
}

pub struct CommitRequest<'a> {
    pub invocation_id: [u8; 16],
    pub principal_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub rows: &'a [RowWrite],
    pub indexes: &'a [IndexWrite],
    pub foreign_keys: &'a [ForeignKeyCheck],
    pub catalog_guards: &'a [CatalogGuard],
    pub catalog: &'a [CatalogWrite],
    pub audit: &'a [AuditEvent],
}

#[derive(Clone, Copy)]
struct CatalogCommitRequest<'a> {
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &'a [CatalogWrite],
    audit_events: &'a [AuditEvent],
}

#[derive(Clone, Copy)]
enum CatalogAuthorizationMode {
    Trusted,
    IndexDdl,
    MembershipDdl,
    SystemPrivilegeDdl,
    ObjectGrantDdl,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitError {
    DuplicateKey,
    OutcomeAlreadyExists,
    NotReady,
    UnsupportedFormat,
    Conflict,
    InvalidWrite,
    InvalidMetadata,
    InvalidRow,
    InvalidIndex,
    IndexOverlayLimitExceeded,
    UniqueViolation,
    ForeignKeyViolation,
    ForeignKeyValidationLimitExceeded,
    IndexDdlLimitExceeded,
    DdlFenceUnavailable,
    DdlAuthorizationDenied,
    InvocationExpired,
    InvalidCatalog,
    InvalidAudit,
    AuditAlreadyExists,
    SequenceExhausted,
    Storage(String),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "storage commit failed: {self:?}")
    }
}

impl std::error::Error for CommitError {}

fn storage_error(error: impl std::fmt::Display) -> CommitError {
    CommitError::Storage(error.to_string())
}

fn decode_catalog_record(
    key: &[u8],
    bytes: &[u8],
) -> Result<crate::catalog::CatalogRecord, CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.logical_key != key
        || (record.family_code == 7 && decode_catalog_expression_record(&record).is_err())
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(record)
}

fn decode_membership_record(key: &[u8], bytes: &[u8]) -> Result<Membership, CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.family_code != 11 || record.logical_key != key || key.len() != 36 {
        return Err(CommitError::InvalidCatalog);
    }
    let member_id = key[3..19]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let role_id: [u8; 16] = key[20..36]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    Ok(Membership { member_id, role_id })
}

fn decode_principal_kind(key: &[u8], bytes: &[u8]) -> Result<u16, CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.family_code != 10 || record.logical_key != key || key.len() != 19 {
        return Err(CommitError::InvalidCatalog);
    }
    let kind_field = record
        .fields
        .iter()
        .find(|field| field.tag == 1)
        .ok_or(CommitError::InvalidCatalog)?;
    let kind = u16::from_le_bytes(
        kind_field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if !matches!(kind, 1 | 2) {
        return Err(CommitError::InvalidCatalog);
    }
    let login_field = record
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .ok_or(CommitError::InvalidCatalog)?;
    if (kind == 2 && login_field.payload != [0])
        || record
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .is_none_or(|field| {
                field.payload.len() != 8
                    || u64::from_le_bytes(field.payload[..8].try_into().unwrap()) == 0
            })
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(kind)
}

fn final_principal_kind(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
    principal_id: [u8; 16],
) -> Result<u16, CommitError> {
    let mut key = 10u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&principal_id);
    if let Some(write) = catalog_writes.iter().find(|write| write.key == key) {
        return decode_principal_kind(
            &key,
            write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?,
        );
    }
    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let bytes = table
        .get(&key[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidCatalog)?
        .value()
        .to_vec();
    decode_principal_kind(&key, &bytes)
}

fn validate_foreign_key_constraints(
    transaction: &WriteTransaction,
    writes: &[RowWrite],
    foreign_keys: &[ForeignKeyCheck],
    force_validation: bool,
) -> Result<(), CommitError> {
    let mut row_visits = 0usize;
    let write_keys = writes.iter().map(|write| write.key).collect::<HashSet<_>>();
    for foreign_key in foreign_keys {
        let source_touched = writes
            .iter()
            .any(|write| write.key[..16] == foreign_key.source_table_id);
        let target_touched = writes
            .iter()
            .any(|write| write.key[..16] == foreign_key.target_table_id);
        if !force_validation && !source_touched && !target_touched {
            continue;
        }
        validate_foreign_key_definition(foreign_key)?;
        let mut comparison_key_bytes = 0usize;
        let target_keys = collect_final_foreign_key_keys(
            transaction,
            writes,
            foreign_key.target_table_id,
            foreign_key.target_schema_version,
            &foreign_key.target_schema,
            &foreign_key.target_ordinals,
            &foreign_key.target_ordinals,
            &foreign_key.target_schema,
            &mut row_visits,
            &mut comparison_key_bytes,
            &write_keys,
        )?;
        let source_keys = collect_final_foreign_key_keys(
            transaction,
            writes,
            foreign_key.source_table_id,
            foreign_key.source_schema_version,
            &foreign_key.source_schema,
            &foreign_key.source_ordinals,
            &foreign_key.target_ordinals,
            &foreign_key.target_schema,
            &mut row_visits,
            &mut comparison_key_bytes,
            &write_keys,
        )?;
        if source_keys.iter().any(|key| !target_keys.contains(key)) {
            return Err(CommitError::ForeignKeyViolation);
        }
    }
    Ok(())
}

fn load_final_table_schema<T>(
    catalog: &T,
    writes: &[CatalogWrite],
    table_id: [u8; 16],
) -> Result<FinalTableSchema, CommitError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let table_key = id_catalog_key(5, table_id);
    let table_bytes =
        final_catalog_value(catalog, writes, &table_key)?.ok_or(CommitError::InvalidCatalog)?;
    let schema_version = u64::from_le_bytes(
        catalog_field_bytes(&table_key, &table_bytes, 5, 2)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if schema_version == 0 {
        return Err(CommitError::InvalidCatalog);
    }

    let prefix = 6u16.to_be_bytes();
    let mut count = 0usize;
    let mut columns = Vec::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if writes.iter().any(|write| write.key == key) {
            continue;
        }
        count += 1;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if column_table_id(key, value.value())? == table_id {
            columns.push(decode_final_column(catalog, writes, key, value.value())?);
        }
    }
    for write in writes.iter().filter(|write| write.key.starts_with(&prefix)) {
        let Some(bytes) = &write.payload else {
            continue;
        };
        count += 1;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if column_table_id(&write.key, bytes)? == table_id {
            columns.push(decode_final_column(catalog, writes, &write.key, bytes)?);
        }
    }
    if columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(CommitError::InvalidCatalog);
    }
    columns.sort_by_key(|(ordinal, _, _)| *ordinal);
    let mut column_ids = Vec::with_capacity(columns.len());
    let mut schemas = Vec::with_capacity(columns.len());
    for (expected_ordinal, (ordinal, column_id, schema)) in columns.into_iter().enumerate() {
        if ordinal != expected_ordinal {
            return Err(CommitError::InvalidCatalog);
        }
        column_ids.push(column_id);
        schemas.push(schema);
    }
    Ok(FinalTableSchema {
        schema_version,
        column_ids,
        columns: schemas,
    })
}

fn decode_final_column<T>(
    catalog: &T,
    writes: &[CatalogWrite],
    key: &[u8],
    bytes: &[u8],
) -> Result<(usize, [u8; 16], ColumnSchema), CommitError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let column_id: [u8; 16] = key
        .get(3..19)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    if column_id == [0; 16] {
        return Err(CommitError::InvalidCatalog);
    }
    let ordinal = u32::from_le_bytes(
        catalog_field_bytes(key, bytes, 6, 2)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ) as usize;
    let type_id: [u8; 16] = catalog_field_bytes(key, bytes, 6, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let parameters = catalog_field_bytes(key, bytes, 6, 5)?;
    let nullable = match catalog_field_bytes(key, bytes, 6, 6)?.as_slice() {
        [0] => false,
        [1] => true,
        _ => return Err(CommitError::InvalidCatalog),
    };
    let introduced_schema_version = u64::from_le_bytes(
        catalog_field_bytes(key, bytes, 6, 9)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    let table_id = column_table_id(key, bytes)?;
    let table_key = id_catalog_key(5, table_id);
    let table_bytes =
        final_catalog_value(catalog, writes, &table_key)?.ok_or(CommitError::InvalidCatalog)?;
    let schema_version = u64::from_le_bytes(
        catalog_field_bytes(&table_key, &table_bytes, 5, 2)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if introduced_schema_version == 0 || introduced_schema_version > schema_version {
        return Err(CommitError::InvalidCatalog);
    }
    let type_key = id_catalog_key(4, type_id);
    let type_bytes =
        final_catalog_value(catalog, writes, &type_key)?.ok_or(CommitError::InvalidCatalog)?;
    let type_record =
        decode_registered_envelope(&type_bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if type_record.family_code != 4 || type_record.logical_key != type_key {
        return Err(CommitError::InvalidCatalog);
    }
    let scalar_type = decode_builtin_scalar_type(&type_record, &parameters)
        .map_err(|_| CommitError::InvalidCatalog)?;
    Ok((
        ordinal,
        column_id,
        ColumnSchema {
            scalar_type,
            nullable,
        },
    ))
}

fn load_new_foreign_key_checks(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    writes: &[CatalogWrite],
) -> Result<Vec<ForeignKeyCheck>, CommitError> {
    let prefix = 8u16.to_be_bytes();
    if !writes.iter().any(|write| write.key.starts_with(&prefix)) {
        return Ok(Vec::new());
    }
    let mut foreign_key_count = 0usize;
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if writes.iter().any(|write| write.key == key) {
            continue;
        }
        if u16::from_le_bytes(
            catalog_field_bytes(key, value.value(), 8, 2)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ) == 5
        {
            foreign_key_count += 1;
            if foreign_key_count > 256 {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    let mut checks = Vec::new();
    for write in writes.iter().filter(|write| write.key.starts_with(&prefix)) {
        let Some(bytes) = write.payload.as_deref() else {
            continue;
        };
        if u16::from_le_bytes(
            catalog_field_bytes(&write.key, bytes, 8, 2)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ) != 5
        {
            continue;
        }
        foreign_key_count += 1;
        if foreign_key_count > 256 {
            return Err(CommitError::InvalidCatalog);
        }
        if write.expected_value == write.payload {
            continue;
        }
        let source_table_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 1)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let source_column_ids = catalog_id_list_field(&write.key, bytes, 8, 3)?;
        let target_constraint_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 5)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let target_column_ids = catalog_id_list_field(&write.key, bytes, 8, 6)?;
        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = final_catalog_value(catalog, writes, &target_constraint_key)?
            .ok_or(CommitError::InvalidCatalog)?;
        let target_table_id: [u8; 16] =
            catalog_field_bytes(&target_constraint_key, &target_constraint_bytes, 8, 1)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
        if source_column_ids.is_empty() || source_column_ids.len() != target_column_ids.len() {
            return Err(CommitError::InvalidCatalog);
        }
        let source = load_final_table_schema(catalog, writes, source_table_id)?;
        let target = load_final_table_schema(catalog, writes, target_table_id)?;
        let source_ordinals = source_column_ids
            .iter()
            .map(|column_id| {
                source
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                    .ok_or(CommitError::InvalidCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let target_ordinals = target_column_ids
            .iter()
            .map(|column_id| {
                target
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                    .ok_or(CommitError::InvalidCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if source_ordinals
            .iter()
            .zip(&target_ordinals)
            .any(|(source_ordinal, target_ordinal)| {
                source.columns[*source_ordinal].scalar_type
                    != target.columns[*target_ordinal].scalar_type
            })
        {
            return Err(CommitError::InvalidCatalog);
        }
        checks.push(ForeignKeyCheck {
            source_table_id,
            source_schema_version: source.schema_version,
            source_schema: source.columns,
            source_ordinals,
            target_table_id,
            target_schema_version: target.schema_version,
            target_schema: target.columns,
            target_ordinals,
        });
    }
    Ok(checks)
}

fn validate_foreign_key_definition(foreign_key: &ForeignKeyCheck) -> Result<(), CommitError> {
    if foreign_key.source_table_id == [0; 16]
        || foreign_key.target_table_id == [0; 16]
        || foreign_key.source_schema_version == 0
        || foreign_key.target_schema_version == 0
        || foreign_key.source_ordinals.is_empty()
        || foreign_key.source_ordinals.len() != foreign_key.target_ordinals.len()
        || foreign_key
            .source_ordinals
            .iter()
            .any(|ordinal| *ordinal >= foreign_key.source_schema.len())
        || foreign_key
            .target_ordinals
            .iter()
            .any(|ordinal| *ordinal >= foreign_key.target_schema.len())
        || foreign_key
            .source_ordinals
            .iter()
            .zip(&foreign_key.target_ordinals)
            .any(|(source, target)| {
                foreign_key.source_schema[*source].scalar_type
                    != foreign_key.target_schema[*target].scalar_type
            })
        || foreign_key
            .source_ordinals
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != foreign_key.source_ordinals.len()
        || foreign_key
            .target_ordinals
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != foreign_key.target_ordinals.len()
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_final_foreign_key_keys(
    transaction: &WriteTransaction,
    writes: &[RowWrite],
    table_id: [u8; 16],
    schema_version: u64,
    schema: &[ColumnSchema],
    ordinals: &[usize],
    comparison_ordinals: &[usize],
    comparison_schema: &[ColumnSchema],
    row_visits: &mut usize,
    comparison_key_bytes: &mut usize,
    write_keys: &HashSet<[u8; 32]>,
) -> Result<HashSet<Vec<u8>>, CommitError> {
    let comparison_columns = comparison_ordinals
        .iter()
        .map(|ordinal| {
            comparison_schema
                .get(*ordinal)
                .map(|column| ColumnSchema {
                    scalar_type: column.scalar_type,
                    nullable: true,
                })
                .ok_or(CommitError::InvalidCatalog)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut keys = HashSet::new();
    let mut start = [0; 32];
    start[..16].copy_from_slice(&table_id);
    let mut end = [u8::MAX; 32];
    end[..16].copy_from_slice(&table_id);
    let table = transaction.open_table(ROWS).map_err(storage_error)?;
    for entry in table
        .range(start.as_slice()..=end.as_slice())
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let row_key: [u8; 32] = key
            .value()
            .try_into()
            .map_err(|_| CommitError::InvalidRow)?;
        *row_visits = row_visits
            .checked_add(1)
            .ok_or(CommitError::ForeignKeyValidationLimitExceeded)?;
        if *row_visits > MAX_FOREIGN_KEY_ROW_VISITS_PER_COMMIT {
            return Err(CommitError::ForeignKeyValidationLimitExceeded);
        }
        if write_keys.contains(&row_key) {
            continue;
        }
        let row = RowRecord::decode(value.value()).map_err(|_| CommitError::InvalidRow)?;
        let tuple = decode_tuple(schema, schema_version, &row.payload)
            .map_err(|_| CommitError::InvalidRow)?;
        if let Some(key) = foreign_key_tuple_key(&tuple.values, ordinals, &comparison_columns)? {
            remember_foreign_key_key(&mut keys, key, comparison_key_bytes)?;
        }
    }
    drop(table);

    for write in writes.iter().filter(|write| write.key[..16] == table_id) {
        *row_visits = row_visits
            .checked_add(1)
            .ok_or(CommitError::ForeignKeyValidationLimitExceeded)?;
        if *row_visits > MAX_FOREIGN_KEY_ROW_VISITS_PER_COMMIT {
            return Err(CommitError::ForeignKeyValidationLimitExceeded);
        }
        let Some(payload) = write.payload.as_deref() else {
            continue;
        };
        let tuple =
            decode_tuple(schema, schema_version, payload).map_err(|_| CommitError::InvalidRow)?;
        if let Some(key) = foreign_key_tuple_key(&tuple.values, ordinals, &comparison_columns)? {
            remember_foreign_key_key(&mut keys, key, comparison_key_bytes)?;
        }
    }
    Ok(keys)
}

fn remember_foreign_key_key(
    keys: &mut HashSet<Vec<u8>>,
    key: Vec<u8>,
    comparison_key_bytes: &mut usize,
) -> Result<(), CommitError> {
    if keys.contains(&key) {
        return Ok(());
    }
    let next_bytes = comparison_key_bytes
        .checked_add(key.len())
        .filter(|bytes| *bytes <= MAX_FOREIGN_KEY_COMPARISON_KEY_BYTES_PER_COMMIT)
        .ok_or(CommitError::ForeignKeyValidationLimitExceeded)?;
    keys.insert(key);
    *comparison_key_bytes = next_bytes;
    Ok(())
}

fn foreign_key_tuple_key(
    values: &[Option<crate::tuple::ScalarValue>],
    ordinals: &[usize],
    comparison_columns: &[ColumnSchema],
) -> Result<Option<Vec<u8>>, CommitError> {
    let projected = ordinals
        .iter()
        .map(|ordinal| {
            values
                .get(*ordinal)
                .cloned()
                .ok_or(CommitError::InvalidCatalog)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if projected.iter().any(Option::is_none) {
        return Ok(None);
    }
    encode_tuple(comparison_columns, 1, &projected)
        .map(Some)
        .map_err(|_| CommitError::ForeignKeyViolation)
}

fn validate_final_membership_graph(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = {
        let table = transaction.open_table(CATALOG).map_err(storage_error)?;
        let mut memberships = HashMap::<Vec<u8>, Membership>::new();
        for entry in table
            .range(membership_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&membership_prefix) {
                break;
            }
            let edge = decode_membership_record(&key, value.value())?;
            memberships.insert(key, edge);
            if memberships.len() > MAX_MEMBERSHIP_EDGES {
                return Err(CommitError::InvalidCatalog);
            }
        }
        memberships
    };
    // Remove old edges first so the bound applies to the final graph, not to
    // an arbitrary ordering of additions and removals in the submitted batch.
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&membership_prefix) && write.payload.is_none())
    {
        memberships.remove(&write.key);
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&membership_prefix) && write.payload.is_some())
    {
        if let Some(bytes) = &write.payload {
            memberships.insert(
                write.key.clone(),
                decode_membership_record(&write.key, bytes)?,
            );
            if memberships.len() > MAX_MEMBERSHIP_EDGES {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    let edges = memberships.values().copied().collect::<Vec<_>>();
    let mut principal_kinds = HashMap::<[u8; 16], u16>::new();
    for edge in &edges {
        for principal_id in [edge.member_id, edge.role_id] {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                principal_kinds.entry(principal_id)
            {
                entry.insert(final_principal_kind(
                    transaction,
                    catalog_writes,
                    principal_id,
                )?);
            }
        }
        if principal_kinds.get(&edge.role_id) != Some(&2) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    validate_membership_graph(&edges).map_err(|_| CommitError::InvalidCatalog)?;

    // Membership changes invalidate the member's effective role closure. For
    // existing principals, require the same DDL transaction to advance the
    // principal authority epoch so readers also detect newly added edges.
    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    // Epochs must never be restored to an earlier byte value after a change:
    // snapshot guards rely on them to detect newly added membership/grants.
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&10u16.to_be_bytes()))
    {
        let Some(next) = &write.payload else {
            continue;
        };
        decode_principal_kind(&write.key, next)?;
        let next_epoch = principal_authority_epoch(&write.key, next)?;
        let current = table
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if let Some(current) = current
            && current != *next
            && next_epoch <= principal_authority_epoch(&write.key, &current)?
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    let validate_principal_epoch_bumps = |changed_principals: &HashSet<[u8; 16]>| {
        for principal_id in changed_principals {
            let mut principal_key = 10u16.to_be_bytes().to_vec();
            principal_key.push(1);
            principal_key.extend_from_slice(principal_id);
            let current_epoch = table
                .get(&principal_key[..])
                .map_err(storage_error)?
                .map(|value| principal_authority_epoch(&principal_key, value.value()))
                .transpose()?;
            let final_bytes = if let Some(write) = catalog_writes
                .iter()
                .find(|write| write.key == principal_key)
            {
                write.payload.clone()
            } else {
                table
                    .get(&principal_key[..])
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
            };
            if let (Some(current_epoch), Some(final_bytes)) = (current_epoch, final_bytes)
                && principal_authority_epoch(&principal_key, &final_bytes)? <= current_epoch
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        Ok::<(), CommitError>(())
    };
    let mut changed_members = HashSet::<[u8; 16]>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&membership_prefix))
    {
        let current = table
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if write.key.len() != 36 {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(bytes) = &write.payload {
            let next_epoch = membership_epoch(&write.key, bytes)?;
            if let Some(current_bytes) = &current
                && next_epoch <= membership_epoch(&write.key, current_bytes)?
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        changed_members.insert(
            write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
    }
    validate_principal_epoch_bumps(&changed_members)?;

    // A grant addition or revocation changes the effective permissions for
    // its grantee, so existing grantees must advance their authority epoch.
    let grant_prefix = 12u16.to_be_bytes();
    let mut changed_grantees = HashSet::<[u8; 16]>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&grant_prefix))
    {
        let current = table
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if write.key.len() != 59 {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(bytes) = &write.payload {
            let next_epoch = grant_authority_epoch(&write.key, bytes)?;
            if let Some(current_bytes) = &current
                && next_epoch <= grant_authority_epoch(&write.key, current_bytes)?
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        changed_grantees.insert(
            write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
    }
    validate_principal_epoch_bumps(&changed_grantees)?;

    let mut changed_policies = HashSet::<[u8; 16]>::new();
    let mut affected_tables = HashSet::<[u8; 16]>::new();
    for write in catalog_writes.iter().filter(|write| {
        write.key.starts_with(&8u16.to_be_bytes())
            || write.key.starts_with(&13u16.to_be_bytes())
            || write.key.starts_with(&14u16.to_be_bytes())
    }) {
        let current = table
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if write.key.starts_with(&8u16.to_be_bytes()) {
            if write.key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(bytes) = &write.payload {
                validate_constraint_shape(&write.key, bytes)?;
            }
            let current_table = current
                .as_deref()
                .map(|bytes| constraint_table_id(&write.key, bytes))
                .transpose()?;
            let final_table = write
                .payload
                .as_deref()
                .map(|bytes| constraint_table_id(&write.key, bytes))
                .transpose()?;
            if current_table.is_some() && final_table.is_some() && current_table != final_table {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(table_id) = current_table.or(final_table) {
                affected_tables.insert(table_id);
                for constraint_bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    if u16::from_le_bytes(
                        catalog_field_bytes(&write.key, constraint_bytes, 8, 2)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    ) != 5
                    {
                        continue;
                    }
                    let target_constraint_id: [u8; 16] =
                        catalog_field_bytes(&write.key, constraint_bytes, 8, 5)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                    let target_key = id_catalog_key(8, target_constraint_id);
                    let target_bytes = final_catalog_value(&table, catalog_writes, &target_key)?
                        .or(table
                            .get(&target_key[..])
                            .map_err(storage_error)?
                            .map(|value| value.value().to_vec()))
                        .ok_or(CommitError::InvalidCatalog)?;
                    affected_tables.insert(constraint_table_id(&target_key, &target_bytes)?);
                }
            }
        } else if write.key.starts_with(&13u16.to_be_bytes()) {
            let policy_id: [u8; 16] = write
                .key
                .get(3..19)
                .ok_or(CommitError::InvalidCatalog)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            changed_policies.insert(policy_id);
            if let Some(bytes) = &write.payload {
                affected_tables.insert(policy_table_id(&write.key, bytes)?);
            }
            if let Some(bytes) = &current {
                affected_tables.insert(policy_table_id(&write.key, bytes)?);
            }
        } else {
            if write.key.len() != 39 {
                return Err(CommitError::InvalidCatalog);
            }
            let policy_id: [u8; 16] = write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            changed_policies.insert(policy_id);
            let policy_key = catalog_id_key(13, policy_id);
            let policy_bytes = if let Some(candidate) = catalog_writes
                .iter()
                .find(|candidate| candidate.key == policy_key)
            {
                if let Some(bytes) = &candidate.payload {
                    bytes.clone()
                } else {
                    table
                        .get(&policy_key[..])
                        .map_err(storage_error)?
                        .ok_or(CommitError::InvalidCatalog)?
                        .value()
                        .to_vec()
                }
            } else {
                table
                    .get(&policy_key[..])
                    .map_err(storage_error)?
                    .ok_or(CommitError::InvalidCatalog)?
                    .value()
                    .to_vec()
            };
            affected_tables.insert(policy_table_id(&policy_key, &policy_bytes)?);
        }
    }
    for policy_id in &changed_policies {
        let policy_key = catalog_id_key(13, *policy_id);
        let current = table
            .get(&policy_key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        let final_bytes =
            if let Some(write) = catalog_writes.iter().find(|write| write.key == policy_key) {
                write.payload.clone()
            } else {
                current.clone()
            };
        let final_policy_exists = final_bytes.is_some();
        if let (Some(current), Some(final_bytes)) = (current, final_bytes)
            && policy_authority_epoch(&policy_key, &final_bytes)?
                <= policy_authority_epoch(&policy_key, &current)?
        {
            return Err(CommitError::InvalidCatalog);
        }
        let mut target_prefix = 14u16.to_be_bytes().to_vec();
        target_prefix.push(1);
        target_prefix.extend_from_slice(policy_id);
        let mut final_targets = HashSet::new();
        for entry in table
            .range(target_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, _) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&target_prefix) {
                break;
            }
            final_targets.insert(key);
            if final_targets.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(CommitError::InvalidCatalog);
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&target_prefix) && write.payload.is_none())
        {
            final_targets.remove(&write.key);
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&target_prefix) && write.payload.is_some())
        {
            final_targets.insert(write.key.clone());
            if final_targets.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(CommitError::InvalidCatalog);
            }
        }
        if !final_policy_exists && !final_targets.is_empty() {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for table_id in affected_tables {
        let table_key = catalog_id_key(5, table_id);
        let current = table
            .get(&table_key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        let final_bytes =
            if let Some(write) = catalog_writes.iter().find(|write| write.key == table_key) {
                write.payload.clone()
            } else {
                current.clone()
            };
        if let (Some(current), Some(final_bytes)) = (current, final_bytes)
            && table_write_epoch(&table_key, &final_bytes)?
                <= table_write_epoch(&table_key, &current)?
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    validate_column_schema_epochs(&table, catalog_writes)?;
    validate_final_constraints(&table, catalog_writes)?;
    Ok(())
}

#[derive(Default)]
struct DependencyCatalogGraph {
    database_id: Option<[u8; 16]>,
    object_parents: HashMap<[u8; 16], Option<[u8; 16]>>,
    object_kinds: HashMap<[u8; 16], u16>,
    typed_object_kinds: HashMap<[u8; 16], u16>,
    column_tables: HashMap<[u8; 16], [u8; 16]>,
    index_tables: HashMap<[u8; 16], [u8; 16]>,
    index_columns: HashMap<[u8; 16], Vec<[u8; 16]>>,
    policy_tables: HashMap<[u8; 16], [u8; 16]>,
    policy_kinds: HashMap<[u8; 16], u16>,
    constraint_kinds: HashMap<[u8; 16], u16>,
    foreign_key_targets: HashMap<[u8; 16], [u8; 16]>,
    backing_index_targets: HashMap<[u8; 16], [u8; 16]>,
    index_owners: HashMap<[u8; 16], [u8; 16]>,
    expression_reference_edges: HashSet<([u8; 16], [u8; 16])>,
    authority_edges: HashSet<([u8; 16], [u8; 16])>,
    authority_principals: HashSet<[u8; 16]>,
    dependencies: Vec<([u8; 16], [u8; 16], u16)>,
}

/// Keep dependency references valid over the final catalog produced by this
/// transaction, including dependencies or endpoint objects created together.
fn validate_final_dependency_edges(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
    database_uuid: [u8; 16],
) -> Result<(), CommitError> {
    if !catalog_writes.iter().any(|write| {
        write.key.len() >= 2
            && matches!(
                u16::from_be_bytes([write.key[0], write.key[1]]),
                1 | 2 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15
            )
    }) {
        return Ok(());
    }

    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let mut graph = DependencyCatalogGraph::default();
    for family_code in [1u16, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] {
        let prefix = family_code.to_be_bytes();
        let mut family_count = 0usize;
        for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if catalog_writes.iter().any(|write| write.key == key) {
                continue;
            }
            family_count += 1;
            if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            collect_dependency_catalog_record(key, value.value(), &mut graph)?;
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&prefix))
        {
            if let Some(bytes) = &write.payload {
                family_count += 1;
                if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                    return Err(CommitError::InvalidCatalog);
                }
                collect_dependency_catalog_record(&write.key, bytes, &mut graph)?;
            }
        }
    }

    let DependencyCatalogGraph {
        database_id,
        object_parents,
        object_kinds,
        typed_object_kinds,
        column_tables,
        index_tables,
        index_columns,
        policy_tables: _,
        policy_kinds: _,
        constraint_kinds,
        foreign_key_targets,
        backing_index_targets,
        index_owners,
        expression_reference_edges,
        authority_edges,
        authority_principals,
        dependencies,
    } = &graph;
    let database_record_changed = catalog_writes
        .iter()
        .any(|write| write.key.starts_with(&1u16.to_be_bytes()));
    if database_id.is_some_and(|database_id| database_id != database_uuid)
        || (database_record_changed && *database_id != Some(database_uuid))
        || dependencies
            .iter()
            .any(|(dependent_id, referenced_id, kind)| {
                !object_parents.contains_key(dependent_id)
                    || !(object_parents.contains_key(referenced_id)
                        || *kind == 5 && Some(*referenced_id) == *database_id)
            })
    {
        return Err(CommitError::InvalidCatalog);
    }
    if index_tables.iter().any(|(index_id, table_id)| {
        let Some(columns) = index_columns.get(index_id) else {
            return true;
        };
        object_kinds.get(table_id) != Some(&3)
            || typed_object_kinds.get(table_id) != Some(&3)
            || columns.is_empty()
            || columns.iter().any(|column_id| {
                object_kinds.get(column_id) != Some(&4)
                    || typed_object_kinds.get(column_id) != Some(&4)
                    || column_tables.get(column_id) != Some(table_id)
            })
    }) {
        return Err(CommitError::InvalidCatalog);
    }
    let actual_expression_reference_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 2)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<HashSet<_>>();
    if &actual_expression_reference_edges != expression_reference_edges
        || expression_reference_edges
            .iter()
            .any(|(dependent_id, referenced_id)| {
                let endpoint_kinds = (
                    object_kinds.get(dependent_id),
                    object_kinds.get(referenced_id),
                    typed_object_kinds.get(dependent_id),
                    typed_object_kinds.get(referenced_id),
                );
                !matches!(
                    endpoint_kinds,
                    (Some(4), Some(5), Some(4), Some(5))
                        | (Some(6), Some(5), Some(6), Some(5))
                        | (Some(10), Some(5), Some(10), Some(5))
                        | (Some(5), Some(4), Some(5), Some(4))
                        | (Some(5), Some(9), Some(5), Some(9))
                )
            })
    {
        return Err(CommitError::InvalidCatalog);
    }
    let actual_authority_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 5)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<HashSet<_>>();
    if &actual_authority_edges != authority_edges
        || authority_principals.iter().any(|principal_id| {
            object_kinds.get(principal_id) != typed_object_kinds.get(principal_id)
        })
        || authority_edges.iter().any(|(dependent_id, referenced_id)| {
            !authority_principals.contains(dependent_id)
                && !matches!(object_kinds.get(dependent_id), Some(10))
                || (!authority_principals.contains(referenced_id)
                    && !object_parents.contains_key(referenced_id)
                    && Some(*referenced_id) != *database_id)
        })
    {
        return Err(CommitError::InvalidCatalog);
    }
    if !crate::catalog::validate_owned_child_graph(object_parents, dependencies) {
        return Err(CommitError::InvalidCatalog);
    }
    let foreign_key_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 3)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<HashSet<_>>();
    if dependencies.iter().filter(|(_, _, kind)| *kind == 3).any(
        |(dependent_id, referenced_id, _)| {
            object_kinds.get(dependent_id) != Some(&6)
                || object_kinds.get(referenced_id) != Some(&6)
                || constraint_kinds.get(dependent_id) != Some(&5)
                || !matches!(constraint_kinds.get(referenced_id), Some(2 | 3))
                || foreign_key_targets.get(dependent_id) != Some(referenced_id)
        },
    ) || foreign_key_targets
        .iter()
        .any(|(dependent_id, referenced_id)| {
            !foreign_key_edges.contains(&(*dependent_id, *referenced_id))
        })
    {
        return Err(CommitError::InvalidCatalog);
    }
    let backing_index_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 4)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<HashSet<_>>();
    if dependencies.iter().filter(|(_, _, kind)| *kind == 4).any(
        |(dependent_id, referenced_id, _)| {
            object_kinds.get(dependent_id) != Some(&6)
                || object_kinds.get(referenced_id) != Some(&7)
                || !matches!(constraint_kinds.get(dependent_id), Some(2 | 3))
                || backing_index_targets.get(dependent_id) != Some(referenced_id)
                || index_owners.get(referenced_id) != Some(dependent_id)
        },
    ) || backing_index_targets
        .iter()
        .any(|(dependent_id, referenced_id)| {
            !backing_index_edges.contains(&(*dependent_id, *referenced_id))
        })
        || index_owners.iter().any(|(referenced_id, dependent_id)| {
            !backing_index_edges.contains(&(*dependent_id, *referenced_id))
        })
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

/// Require every object owner to resolve to a user or role in the final
/// catalog, including when a principal is changed or removed.
fn validate_final_object_owners(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let is_relevant_family =
        |key: &[u8]| key.len() >= 2 && matches!(u16::from_be_bytes([key[0], key[1]]), 2 | 10);
    if !catalog_writes
        .iter()
        .any(|write| is_relevant_family(&write.key))
    {
        return Ok(());
    }

    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let mut owner_ids = Vec::<[u8; 16]>::new();
    let mut principal_ids = HashSet::<[u8; 16]>::new();
    for family_code in [2u16, 10] {
        let prefix = family_code.to_be_bytes();
        let mut family_count = 0usize;
        for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if catalog_writes.iter().any(|write| write.key == key) {
                continue;
            }
            family_count += 1;
            if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            collect_owner_catalog_record(
                family_code,
                key,
                value.value(),
                &mut owner_ids,
                &mut principal_ids,
            )?;
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&prefix))
        {
            if let Some(bytes) = &write.payload {
                family_count += 1;
                if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                    return Err(CommitError::InvalidCatalog);
                }
                collect_owner_catalog_record(
                    family_code,
                    &write.key,
                    bytes,
                    &mut owner_ids,
                    &mut principal_ids,
                )?;
            }
        }
    }
    if owner_ids
        .iter()
        .any(|owner_id| !principal_ids.contains(owner_id))
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

/// Require the final set of user and role names to be unique in their shared
/// database namespace. The DDL layer supplies already-normalized names.
fn validate_final_principal_names(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let prefix = 10u16.to_be_bytes();
    if !catalog_writes
        .iter()
        .any(|write| write.key.starts_with(&prefix))
    {
        return Ok(());
    }

    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let changed_keys = catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
        .map(|write| write.key.as_slice())
        .collect::<HashSet<_>>();
    let mut names = HashSet::<Vec<u8>>::new();
    let mut final_count = 0usize;
    for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if changed_keys.contains(key) {
            continue;
        }
        final_count += 1;
        if final_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value();
        let name = catalog_field_bytes(key, bytes, 10, 2)?;
        if !names.insert(name) {
            return Err(CommitError::InvalidCatalog);
        }
    }

    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
    {
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let Some(bytes) = &write.payload else {
            continue;
        };
        final_count += 1;
        if final_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        decode_catalog_record(&write.key, bytes)?;
        let name = catalog_field_bytes(&write.key, bytes, 10, 2)?;
        if !names.insert(name) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    Ok(())
}

fn collect_owner_catalog_record(
    family_code: u16,
    key: &[u8],
    bytes: &[u8],
    owner_ids: &mut Vec<[u8; 16]>,
    principal_ids: &mut HashSet<[u8; 16]>,
) -> Result<(), CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if key.len() != 19 || record.family_code != family_code {
        return Err(CommitError::InvalidCatalog);
    }
    let object_id = key[3..19]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    match family_code {
        2 => {
            let owner_id = catalog_field_bytes(key, bytes, 2, 4)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            owner_ids.push(owner_id);
        }
        10 => {
            let kind = u16::from_le_bytes(
                catalog_field_bytes(key, bytes, 10, 1)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !matches!(kind, 1 | 2) || !principal_ids.insert(object_id) {
                return Err(CommitError::InvalidCatalog);
            }
        }
        _ => return Err(CommitError::InvalidCatalog),
    }
    Ok(())
}

/// Require affected object IDs to retain exactly their matching typed rows in
/// the final catalog, with neither side surviving alone.
fn validate_final_typed_object_mappings(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    const TYPED_FAMILIES: [u16; 9] = [3, 4, 5, 6, 7, 8, 9, 10, 13];
    let is_relevant_family = |family: u16| family == 2 || TYPED_FAMILIES.contains(&family);
    if !catalog_writes.iter().any(|write| {
        write.key.len() >= 2 && is_relevant_family(u16::from_be_bytes([write.key[0], write.key[1]]))
    }) {
        return Ok(());
    }
    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    if catalog_writes
        .iter()
        .any(|write| write.key.len() >= 2 && write.key[..2] == 4u16.to_be_bytes())
    {
        validate_final_builtin_type_catalog(&table, catalog_writes)?;
    }
    let mut changed_ids = HashSet::<[u8; 16]>::new();
    for write in catalog_writes.iter().filter(|write| {
        write.key.len() >= 2 && is_relevant_family(u16::from_be_bytes([write.key[0], write.key[1]]))
    }) {
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let object_id = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        changed_ids.insert(object_id);
    }

    for object_id in changed_ids {
        let object_key = id_catalog_key(2, object_id);
        let object_bytes = final_catalog_value(&table, catalog_writes, &object_key)?;
        let object_kind = object_bytes
            .as_deref()
            .map(|bytes| catalog_field_bytes(&object_key, bytes, 2, 1))
            .transpose()?
            .map(|bytes| -> Result<u16, CommitError> {
                let bytes = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                Ok(u16::from_le_bytes(bytes))
            })
            .transpose()?;

        if let Some(object_kind) = object_kind {
            let typed_family = match object_kind {
                1 => 3,
                2 => 4,
                3 => 5,
                4 => 6,
                5 => 7,
                6 => 8,
                7 => 9,
                8 | 9 => 10,
                10 => 13,
                _ => return Err(CommitError::InvalidCatalog),
            };
            let typed_key = id_catalog_key(typed_family, object_id);
            if final_catalog_value(&table, catalog_writes, &typed_key)?.is_none() {
                return Err(CommitError::InvalidCatalog);
            }
        }

        for family in TYPED_FAMILIES {
            let typed_key = id_catalog_key(family, object_id);
            let Some(typed_bytes) = final_catalog_value(&table, catalog_writes, &typed_key)? else {
                continue;
            };
            decode_catalog_record(&typed_key, &typed_bytes)?;
            let expected_kind = match family {
                3 => 1,
                4 => 2,
                5 => 3,
                6 => 4,
                7 => 5,
                8 => 6,
                9 => 7,
                10 => {
                    let principal_kind = u16::from_le_bytes(
                        catalog_field_bytes(&typed_key, &typed_bytes, family, 1)?
                            .as_slice()
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    );
                    match principal_kind {
                        1 => 8,
                        2 => 9,
                        _ => return Err(CommitError::InvalidCatalog),
                    }
                }
                13 => 10,
                _ => unreachable!(),
            };
            if object_kind != Some(expected_kind) {
                return Err(CommitError::InvalidCatalog);
            }
            let name_tag = match family {
                6 => Some(3),
                10 => Some(2),
                _ => None,
            };
            if let Some(name_tag) = name_tag {
                let object_bytes = object_bytes.as_deref().ok_or(CommitError::InvalidCatalog)?;
                if catalog_field_bytes(&object_key, object_bytes, 2, 3)?
                    != catalog_field_bytes(&typed_key, &typed_bytes, family, name_tag)?
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
    }
    Ok(())
}

/// Keep the object and typed rows named by the database bootstrap descriptor
/// valid in the final catalog view.
fn validate_final_bootstrap_builtin_targets(
    transaction: &WriteTransaction,
    writes: &[CatalogWrite],
    database_uuid: [u8; 16],
) -> Result<(), CommitError> {
    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    let database_key = id_catalog_key(1, database_uuid);
    let Some(database_bytes) = final_catalog_value(&catalog, writes, &database_key)? else {
        return Ok(());
    };
    let encoded = catalog_field_bytes(&database_key, &database_bytes, 1, 3)?;
    let count_bytes: [u8; 4] = encoded
        .get(..4)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    if count != 30 || encoded.len() != 4 + count * 20 {
        return Err(CommitError::InvalidCatalog);
    }
    let expected_codes = [1u16, 2, 3].into_iter().chain(101..=111).chain(201..=216);
    let mut mappings = HashMap::<u16, [u8; 16]>::new();
    let mut ids = HashSet::<[u8; 16]>::new();
    for (entry, expected_code) in encoded[4..].chunks_exact(20).zip(expected_codes) {
        let code = u16::from_le_bytes([entry[0], entry[1]]);
        let reserved = u16::from_le_bytes([entry[2], entry[3]]);
        let id: [u8; 16] = entry[4..]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if code != expected_code
            || reserved != 0
            || id == [0; 16]
            || mappings.insert(code, id).is_some()
            || !ids.insert(id)
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for code in [1u16, 2] {
        let schema_id = mappings.get(&code).ok_or(CommitError::InvalidCatalog)?;
        validate_final_builtin_typed_object(&catalog, writes, *schema_id, 3, 1)?;
    }
    let admin_role_id = mappings.get(&3).ok_or(CommitError::InvalidCatalog)?;
    validate_final_builtin_typed_object(&catalog, writes, *admin_role_id, 10, 9)?;
    let principal_key = id_catalog_key(10, *admin_role_id);
    let principal_bytes = final_catalog_value(&catalog, writes, &principal_key)?
        .ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_bytes(&principal_key, &principal_bytes, 10, 1)? != 2u16.to_le_bytes()
        || catalog_field_bytes(&principal_key, &principal_bytes, 10, 2)? != b"database_admin"
    {
        return Err(CommitError::InvalidCatalog);
    }
    let object_key = id_catalog_key(2, *admin_role_id);
    let object_bytes =
        final_catalog_value(&catalog, writes, &object_key)?.ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_bytes(&object_key, &object_bytes, 2, 3)? != b"database_admin" {
        return Err(CommitError::InvalidCatalog);
    }
    for code in 101u16..=111 {
        let type_id = mappings.get(&code).ok_or(CommitError::InvalidCatalog)?;
        validate_final_builtin_typed_object(&catalog, writes, *type_id, 4, 2)?;
        let type_key = id_catalog_key(4, *type_id);
        let type_bytes =
            final_catalog_value(&catalog, writes, &type_key)?.ok_or(CommitError::InvalidCatalog)?;
        let type_record = decode_catalog_record(&type_key, &type_bytes)?;
        if validate_builtin_type_profile(&type_record).map_err(|_| CommitError::InvalidCatalog)?
            != code - 100
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    Ok(())
}

fn validate_final_builtin_typed_object(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    writes: &[CatalogWrite],
    object_id: [u8; 16],
    family: u16,
    object_kind: u16,
) -> Result<(), CommitError> {
    let typed_key = id_catalog_key(family, object_id);
    let typed_bytes =
        final_catalog_value(catalog, writes, &typed_key)?.ok_or(CommitError::InvalidCatalog)?;
    let typed_record = decode_catalog_record(&typed_key, &typed_bytes)?;
    if family == 4 && validate_builtin_type_profile(&typed_record).is_err() {
        return Err(CommitError::InvalidCatalog);
    }
    let object_key = id_catalog_key(2, object_id);
    let object_bytes =
        final_catalog_value(catalog, writes, &object_key)?.ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_bytes(&object_key, &object_bytes, 2, 1)? != object_kind.to_le_bytes() {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

fn insert_expression_reference_edge(
    edges: &mut HashSet<([u8; 16], [u8; 16])>,
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
) -> Result<(), CommitError> {
    edges.insert((dependent_id, referenced_id));
    if edges.len() > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

fn insert_authority_edge(
    edges: &mut HashSet<([u8; 16], [u8; 16])>,
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
) -> Result<(), CommitError> {
    edges.insert((dependent_id, referenced_id));
    if edges.len() > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

fn validate_final_builtin_type_catalog(
    table: &redb::Table<'_, &[u8], &[u8]>,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let family_prefix = 4u16.to_be_bytes();
    let mut seen_kinds = HashSet::new();
    let mut family_count = 0usize;
    for entry in table
        .range(family_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&family_prefix) {
            break;
        }
        if catalog_writes.iter().any(|write| write.key == key) {
            continue;
        }
        family_count += 1;
        if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        let record = decode_catalog_record(key, value.value())?;
        let builtin_kind =
            validate_builtin_type_profile(&record).map_err(|_| CommitError::InvalidCatalog)?;
        if !seen_kinds.insert(builtin_kind) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&family_prefix))
    {
        if let Some(bytes) = &write.payload {
            family_count += 1;
            if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            let record = decode_catalog_record(&write.key, bytes)?;
            let builtin_kind =
                validate_builtin_type_profile(&record).map_err(|_| CommitError::InvalidCatalog)?;
            if !seen_kinds.insert(builtin_kind) {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

fn collect_dependency_catalog_record(
    key: &[u8],
    bytes: &[u8],
    graph: &mut DependencyCatalogGraph,
) -> Result<(), CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    match record.family_code {
        1 => {
            if key.len() != 19
                || graph
                    .database_id
                    .replace(key[3..19].try_into().unwrap())
                    .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        2 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let object_id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let parent_id = record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .map(|field| {
                    field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)
                })
                .transpose()?;
            let object_kind = u16::from_le_bytes(
                catalog_field_bytes(key, bytes, 2, 1)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !(1..=10).contains(&object_kind)
                || graph.object_parents.insert(object_id, parent_id).is_some()
                || graph.object_kinds.insert(object_id, object_kind).is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        5 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let table_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.insert(table_id, 3).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
        }
        6 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let column_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.insert(column_id, 4).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            let table_id: [u8; 16] = catalog_field_bytes(key, bytes, 6, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.column_tables.insert(column_id, table_id).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            for tag in [7, 10] {
                if let Some(field) = record.fields.iter().find(|field| field.tag == tag) {
                    let expression_id: [u8; 16] = field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    insert_expression_reference_edge(
                        &mut graph.expression_reference_edges,
                        column_id,
                        expression_id,
                    )?;
                }
            }
        }
        7 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let expression = decode_catalog_expression_record(&record)
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph
                .typed_object_kinds
                .insert(expression.expression_id, 5)
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            let mut unique_dependencies = HashSet::new();
            for dependency_id in expression.dependencies {
                if dependency_id == [0; 16] || !unique_dependencies.insert(dependency_id) {
                    return Err(CommitError::InvalidCatalog);
                }
                insert_expression_reference_edge(
                    &mut graph.expression_reference_edges,
                    expression.expression_id,
                    dependency_id,
                )?;
            }
        }
        8 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let constraint_id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.insert(constraint_id, 6).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            let kind = u16::from_le_bytes(
                catalog_field_bytes(key, bytes, 8, 2)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !matches!(kind, 1..=5)
                || graph.constraint_kinds.insert(constraint_id, kind).is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if kind == 5
                && graph
                    .foreign_key_targets
                    .insert(
                        constraint_id,
                        catalog_field_bytes(key, bytes, 8, 5)?
                            .as_slice()
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    )
                    .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if matches!(kind, 2 | 3)
                && graph
                    .backing_index_targets
                    .insert(
                        constraint_id,
                        catalog_field_bytes(key, bytes, 8, 11)?
                            .as_slice()
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    )
                    .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if kind == 4 {
                insert_expression_reference_edge(
                    &mut graph.expression_reference_edges,
                    constraint_id,
                    catalog_field_bytes(key, bytes, 8, 4)?
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                )?;
            } else if record.fields.iter().any(|field| field.tag == 4) {
                return Err(CommitError::InvalidCatalog);
            }
        }
        9 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let index_id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let table_id: [u8; 16] = catalog_field_bytes(key, bytes, 9, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let columns = catalog_id_list_field(key, bytes, 9, 2)?;
            let mut unique_columns = HashSet::new();
            if graph.typed_object_kinds.insert(index_id, 7).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            if columns.is_empty()
                || columns
                    .iter()
                    .any(|column_id| !unique_columns.insert(*column_id))
                || graph.index_tables.insert(index_id, table_id).is_some()
                || graph.index_columns.insert(index_id, columns).is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(field) = record.fields.iter().find(|field| field.tag == 6)
                && graph
                    .index_owners
                    .insert(
                        index_id,
                        field
                            .payload
                            .as_slice()
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    )
                    .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        10 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let principal_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let principal_kind = decode_principal_kind(key, bytes)?;
            let object_kind = match principal_kind {
                1 => 8,
                2 => 9,
                _ => return Err(CommitError::InvalidCatalog),
            };
            if graph
                .typed_object_kinds
                .insert(principal_id, object_kind)
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            graph.authority_principals.insert(principal_id);
        }
        11 => {
            if key.len() != 36 || catalog_field_u64(key, bytes, 11, 2)? == 0 {
                return Err(CommitError::InvalidCatalog);
            }
            let member_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let role_id: [u8; 16] = key[20..36]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let grantor_id: [u8; 16] = catalog_field_bytes(key, bytes, 11, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if !graph.authority_principals.contains(&grantor_id)
                || !graph
                    .typed_object_kinds
                    .get(&member_id)
                    .is_some_and(|kind| matches!(*kind, 8 | 9))
                || graph.typed_object_kinds.get(&role_id) != Some(&9)
            {
                return Err(CommitError::InvalidCatalog);
            }
            insert_authority_edge(&mut graph.authority_edges, member_id, role_id)?;
        }
        12 => {
            if key.len() != 59
                || key[2] != 1
                || key[19] != 1
                || key[36] != 1
                || key[39] != 1
                || key[42] != 1
                || catalog_field_u64(key, bytes, 12, 2)? == 0
            {
                return Err(CommitError::InvalidCatalog);
            }
            let grantee_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let object_id: [u8; 16] = key[20..36]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let privilege = u16::from_be_bytes([key[37], key[38]]);
            let scope = u16::from_be_bytes([key[40], key[41]]);
            let target_id: [u8; 16] = key[43..59]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let grantor_id: [u8; 16] = catalog_field_bytes(key, bytes, 12, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if !graph.authority_principals.contains(&grantee_id)
                || !graph.authority_principals.contains(&grantor_id)
            {
                return Err(CommitError::InvalidCatalog);
            }
            let target_kind = graph.object_kinds.get(&target_id).copied();
            let valid_scope = match (privilege, scope) {
                (1 | 7, 1) => object_id == target_id && target_kind == Some(1),
                (2..=4 | 6, 1) => object_id == target_id && target_kind == Some(3),
                (5, 1) => object_id == target_id && target_kind == Some(3),
                (8..=11, 1) => object_id == target_id && matches!(target_kind, Some(1 | 3)),
                (2..=4 | 6, 2) => {
                    graph.object_kinds.get(&object_id) == Some(&3)
                        && target_kind == Some(4)
                        && graph.column_tables.get(&target_id) == Some(&object_id)
                }
                (12..=14, 1) => object_id == target_id && Some(target_id) == graph.database_id,
                _ => false,
            };
            if !valid_scope {
                return Err(CommitError::InvalidCatalog);
            }
            insert_authority_edge(&mut graph.authority_edges, grantee_id, target_id)?;
        }
        14 => {
            if key.len() != 39 || key[2] != 1 || key[19] != 1 || key[22] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            let policy_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let target_kind = u16::from_be_bytes([key[20], key[21]]);
            let target_id: [u8; 16] = key[23..39]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.get(&policy_id) != Some(&10) {
                return Err(CommitError::InvalidCatalog);
            }
            match target_kind {
                1 if graph.typed_object_kinds.get(&target_id) == Some(&8) => {
                    insert_authority_edge(&mut graph.authority_edges, policy_id, target_id)?;
                }
                2 if graph.typed_object_kinds.get(&target_id) == Some(&9) => {
                    insert_authority_edge(&mut graph.authority_edges, policy_id, target_id)?;
                }
                3 if graph.policy_kinds.get(&policy_id) == Some(&2)
                    && graph.typed_object_kinds.get(&target_id) == Some(&4)
                    && graph.column_tables.get(&target_id)
                        == graph.policy_tables.get(&policy_id)
                    && graph.policy_tables.get(&policy_id).is_some_and(|table_id| {
                        graph.object_kinds.get(table_id) == Some(&3)
                            && graph.typed_object_kinds.get(table_id) == Some(&3)
                    }) => {}
                _ => return Err(CommitError::InvalidCatalog),
            }
        }
        13 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let policy_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.insert(policy_id, 10).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            let table_id: [u8; 16] = catalog_field_bytes(key, bytes, 13, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let policy_kind = u16::from_le_bytes(
                catalog_field_bytes(key, bytes, 13, 2)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !matches!(policy_kind, 1 | 2)
                || graph.policy_tables.insert(policy_id, table_id).is_some()
                || graph.policy_kinds.insert(policy_id, policy_kind).is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            for tag in [5, 6] {
                if let Some(field) = record.fields.iter().find(|field| field.tag == tag) {
                    let expression_id: [u8; 16] = field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    insert_expression_reference_edge(
                        &mut graph.expression_reference_edges,
                        policy_id,
                        expression_id,
                    )?;
                }
            }
        }
        15 => {
            if key.len() != 39 {
                return Err(CommitError::InvalidCatalog);
            }
            let dependent_id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let referenced_id = key[20..36]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let kind = u16::from_be_bytes(
                key[37..39]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !matches!(kind, 1..=5) {
                return Err(CommitError::InvalidCatalog);
            }
            graph.dependencies.push((dependent_id, referenced_id, kind));
        }
        _ => return Err(CommitError::InvalidCatalog),
    }
    Ok(())
}

/// Revalidate surviving constraints when definitions they reference can change.
/// Stream the current family and overlay staged changes so deletion of a
/// referenced object cannot leave an unchanged constraint dangling.
fn validate_final_constraints(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    if !writes.iter().any(|write| {
        write.key.len() >= 2 && (5..=9).contains(&u16::from_be_bytes([write.key[0], write.key[1]]))
    }) {
        return Ok(());
    }
    let prefix = 8u16.to_be_bytes();
    let mut count = 0usize;
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if writes.iter().any(|write| write.key == key) {
            continue;
        }
        count += 1;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        validate_constraint_shape(key, value.value())?;
        validate_constraint_references(catalog, writes, key, value.value())?;
    }
    for write in writes.iter().filter(|write| write.key.starts_with(&prefix)) {
        if let Some(bytes) = &write.payload {
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            validate_constraint_shape(&write.key, bytes)?;
            validate_constraint_references(catalog, writes, &write.key, bytes)?;
        }
    }
    Ok(())
}

fn validate_constraint_shape(key: &[u8], bytes: &[u8]) -> Result<(), CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.family_code != 8 || record.logical_key != key {
        return Err(CommitError::InvalidCatalog);
    }
    let field = |tag: u16| {
        record
            .fields
            .iter()
            .find(|candidate| candidate.tag == tag)
            .map(|candidate| candidate.payload.as_slice())
            .ok_or(CommitError::InvalidCatalog)
    };
    let optional = |tag: u16| {
        record
            .fields
            .iter()
            .find(|candidate| candidate.tag == tag)
            .map(|candidate| candidate.payload.as_slice())
    };
    let read_u16 = |tag| -> Result<u16, CommitError> {
        Ok(u16::from_le_bytes(
            field(tag)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ))
    };
    let read_ids = |tag| -> Result<Vec<[u8; 16]>, CommitError> {
        let bytes = field(tag)?;
        if bytes.len() < 4 {
            return Err(CommitError::InvalidCatalog);
        }
        let count = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        if count > 256 || bytes.len() != 4 + count * 16 {
            return Err(CommitError::InvalidCatalog);
        }
        bytes[4..]
            .chunks_exact(16)
            .map(|value| value.try_into().map_err(|_| CommitError::InvalidCatalog))
            .collect()
    };
    let kind = read_u16(2)?;
    let source_columns = read_ids(3)?;
    let referenced_columns = read_ids(6)?;
    let referenced_constraint = optional(5).is_some();
    let expression = optional(4).is_some();
    let backing_index = optional(11).is_some();
    let actions = [read_u16(7)?, read_u16(8)?, read_u16(9)?];
    let valid = read_u16(10)? == 1
        && match kind {
            1 => {
                source_columns.len() == 1
                    && !expression
                    && !referenced_constraint
                    && referenced_columns.is_empty()
                    && !backing_index
                    && actions == [0, 0, 0]
            }
            2 | 3 => {
                !source_columns.is_empty()
                    && !expression
                    && !referenced_constraint
                    && referenced_columns.is_empty()
                    && backing_index
                    && actions == [0, 0, 0]
            }
            4 => {
                source_columns.is_empty()
                    && expression
                    && !referenced_constraint
                    && referenced_columns.is_empty()
                    && !backing_index
                    && actions == [0, 0, 0]
            }
            5 => {
                !source_columns.is_empty()
                    && referenced_columns.len() == source_columns.len()
                    && !expression
                    && referenced_constraint
                    && actions == [1, 1, 1]
            }
            _ => false,
        };
    if valid {
        Ok(())
    } else {
        Err(CommitError::InvalidCatalog)
    }
}

fn final_catalog_value<T>(
    catalog: &T,
    writes: &[CatalogWrite],
    key: &[u8],
) -> Result<Option<Vec<u8>>, CommitError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    if let Some(write) = writes.iter().find(|write| write.key == key) {
        return Ok(write.payload.clone());
    }
    Ok(catalog
        .get(key)
        .map_err(storage_error)?
        .map(|value| value.value().to_vec()))
}

fn catalog_field_bytes(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<Vec<u8>, CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.family_code != family || record.logical_key != key {
        return Err(CommitError::InvalidCatalog);
    }
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.clone())
        .ok_or(CommitError::InvalidCatalog)
}

fn catalog_id_list_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<Vec<[u8; 16]>, CommitError> {
    let payload = catalog_field_bytes(key, bytes, family, tag)?;
    if payload.len() < 4 {
        return Err(CommitError::InvalidCatalog);
    }
    let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
    if count > 256 || payload.len() != 4 + count * 16 {
        return Err(CommitError::InvalidCatalog);
    }
    payload[4..]
        .chunks_exact(16)
        .map(|id| id.try_into().map_err(|_| CommitError::InvalidCatalog))
        .collect()
}

fn id_catalog_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

fn validate_constraint_references(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    writes: &[CatalogWrite],
    key: &[u8],
    bytes: &[u8],
) -> Result<(), CommitError> {
    let table_id = constraint_table_id(key, bytes)?;
    let table_key = id_catalog_key(5, table_id);
    let Some(table_bytes) = final_catalog_value(catalog, writes, &table_key)? else {
        return Err(CommitError::InvalidCatalog);
    };
    let table_record =
        decode_registered_envelope(&table_bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if table_record.family_code != 5 || table_record.logical_key != table_key {
        return Err(CommitError::InvalidCatalog);
    }
    let kind = u16::from_le_bytes(
        catalog_field_bytes(key, bytes, 8, 2)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    let source_columns = catalog_id_list_field(key, bytes, 8, 3)?;
    let referenced_columns = catalog_id_list_field(key, bytes, 8, 6)?;
    for column_id in &source_columns {
        let column_key = id_catalog_key(6, *column_id);
        let Some(column_bytes) = final_catalog_value(catalog, writes, &column_key)? else {
            return Err(CommitError::InvalidCatalog);
        };
        if column_table_id(&column_key, &column_bytes)? != table_id {
            return Err(CommitError::InvalidCatalog);
        }
        if kind == 2 && catalog_field_bytes(&column_key, &column_bytes, 6, 6)? != [0] {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if kind == 1 {
        let column_key = id_catalog_key(6, source_columns[0]);
        if catalog_field_bytes(
            &column_key,
            &final_catalog_value(catalog, writes, &column_key)?
                .ok_or(CommitError::InvalidCatalog)?,
            6,
            6,
        )? != [0]
        {
            return Err(CommitError::InvalidCatalog);
        }
    } else if kind == 2 || kind == 3 {
        let constraint_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let index_id: [u8; 16] = catalog_field_bytes(key, bytes, 8, 11)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let index_key = id_catalog_key(9, index_id);
        let Some(index_bytes) = final_catalog_value(catalog, writes, &index_key)? else {
            return Err(CommitError::InvalidCatalog);
        };
        let index_table: [u8; 16] = catalog_field_bytes(&index_key, &index_bytes, 9, 1)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let index_columns = catalog_id_list_field(&index_key, &index_bytes, 9, 2)?;
        let unique = catalog_field_bytes(&index_key, &index_bytes, 9, 4)? == [1];
        let owner = catalog_field_bytes(&index_key, &index_bytes, 9, 6)?;
        let state = u16::from_le_bytes(
            catalog_field_bytes(&index_key, &index_bytes, 9, 8)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
        if index_table != table_id
            || index_columns != source_columns
            || !unique
            || owner != constraint_id
            || state != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
    } else if kind == 4 {
        let expression_id: [u8; 16] = catalog_field_bytes(key, bytes, 8, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let expression_key = id_catalog_key(7, expression_id);
        let Some(expression_bytes) = final_catalog_value(catalog, writes, &expression_key)? else {
            return Err(CommitError::InvalidCatalog);
        };
        let expression_record = decode_catalog_record(&expression_key, &expression_bytes)?;
        let expression = decode_catalog_expression_record(&expression_record)
            .map_err(|_| CommitError::InvalidCatalog)?;
        if expression.purpose != crate::expression::ExpressionPurpose::Check {
            return Err(CommitError::InvalidCatalog);
        }
    } else if kind == 5 {
        let target_id: [u8; 16] = catalog_field_bytes(key, bytes, 8, 5)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let target_key = id_catalog_key(8, target_id);
        let Some(target_bytes) = final_catalog_value(catalog, writes, &target_key)? else {
            return Err(CommitError::InvalidCatalog);
        };
        let target_kind = u16::from_le_bytes(
            catalog_field_bytes(&target_key, &target_bytes, 8, 2)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
        let target_table_id = constraint_table_id(&target_key, &target_bytes)?;
        let target_columns = catalog_id_list_field(&target_key, &target_bytes, 8, 3)?;
        if !matches!(target_kind, 2 | 3) || target_columns != referenced_columns {
            return Err(CommitError::InvalidCatalog);
        }
        validate_constraint_shape(&target_key, &target_bytes)?;
        validate_constraint_references(catalog, writes, &target_key, &target_bytes)?;
        for column_id in &referenced_columns {
            let column_key = id_catalog_key(6, *column_id);
            let Some(column_bytes) = final_catalog_value(catalog, writes, &column_key)? else {
                return Err(CommitError::InvalidCatalog);
            };
            if column_table_id(&column_key, &column_bytes)? != target_table_id {
                return Err(CommitError::InvalidCatalog);
            }
        }
        for (source_column_id, target_column_id) in source_columns.iter().zip(&referenced_columns) {
            let source_column_key = id_catalog_key(6, *source_column_id);
            let target_column_key = id_catalog_key(6, *target_column_id);
            let source_column = final_catalog_value(catalog, writes, &source_column_key)?
                .ok_or(CommitError::InvalidCatalog)?;
            let target_column = final_catalog_value(catalog, writes, &target_column_key)?
                .ok_or(CommitError::InvalidCatalog)?;
            if catalog_field_bytes(&source_column_key, &source_column, 6, 4)?
                != catalog_field_bytes(&target_column_key, &target_column, 6, 4)?
                || catalog_field_bytes(&source_column_key, &source_column, 6, 5)?
                    != catalog_field_bytes(&target_column_key, &target_column, 6, 5)?
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

fn validate_column_schema_epochs(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&5u16.to_be_bytes()))
    {
        let Some(next) = &write.payload else {
            continue;
        };
        let next_schema = table_schema_version(&write.key, next)?;
        let next_write = table_write_epoch(&write.key, next)?;
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if let Some(current) = current
            && current != *next
        {
            let current_schema = table_schema_version(&write.key, &current)?;
            let current_write = table_write_epoch(&write.key, &current)?;
            if next_schema < current_schema
                || next_write < current_write
                || (next_schema == current_schema && next_write == current_write)
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    let column_prefix = 6u16.to_be_bytes();
    let mut affected_tables = HashSet::<[u8; 16]>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&column_prefix))
    {
        let current = catalog
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let current_table = current
            .as_deref()
            .map(|bytes| column_table_id(&write.key, bytes))
            .transpose()?;
        let final_table = write
            .payload
            .as_deref()
            .map(|bytes| column_table_id(&write.key, bytes))
            .transpose()?;
        if current_table.is_some() && final_table.is_some() && current_table != final_table {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(table_id) = current_table.or(final_table) {
            affected_tables.insert(table_id);
        }
    }
    for table_id in affected_tables {
        let table_key = catalog_id_key(5, table_id);
        let current = catalog
            .get(&table_key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        let final_bytes =
            if let Some(write) = catalog_writes.iter().find(|write| write.key == table_key) {
                write.payload.clone()
            } else {
                current.clone()
            };
        if let (Some(current), Some(final_bytes)) = (current, final_bytes)
            && table_schema_version(&table_key, &final_bytes)?
                <= table_schema_version(&table_key, &current)?
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    Ok(())
}

fn catalog_id_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

fn catalog_field_u64(key: &[u8], bytes: &[u8], family: u16, tag: u16) -> Result<u64, CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != family {
        return Err(CommitError::InvalidCatalog);
    }
    let field = record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .ok_or(CommitError::InvalidCatalog)?;
    let value = u64::from_le_bytes(
        field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if value == 0 {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(value)
}

fn catalog_field_u16(key: &[u8], bytes: &[u8], family: u16, tag: u16) -> Result<u16, CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != family {
        return Err(CommitError::InvalidCatalog);
    }
    let field = record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .ok_or(CommitError::InvalidCatalog)?;
    Ok(u16::from_le_bytes(
        field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ))
}

fn policy_table_id(key: &[u8], bytes: &[u8]) -> Result<[u8; 16], CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 13 || key.len() != 19 {
        return Err(CommitError::InvalidCatalog);
    }
    record
        .fields
        .iter()
        .find(|field| field.tag == 1)
        .ok_or(CommitError::InvalidCatalog)?
        .payload
        .as_slice()
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)
}

fn column_table_id(key: &[u8], bytes: &[u8]) -> Result<[u8; 16], CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 6 || key.len() != 19 {
        return Err(CommitError::InvalidCatalog);
    }
    record
        .fields
        .iter()
        .find(|field| field.tag == 1)
        .ok_or(CommitError::InvalidCatalog)?
        .payload
        .as_slice()
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)
}

fn constraint_table_id(key: &[u8], bytes: &[u8]) -> Result<[u8; 16], CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 8 || key.len() != 19 {
        return Err(CommitError::InvalidCatalog);
    }
    record
        .fields
        .iter()
        .find(|field| field.tag == 1)
        .ok_or(CommitError::InvalidCatalog)?
        .payload
        .as_slice()
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)
}

fn policy_authority_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    catalog_field_u64(key, bytes, 13, 7)
}

fn table_write_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    catalog_field_u64(key, bytes, 5, 3)
}

fn table_schema_version(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    catalog_field_u64(key, bytes, 5, 2)
}

fn membership_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 11 || key.len() != 36 {
        return Err(CommitError::InvalidCatalog);
    }
    let field = record
        .fields
        .iter()
        .find(|field| field.tag == 2)
        .ok_or(CommitError::InvalidCatalog)?;
    let epoch = u64::from_le_bytes(
        field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if epoch == 0 {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(epoch)
}

fn grant_authority_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 12 || key.len() != 59 {
        return Err(CommitError::InvalidCatalog);
    }
    let field = record
        .fields
        .iter()
        .find(|field| field.tag == 2)
        .ok_or(CommitError::InvalidCatalog)?;
    let epoch = u64::from_le_bytes(
        field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if epoch == 0 {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(epoch)
}

fn principal_authority_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 10 || key.len() != 19 {
        return Err(CommitError::InvalidCatalog);
    }
    let field = record
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or(CommitError::InvalidCatalog)?;
    let epoch = u64::from_le_bytes(
        field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if epoch == 0 {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(epoch)
}

fn decode_index_definition(key: &[u8], bytes: &[u8]) -> Result<([u8; 16], [u8; 16]), CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 9 || key.len() != 19 {
        return Err(CommitError::InvalidCatalog);
    }
    let index_id = key[3..19]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let table_id = record
        .fields
        .iter()
        .find(|field| field.tag == 1)
        .ok_or(CommitError::InvalidCatalog)?
        .payload
        .as_slice()
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    Ok((index_id, table_id))
}

fn load_index_definitions(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(IndexTableMap, IndexTableMap), CommitError> {
    let prefix = 9u16.to_be_bytes();
    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let mut current = HashMap::new();
    for entry in table.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&prefix) {
            continue;
        }
        let (index_id, table_id) = decode_index_definition(&key, value.value())?;
        if current.insert(index_id, table_id).is_some() {
            return Err(CommitError::InvalidCatalog);
        }
    }
    let mut final_definitions = current.clone();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
    {
        let index_id: [u8; 16] = write
            .key
            .get(3..19)
            .ok_or(CommitError::InvalidCatalog)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        match &write.payload {
            Some(payload) => {
                let (_, table_id) = decode_index_definition(&write.key, payload)?;
                final_definitions.insert(index_id, table_id);
            }
            None => {
                final_definitions.remove(&index_id);
            }
        }
    }
    Ok((current, final_definitions))
}

fn validate_index_definition_changes(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
    row_writes: &[RowWrite],
    backfill: Option<&IndexDdlPlan>,
) -> Result<(), CommitError> {
    let family_prefix = 9u16.to_be_bytes();
    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    let rows = transaction.open_table(ROWS).map_err(storage_error)?;
    let indexes = transaction.open_table(INDEXES).map_err(storage_error)?;

    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&family_prefix))
    {
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() {
            continue;
        }

        let index_id: [u8; 16] = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let planned_create = current.is_none()
            && write.payload.is_some()
            && backfill.is_some_and(|plan| plan.created_indexes.contains(&index_id));
        let planned_drop = current.is_some()
            && write.payload.is_none()
            && backfill.is_some_and(|plan| plan.dropped_indexes.contains(&index_id));
        let planned_rebuild = current.is_some()
            && write.payload.is_some()
            && backfill.is_some_and(|plan| plan.rebuilt_indexes.contains(&index_id));
        let mut affected_tables = HashSet::new();
        if let Some(current) = current.as_deref() {
            affected_tables.insert(decode_index_definition(&write.key, current)?.1);
        }
        if let Some(next) = write.payload.as_deref() {
            affected_tables.insert(decode_index_definition(&write.key, next)?.1);
        }

        for table_id in affected_tables {
            let has_persisted_rows = if let Some(entry) = rows
                .range(table_id.as_slice()..)
                .map_err(storage_error)?
                .next()
            {
                let (key, _) = entry.map_err(storage_error)?;
                let key = key.value();
                if key.starts_with(&table_id) {
                    if key.len() != 32 {
                        return Err(CommitError::InvalidRow);
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if row_writes
                .iter()
                .any(|row| row.key[..16] == table_id && row.payload.is_some())
            {
                return Err(CommitError::InvalidCatalog);
            }
            let planned_for_table = backfill.is_some_and(|plan| {
                plan.index_tables.get(&index_id) == Some(&table_id)
                    && (planned_create || planned_drop || planned_rebuild)
            });
            if has_persisted_rows && !planned_for_table {
                return Err(CommitError::InvalidCatalog);
            }
        }

        if let Some(entry) = indexes
            .range(index_id.as_slice()..)
            .map_err(storage_error)?
            .next()
        {
            let (key, _) = entry.map_err(storage_error)?;
            if key.value().starts_with(&index_id) && !(planned_drop || planned_rebuild) {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

fn decode_index_ddl_definition(
    key: &[u8],
    bytes: &[u8],
    schema: &FinalTableSchema,
) -> Result<IndexDdlDefinition, CommitError> {
    let (index_id, table_id) = decode_index_definition(key, bytes)?;
    let record = decode_catalog_record(key, bytes)?;
    let column_ids = catalog_id_list_field(key, bytes, 9, 2)?;
    let unique = match catalog_field_bytes(key, bytes, 9, 4)?.as_slice() {
        [0] => false,
        [1] => true,
        _ => return Err(CommitError::InvalidCatalog),
    };
    let storage_object_id: [u8; 16] = catalog_field_bytes(key, bytes, 9, 7)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let schema_version = u64::from_le_bytes(
        catalog_field_bytes(key, bytes, 9, 10)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    let owner_is_invalid = record
        .fields
        .iter()
        .find(|field| field.tag == 6)
        .is_some_and(
            |field| match <[u8; 16]>::try_from(field.payload.as_slice()) {
                Ok(owner) => owner == [0; 16],
                Err(_) => true,
            },
        );
    let has_owner = record.fields.iter().any(|field| field.tag == 6);
    let mut seen_columns = HashSet::new();
    if index_id == [0; 16]
        || table_id == [0; 16]
        || column_ids.is_empty()
        || column_ids.len() > 256
        || column_ids.iter().any(|id| {
            *id == [0; 16] || !seen_columns.insert(*id) || !schema.column_ids.contains(id)
        })
        || u16::from_le_bytes(
            catalog_field_bytes(key, bytes, 9, 3)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ) != 1
        || u16::from_le_bytes(
            catalog_field_bytes(key, bytes, 9, 5)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ) != 1
        || owner_is_invalid
        || (has_owner && !unique)
        || storage_object_id == [0; 16]
        || u16::from_le_bytes(
            catalog_field_bytes(key, bytes, 9, 8)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ) != 1
        || u32::from_le_bytes(
            catalog_field_bytes(key, bytes, 9, 9)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ) != 1
        || schema_version != schema.schema_version
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(IndexDdlDefinition {
        index_id,
        table_id,
        column_ids,
        unique,
    })
}

fn map_index_key_error(error: crate::index_key::DeriveIndexKeyError) -> CommitError {
    match error {
        crate::index_key::DeriveIndexKeyError::InvalidDefinition => CommitError::InvalidCatalog,
        crate::index_key::DeriveIndexKeyError::Tuple(_) => CommitError::InvalidRow,
    }
}

fn validate_final_catalog_index_definitions(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&9u16.to_be_bytes()))
    {
        let Some(payload) = write.payload.as_deref() else {
            continue;
        };
        if write.expected_value.as_deref() == Some(payload) {
            continue;
        }
        let (_, table_id) = decode_index_definition(&write.key, payload)?;
        let schema = load_final_table_schema(&catalog, catalog_writes, table_id)?;
        decode_index_ddl_definition(&write.key, payload, &schema)?;
    }
    Ok(())
}

fn build_index_ddl_plan(
    database: &Database,
    catalog_writes: &[CatalogWrite],
) -> Result<IndexDdlPlan, CommitError> {
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut new_indexes = Vec::new();
    let mut dropped_indexes = Vec::new();
    let mut updated_indexes = Vec::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&9u16.to_be_bytes()))
    {
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current != write.expected_value {
            return Err(CommitError::Conflict);
        }
        if current.is_none()
            && let Some(payload) = write.payload.as_deref()
        {
            let (_, table_id) = decode_index_definition(&write.key, payload)?;
            new_indexes.push((write.key.clone(), payload.to_vec(), table_id));
        } else if let (Some(current), None) = (current.as_deref(), write.payload.as_deref()) {
            let (index_id, table_id) = decode_index_definition(&write.key, current)?;
            dropped_indexes.push((index_id, table_id));
        } else if let (Some(current), Some(payload)) =
            (current.as_deref(), write.payload.as_deref())
            && current != payload
        {
            let (old_index_id, old_table_id) = decode_index_definition(&write.key, current)?;
            let (new_index_id, new_table_id) = decode_index_definition(&write.key, payload)?;
            if old_index_id != new_index_id || old_table_id != new_table_id {
                return Err(CommitError::InvalidCatalog);
            }
            updated_indexes.push((
                old_index_id,
                old_table_id,
                write.key.clone(),
                current.to_vec(),
                payload.to_vec(),
            ));
        }
    }

    let mut indexes_by_table = HashMap::<[u8; 16], Vec<(Vec<u8>, Vec<u8>)>>::new();
    for (key, payload, table_id) in new_indexes {
        indexes_by_table
            .entry(table_id)
            .or_default()
            .push((key, payload));
    }
    let mut plan = IndexDdlPlan {
        index_tables: HashMap::new(),
        created_indexes: HashSet::new(),
        dropped_indexes: HashSet::new(),
        rebuilt_indexes: HashSet::new(),
        expected_rows: HashMap::new(),
        row_versions: HashMap::new(),
        writes: Vec::new(),
    };
    let mut total_rows = 0usize;
    let mut total_entries = 0usize;
    let mut total_bytes = 0usize;
    let mut seen_index_ids = HashSet::new();
    let rows = read.open_table(ROWS).map_err(storage_error)?;

    for (table_id, definitions) in indexes_by_table {
        for (key, bytes) in &definitions {
            let (index_id, definition_table) = decode_index_definition(key, bytes)?;
            if definition_table != table_id || !seen_index_ids.insert(index_id) {
                return Err(CommitError::InvalidCatalog);
            }
            plan.index_tables.insert(index_id, definition_table);
            plan.created_indexes.insert(index_id);
        }
        let mut persisted_rows = Vec::new();
        for entry in rows.range(table_id.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&table_id) {
                break;
            }
            if key.len() != 32 {
                return Err(CommitError::InvalidRow);
            }
            total_rows = total_rows
                .checked_add(1)
                .ok_or(CommitError::IndexDdlLimitExceeded)?;
            if total_rows > MAX_INDEX_DDL_ROWS_PER_COMMIT {
                return Err(CommitError::IndexDdlLimitExceeded);
            }
            let bytes = value.value().to_vec();
            total_bytes = total_bytes
                .checked_add(bytes.len())
                .ok_or(CommitError::IndexDdlLimitExceeded)?;
            if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                return Err(CommitError::IndexDdlLimitExceeded);
            }
            let row = RowRecord::decode(&bytes).map_err(|_| CommitError::InvalidRow)?;
            let row_key: [u8; 32] = key.try_into().map_err(|_| CommitError::InvalidRow)?;
            if row_key[16..] == [0; 16] {
                return Err(CommitError::InvalidRow);
            }
            persisted_rows.push((row_key, row));
        }
        if persisted_rows.is_empty() {
            plan.expected_rows.insert(table_id, Vec::new());
            continue;
        }
        let mut schema_changed = false;
        let table_key = id_catalog_key(5, table_id);
        for write in catalog_writes {
            let family = write
                .key
                .get(..2)
                .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
                .map(u16::from_be_bytes);
            if family == Some(5) && write.key == table_key && write.expected_value != write.payload
            {
                schema_changed = true;
            } else if family == Some(6) && write.expected_value != write.payload {
                for bytes in [write.expected_value.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    if column_table_id(&write.key, bytes)? == table_id {
                        schema_changed = true;
                    }
                }
            }
        }
        if schema_changed {
            return Err(CommitError::InvalidCatalog);
        }

        let schema = load_final_table_schema(&catalog, &[], table_id)?;
        let definitions = definitions
            .iter()
            .map(|(key, bytes)| decode_index_ddl_definition(key, bytes, &schema))
            .collect::<Result<Vec<_>, _>>()?;

        let mut expected_rows = Vec::with_capacity(persisted_rows.len());
        for (row_key, row) in persisted_rows {
            expected_rows.push((row_key, row.row_version));
            plan.row_versions.insert(row_key, row.row_version);
            for definition in &definitions {
                total_entries = total_entries
                    .checked_add(1)
                    .ok_or(CommitError::IndexDdlLimitExceeded)?;
                if total_entries > MAX_INDEX_DDL_ENTRIES_PER_COMMIT {
                    return Err(CommitError::IndexDdlLimitExceeded);
                }
                let derived = crate::index_key::derive_index_key(
                    definition.index_id,
                    row_key[16..].try_into().unwrap(),
                    schema.schema_version,
                    &schema.column_ids,
                    &schema.columns,
                    &row.payload,
                    &definition.column_ids,
                )
                .map_err(|error| match error {
                    crate::index_key::DeriveIndexKeyError::InvalidDefinition => {
                        CommitError::InvalidCatalog
                    }
                    crate::index_key::DeriveIndexKeyError::Tuple(_) => CommitError::InvalidRow,
                })?;
                total_bytes = total_bytes
                    .checked_add(derived.key.len())
                    .ok_or(CommitError::IndexDdlLimitExceeded)?;
                if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                    return Err(CommitError::IndexDdlLimitExceeded);
                }
                plan.writes.push(IndexWrite {
                    key: derived.key,
                    row_key,
                    expected_version: None,
                    present: true,
                    unique: definition.unique,
                    null_exempt: definition.unique && derived.has_null,
                });
                plan.index_tables
                    .insert(definition.index_id, definition.table_id);
            }
        }
        plan.expected_rows.insert(table_id, expected_rows);
    }

    let indexes = read.open_table(INDEXES).map_err(storage_error)?;
    let reverse_indexes = read.open_table(ROW_INDEXES).map_err(storage_error)?;
    for (index_id, table_id, key, old_bytes, new_bytes) in updated_indexes {
        if !seen_index_ids.insert(index_id) {
            return Err(CommitError::InvalidCatalog);
        }
        let mut persisted_rows = Vec::new();
        for entry in rows.range(table_id.as_slice()..).map_err(storage_error)? {
            let (row_key, value) = entry.map_err(storage_error)?;
            let row_key = row_key.value();
            if !row_key.starts_with(&table_id) {
                break;
            }
            if row_key.len() != 32 {
                return Err(CommitError::InvalidRow);
            }
            total_rows = total_rows
                .checked_add(1)
                .ok_or(CommitError::IndexDdlLimitExceeded)?;
            if total_rows > MAX_INDEX_DDL_ROWS_PER_COMMIT {
                return Err(CommitError::IndexDdlLimitExceeded);
            }
            let bytes = value.value().to_vec();
            total_bytes = total_bytes
                .checked_add(bytes.len())
                .ok_or(CommitError::IndexDdlLimitExceeded)?;
            if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                return Err(CommitError::IndexDdlLimitExceeded);
            }
            let row = RowRecord::decode(&bytes).map_err(|_| CommitError::InvalidRow)?;
            let row_key: [u8; 32] = row_key.try_into().map_err(|_| CommitError::InvalidRow)?;
            if row_key[16..] == [0; 16] {
                return Err(CommitError::InvalidRow);
            }
            persisted_rows.push((row_key, row));
        }
        if persisted_rows.is_empty() {
            continue;
        }
        let table_key = id_catalog_key(5, table_id);
        for write in catalog_writes {
            let family = write
                .key
                .get(..2)
                .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
                .map(u16::from_be_bytes);
            if family == Some(5) && write.key == table_key && write.expected_value != write.payload
            {
                return Err(CommitError::InvalidCatalog);
            }
            if family == Some(6) && write.expected_value != write.payload {
                for bytes in [write.expected_value.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    if column_table_id(&write.key, bytes)? == table_id {
                        return Err(CommitError::InvalidCatalog);
                    }
                }
            }
        }
        let schema = load_final_table_schema(&catalog, &[], table_id)?;
        let old_definition = decode_index_ddl_definition(&key, &old_bytes, &schema)?;
        let new_definition = decode_index_ddl_definition(&key, &new_bytes, &schema)?;
        plan.index_tables.insert(index_id, table_id);
        plan.rebuilt_indexes.insert(index_id);
        let mut expected_rows = Vec::with_capacity(persisted_rows.len());
        for (row_key, row) in persisted_rows {
            expected_rows.push((row_key, row.row_version));
            plan.row_versions.insert(row_key, row.row_version);
            let row_id = row_key[16..].try_into().unwrap();
            let old_key = crate::index_key::derive_index_key(
                old_definition.index_id,
                row_id,
                schema.schema_version,
                &schema.column_ids,
                &schema.columns,
                &row.payload,
                &old_definition.column_ids,
            )
            .map_err(map_index_key_error)?;
            let new_key = crate::index_key::derive_index_key(
                new_definition.index_id,
                row_id,
                schema.schema_version,
                &schema.column_ids,
                &schema.columns,
                &row.payload,
                &new_definition.column_ids,
            )
            .map_err(map_index_key_error)?;
            let locator_bytes = indexes
                .get(old_key.key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidIndex)?;
            let locator = IndexLocator::decode_for_key(&old_key.key, &locator_bytes)
                .map_err(|_| CommitError::InvalidIndex)?;
            if locator.row_version != row.row_version {
                return Err(CommitError::InvalidIndex);
            }
            let reverse_key = reverse_index_key(&row_key, &old_key.key);
            let marker = reverse_indexes
                .get(&reverse_key[..])
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidIndex)?;
            if !marker.value().is_empty() {
                return Err(CommitError::InvalidIndex);
            }
            if old_key.key == new_key.key {
                total_entries = total_entries
                    .checked_add(1)
                    .ok_or(CommitError::IndexDdlLimitExceeded)?;
                total_bytes = total_bytes
                    .checked_add(new_key.key.len())
                    .and_then(|size| size.checked_add(locator_bytes.len()))
                    .ok_or(CommitError::IndexDdlLimitExceeded)?;
                plan.writes.push(IndexWrite {
                    key: new_key.key,
                    row_key,
                    expected_version: Some(row.row_version),
                    present: true,
                    unique: new_definition.unique,
                    null_exempt: new_definition.unique && new_key.has_null,
                });
            } else {
                total_entries = total_entries
                    .checked_add(2)
                    .ok_or(CommitError::IndexDdlLimitExceeded)?;
                total_bytes = total_bytes
                    .checked_add(old_key.key.len())
                    .and_then(|size| size.checked_add(new_key.key.len()))
                    .and_then(|size| size.checked_add(locator_bytes.len()))
                    .ok_or(CommitError::IndexDdlLimitExceeded)?;
                plan.writes.push(IndexWrite {
                    key: old_key.key,
                    row_key,
                    expected_version: Some(row.row_version),
                    present: false,
                    unique: old_definition.unique,
                    null_exempt: false,
                });
                plan.writes.push(IndexWrite {
                    key: new_key.key,
                    row_key,
                    expected_version: None,
                    present: true,
                    unique: new_definition.unique,
                    null_exempt: new_definition.unique && new_key.has_null,
                });
            }
            if total_entries > MAX_INDEX_DDL_ENTRIES_PER_COMMIT
                || total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT
            {
                return Err(CommitError::IndexDdlLimitExceeded);
            }
        }
        plan.expected_rows.insert(table_id, expected_rows);
    }
    for (index_id, table_id) in dropped_indexes {
        if !seen_index_ids.insert(index_id) {
            return Err(CommitError::InvalidCatalog);
        }
        plan.index_tables.insert(index_id, table_id);
        plan.dropped_indexes.insert(index_id);
        for entry in indexes
            .range(index_id.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&index_id) {
                break;
            }
            if key.len() < 33 {
                return Err(CommitError::InvalidIndex);
            }
            total_entries = total_entries
                .checked_add(1)
                .ok_or(CommitError::IndexDdlLimitExceeded)?;
            if total_entries > MAX_INDEX_DDL_ENTRIES_PER_COMMIT {
                return Err(CommitError::IndexDdlLimitExceeded);
            }
            let locator_bytes = value.value().to_vec();
            total_bytes = total_bytes
                .checked_add(key.len())
                .and_then(|size| size.checked_add(locator_bytes.len()))
                .ok_or(CommitError::IndexDdlLimitExceeded)?;
            if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                return Err(CommitError::IndexDdlLimitExceeded);
            }
            let locator = IndexLocator::decode_for_key(&key, &locator_bytes)
                .map_err(|_| CommitError::InvalidIndex)?;
            let mut row_key = [0; 32];
            row_key[..16].copy_from_slice(&table_id);
            row_key[16..].copy_from_slice(&locator.row_id);
            let row = rows
                .get(&row_key[..])
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidIndex)?;
            let row = RowRecord::decode(row.value()).map_err(|_| CommitError::InvalidRow)?;
            if row.row_version != locator.row_version
                || plan
                    .row_versions
                    .insert(row_key, row.row_version)
                    .is_some_and(|version| version != row.row_version)
            {
                return Err(CommitError::InvalidIndex);
            }
            let reverse_key = reverse_index_key(&row_key, &key);
            let reverse_marker = reverse_indexes
                .get(&reverse_key[..])
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidIndex)?;
            if !reverse_marker.value().is_empty() {
                return Err(CommitError::InvalidIndex);
            }
            plan.writes.push(IndexWrite {
                key,
                row_key,
                expected_version: Some(locator.row_version),
                present: false,
                unique: false,
                null_exempt: false,
            });
        }
    }
    plan.writes.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(plan)
}

fn validate_index_ddl_snapshot(
    transaction: &WriteTransaction,
    plan: &IndexDdlPlan,
) -> Result<(), CommitError> {
    let rows = transaction.open_table(ROWS).map_err(storage_error)?;
    let mut total_rows = 0usize;
    let mut total_bytes = 0usize;
    for (table_id, expected_rows) in &plan.expected_rows {
        let mut actual_rows = Vec::with_capacity(expected_rows.len());
        for entry in rows.range(table_id.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(table_id) {
                break;
            }
            if key.len() != 32 {
                return Err(CommitError::InvalidRow);
            }
            total_rows = total_rows.checked_add(1).ok_or(CommitError::Conflict)?;
            total_bytes = total_bytes
                .checked_add(value.value().len())
                .ok_or(CommitError::Conflict)?;
            if total_rows > MAX_INDEX_DDL_ROWS_PER_COMMIT
                || total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT
            {
                return Err(CommitError::Conflict);
            }
            let row = RowRecord::decode(value.value()).map_err(|_| CommitError::InvalidRow)?;
            actual_rows.push((
                key.try_into().map_err(|_| CommitError::InvalidRow)?,
                row.row_version,
            ));
        }
        if &actual_rows != expected_rows {
            return Err(CommitError::Conflict);
        }
    }
    let indexes = transaction.open_table(INDEXES).map_err(storage_error)?;
    let reverse_indexes = transaction.open_table(ROW_INDEXES).map_err(storage_error)?;
    let mut total_entries = 0usize;
    let mut revalidated_indexes = plan
        .dropped_indexes
        .union(&plan.rebuilt_indexes)
        .copied()
        .collect::<Vec<_>>();
    revalidated_indexes.sort_unstable();
    for index_id in &revalidated_indexes {
        let mut expected_writes = plan.writes.iter().filter(|write| {
            write.expected_version.is_some() && write.key.get(..16) == Some(index_id.as_slice())
        });
        let table_id = plan
            .index_tables
            .get(index_id)
            .ok_or(CommitError::InvalidCatalog)?;
        for entry in indexes
            .range(index_id.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(index_id) {
                break;
            }
            if key.len() < 33 {
                return Err(CommitError::InvalidIndex);
            }
            let locator_bytes = value.value().to_vec();
            total_entries = total_entries.checked_add(1).ok_or(CommitError::Conflict)?;
            total_bytes = total_bytes
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(locator_bytes.len()))
                .ok_or(CommitError::Conflict)?;
            if total_entries > MAX_INDEX_DDL_ENTRIES_PER_COMMIT
                || total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT
            {
                return Err(CommitError::Conflict);
            }
            let locator = IndexLocator::decode_for_key(&key, &locator_bytes)
                .map_err(|_| CommitError::InvalidIndex)?;
            let Some(expected_write) = expected_writes.next() else {
                return Err(CommitError::Conflict);
            };
            if expected_write.key != key
                || expected_write.expected_version != Some(locator.row_version)
            {
                return Err(CommitError::Conflict);
            }
            let mut row_key = [0; 32];
            row_key[..16].copy_from_slice(table_id);
            row_key[16..].copy_from_slice(&locator.row_id);
            let row = rows
                .get(&row_key[..])
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidIndex)?;
            let row = RowRecord::decode(row.value()).map_err(|_| CommitError::InvalidRow)?;
            if row.row_version != locator.row_version
                || plan.row_versions.get(&row_key) != Some(&row.row_version)
            {
                return Err(CommitError::Conflict);
            }
            let reverse_key = reverse_index_key(&row_key, &key);
            let marker = reverse_indexes
                .get(&reverse_key[..])
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidIndex)?;
            if !marker.value().is_empty() {
                return Err(CommitError::InvalidIndex);
            }
        }
        if expected_writes.next().is_some() {
            return Err(CommitError::Conflict);
        }
    }
    Ok(())
}

fn reverse_index_key(row_key: &[u8; 32], index_key: &[u8]) -> Vec<u8> {
    let mut key = row_key.to_vec();
    key.extend_from_slice(&index_key[..index_key.len() - 16]);
    key
}

fn retained_index_mutations(
    transaction: &WriteTransaction,
    row_writes: &[RowWrite],
    explicit_index_writes: &[IndexWrite],
    current_definitions: &HashMap<[u8; 16], [u8; 16]>,
    final_definitions: &HashMap<[u8; 16], [u8; 16]>,
    next_sequence: u64,
) -> Result<Vec<RetainedIndexMutation>, CommitError> {
    let row_writes = row_writes
        .iter()
        .map(|write| (write.key, write))
        .collect::<HashMap<_, _>>();
    let explicit_keys = explicit_index_writes
        .iter()
        .map(|write| write.key.as_slice())
        .collect::<HashSet<_>>();
    let reverse = transaction.open_table(ROW_INDEXES).map_err(storage_error)?;
    let indexes = transaction.open_table(INDEXES).map_err(storage_error)?;
    let rows = transaction.open_table(ROWS).map_err(storage_error)?;
    let mut mutations = Vec::new();

    for (row_key, row_write) in &row_writes {
        for entry in reverse.range(row_key.as_slice()..).map_err(storage_error)? {
            let (stored_reverse_key, marker) = entry.map_err(storage_error)?;
            let stored_reverse_key = stored_reverse_key.value().to_vec();
            if !stored_reverse_key.starts_with(&row_key[..]) {
                break;
            }
            if stored_reverse_key.len() < 49 || !marker.value().is_empty() {
                return Err(CommitError::InvalidIndex);
            }
            let mut index_key = stored_reverse_key[32..].to_vec();
            index_key.extend_from_slice(&row_key[16..]);
            let index_id: [u8; 16] = index_key[..16]
                .try_into()
                .map_err(|_| CommitError::InvalidIndex)?;
            let table_id = current_definitions
                .get(&index_id)
                .ok_or(CommitError::InvalidIndex)?;
            if table_id != &row_key[..16] {
                return Err(CommitError::InvalidIndex);
            }
            let row = rows
                .get(&row_key[..])
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidIndex)?;
            let row = RowRecord::decode(row.value()).map_err(|_| CommitError::InvalidRow)?;
            if row_write.expected_version != Some(row.row_version) {
                return Err(CommitError::InvalidIndex);
            }
            let locator = indexes
                .get(&index_key[..])
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidIndex)?;
            let locator = IndexLocator::decode_for_key(&index_key, locator.value())
                .map_err(|_| CommitError::InvalidIndex)?;
            if locator.row_version != row.row_version {
                return Err(CommitError::InvalidIndex);
            }
            if explicit_keys.contains(index_key.as_slice()) {
                continue;
            }

            let remove =
                row_write.payload.is_none() || final_definitions.get(&index_id) != Some(table_id);
            let next_locator = (!remove).then(|| {
                IndexLocator {
                    row_id: locator.row_id,
                    row_version: next_sequence,
                }
                .encode()
            });
            mutations.push(RetainedIndexMutation {
                index_key,
                row_index_key: stored_reverse_key,
                locator: next_locator,
            });
        }
    }
    Ok(mutations)
}

/// Atomically publish a row write set and commit sequence without index deltas.
/// This low-level helper does not derive index changes from typed row values or
/// enforce authorization; it is not an executor-facing write path.
pub fn commit_rows(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    writes: &[RowWrite],
) -> Result<u64, CommitError> {
    commit_storage_changes(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows: writes,
            indexes: &[],
            foreign_keys: &[],
            catalog_guards: &[],
            catalog: &[],
            audit: &[],
        },
    )
}

/// Atomically publish rows and explicit logical index-key mutations. The writer
/// validates index/table mapping and uniqueness, maintains reverse membership,
/// and refreshes retained locator versions. It does not derive a complete index
/// delta from typed row values or enforce authorization, so it is not an
/// executor-facing write path.
pub fn commit_rows_and_indexes(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    writes: &[RowWrite],
    index_writes: &[IndexWrite],
) -> Result<u64, CommitError> {
    commit_storage_changes(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows: writes,
            indexes: index_writes,
            foreign_keys: &[],
            catalog_guards: &[],
            catalog: &[],
            audit: &[],
        },
    )
}

/// Publish every record family participating in one operation through one
/// Immediate/two-phase redb transaction. This low-level function does not
/// acquire DDL fences; coordinated runtime paths hold the affected shared or
/// exclusive guards before calling it.
pub fn commit_storage_changes(
    database: &Database,
    request: CommitRequest<'_>,
) -> Result<u64, CommitError> {
    commit_storage_changes_inner(database, request, None)
}

fn commit_storage_changes_with_index_ddl(
    database: &Database,
    request: CommitRequest<'_>,
    backfill: &IndexDdlPlan,
) -> Result<u64, CommitError> {
    commit_storage_changes_inner(database, request, Some(backfill))
}

fn commit_storage_changes_inner(
    database: &Database,
    request: CommitRequest<'_>,
    index_ddl_plan: Option<&IndexDdlPlan>,
) -> Result<u64, CommitError> {
    let CommitRequest {
        invocation_id,
        principal_id,
        retention_deadline_us,
        rows: writes,
        indexes: index_writes,
        foreign_keys,
        catalog_guards,
        catalog: catalog_writes,
        audit: audit_events,
    } = request;
    let mut unique = HashSet::with_capacity(writes.len());
    for write in writes {
        if write.key[..16] == [0; 16] {
            return Err(CommitError::InvalidWrite);
        }
        if !unique.insert(write.key) {
            return Err(CommitError::DuplicateKey);
        }
        if write.expected_version.is_none() && write.payload.is_none() {
            return Err(CommitError::InvalidWrite);
        }
    }
    let rows_by_key: std::collections::HashMap<_, _> =
        writes.iter().map(|write| (write.key, write)).collect();
    let mut unique_indexes = HashSet::with_capacity(index_writes.len());
    for index_write in index_writes {
        let row_write = rows_by_key.get(&index_write.row_key);
        let is_index_ddl_write = index_ddl_plan.is_some_and(|plan| {
            let index_id: Option<[u8; 16]> =
                index_write.key.get(..16).and_then(|id| id.try_into().ok());
            index_id
                .and_then(|id| plan.index_tables.get(&id).copied().map(|table| (id, table)))
                .is_some_and(|(index_id, table_id)| {
                    table_id == index_write.row_key[..16]
                        && (if index_write.present {
                            (index_write.expected_version.is_none()
                                && plan.created_indexes.contains(&index_id))
                                || plan.rebuilt_indexes.contains(&index_id)
                        } else {
                            index_write.expected_version.is_some()
                                && (plan.dropped_indexes.contains(&index_id)
                                    || plan.rebuilt_indexes.contains(&index_id))
                        })
                        && plan.writes.contains(index_write)
                })
        });
        if index_write.key.len() < 33
            || index_write.key[index_write.key.len() - 16..] != index_write.row_key[16..]
            || (row_write.is_none() && !is_index_ddl_write)
            || !unique_indexes.insert(index_write.key.as_slice())
            || (index_write.present && row_write.is_some_and(|write| write.payload.is_none()))
            || (!index_write.present && index_write.expected_version.is_none())
        {
            return Err(CommitError::InvalidIndex);
        }
        if let Some(expected_version) = index_write.expected_version
            && expected_version == 0
        {
            return Err(CommitError::InvalidIndex);
        }
    }
    let mut unique_catalog = HashSet::with_capacity(catalog_writes.len());
    for write in catalog_writes {
        if !unique_catalog.insert(write.key.as_slice())
            || (write.expected_value.is_none() && write.payload.is_none())
            || (write.payload.is_some()
                && write.key.len() >= 2
                && u16::from_be_bytes([write.key[0], write.key[1]]) == 16)
        {
            return Err(CommitError::InvalidCatalog);
        }
        for bytes in [write.expected_value.as_ref(), write.payload.as_ref()]
            .into_iter()
            .flatten()
        {
            decode_catalog_record(&write.key, bytes)?;
        }
    }
    let mut unique_guards = HashSet::with_capacity(catalog_guards.len());
    for guard in catalog_guards {
        if !unique_guards.insert(guard.key.as_slice()) {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(expected) = &guard.expected_value {
            decode_catalog_record(&guard.key, expected)?;
        }
    }
    let mut unique_audit = HashSet::with_capacity(audit_events.len());
    let mut encoded_audit = Vec::with_capacity(audit_events.len());
    for event in audit_events {
        let bytes = event.encode().map_err(|_| CommitError::InvalidAudit)?;
        if !unique_audit.insert(event.event_id) {
            return Err(CommitError::InvalidAudit);
        }
        encoded_audit.push((event.event_id, bytes));
    }
    if (!catalog_writes.is_empty() || !audit_events.is_empty()) && encoded_audit.is_empty() {
        return Err(CommitError::InvalidAudit);
    }
    if !catalog_writes.is_empty()
        && !audit_events.iter().any(|event| {
            matches!(
                event.action,
                crate::audit::AuditAction::Ddl
                    | crate::audit::AuditAction::AuthorityChange
                    | crate::audit::AuditAction::TokenLifecycle
            ) && event.result == crate::audit::AuditResult::Succeeded
                && event.actor_id == Some(principal_id)
        })
    {
        return Err(CommitError::InvalidAudit);
    }

    let mut transaction = database.begin_write().map_err(storage_error)?;
    transaction
        .set_durability(redb::Durability::Immediate)
        .map_err(storage_error)?;
    transaction.set_two_phase_commit(true);

    let current_metadata = {
        let table = transaction.open_table(ENGINE_META).map_err(storage_error)?;
        let value = table
            .get(&b"format"[..])
            .map_err(storage_error)?
            .ok_or(CommitError::InvalidMetadata)?
            .value()
            .to_vec();
        EngineMetadata::decode(&value).map_err(|_| CommitError::InvalidMetadata)?
    };
    if !current_metadata.supports_current_formats() {
        return Err(CommitError::UnsupportedFormat);
    }
    if current_metadata.lifecycle != Lifecycle::Ready {
        return Err(CommitError::NotReady);
    }
    let next_sequence = current_metadata
        .commit_sequence
        .checked_add(1)
        .ok_or(CommitError::SequenceExhausted)?;
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id,
        principal_id,
        commit_sequence: next_sequence,
        retention_deadline_us,
    }
    .encode()
    .map_err(|_| CommitError::InvalidWrite)?;
    {
        let table = transaction.open_table(OUTCOMES).map_err(storage_error)?;
        if table
            .get(&invocation_id[..])
            .map_err(storage_error)?
            .is_some()
        {
            return Err(CommitError::OutcomeAlreadyExists);
        }
    }

    let mut conflict = false;
    {
        let table = transaction.open_table(ROWS).map_err(storage_error)?;
        for write in writes {
            let current = table
                .get(&write.key[..])
                .map_err(storage_error)?
                .map(|value| RowRecord::decode(value.value()).map(|row| row.row_version))
                .transpose()
                .map_err(|_| CommitError::InvalidRow)?;
            if current != write.expected_version {
                conflict = true;
                break;
            }
        }
    }
    if conflict {
        transaction.abort().map_err(storage_error)?;
        record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        return Err(CommitError::Conflict);
    }

    if let Some(plan) = index_ddl_plan {
        match validate_index_ddl_snapshot(&transaction, plan) {
            Ok(()) => {}
            Err(CommitError::Conflict) => {
                transaction.abort().map_err(storage_error)?;
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
                return Err(CommitError::Conflict);
            }
            Err(
                error @ (CommitError::InvalidRow
                | CommitError::InvalidIndex
                | CommitError::InvalidCatalog
                | CommitError::Storage(_)),
            ) => {
                transaction.abort().map_err(storage_error)?;
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
                return Err(error);
            }
            Err(error) => return Err(error),
        }
    }

    let mut index_conflict = false;
    {
        let table = transaction.open_table(INDEXES).map_err(storage_error)?;
        let reverse_table = transaction.open_table(ROW_INDEXES).map_err(storage_error)?;
        for write in index_writes {
            let current = table
                .get(&write.key[..])
                .map_err(storage_error)?
                .map(|value| {
                    IndexLocator::decode_for_key(&write.key, value.value())
                        .map(|locator| locator.row_version)
                })
                .transpose()
                .map_err(|_| CommitError::InvalidIndex)?;
            if current != write.expected_version {
                index_conflict = true;
                break;
            }
            let reverse_key = reverse_index_key(&write.row_key, &write.key);
            let reverse_exists = reverse_table
                .get(&reverse_key[..])
                .map_err(storage_error)?
                .is_some();
            if current.is_some() != reverse_exists {
                return Err(CommitError::InvalidIndex);
            }
        }
    }
    if index_conflict {
        transaction.abort().map_err(storage_error)?;
        record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        return Err(CommitError::Conflict);
    }

    let mut unique_violation = false;
    {
        let table = transaction.open_table(INDEXES).map_err(storage_error)?;
        let mut unique_prefixes = HashMap::<Vec<u8>, ([u8; 16], HashSet<[u8; 16]>)>::new();
        for addition in index_writes
            .iter()
            .filter(|write| write.present && write.unique && !write.null_exempt)
        {
            let prefix = &addition.key[..addition.key.len() - 16];
            let (table_id, row_ids) = unique_prefixes
                .entry(prefix.to_vec())
                .or_insert_with(|| (addition.row_key[..16].try_into().unwrap(), HashSet::new()));
            if *table_id != addition.row_key[..16]
                || !row_ids.insert(addition.row_key[16..].try_into().unwrap())
                || row_ids.len() > 1
            {
                unique_violation = true;
                break;
            }
        }
        if !unique_violation && !unique_prefixes.is_empty() {
            let removals = index_writes
                .iter()
                .filter(|write| !write.present)
                .map(|write| write.key.as_slice())
                .collect::<HashSet<_>>();
            'prefixes: for (prefix, (table_id, row_ids)) in &unique_prefixes {
                for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
                    let (stored_key, stored_value) = entry.map_err(storage_error)?;
                    let stored_key = stored_key.value().to_vec();
                    if !stored_key.starts_with(prefix) {
                        break;
                    }
                    if stored_key.len() < 33 {
                        return Err(CommitError::InvalidIndex);
                    }
                    let row_id: [u8; 16] = stored_key[stored_key.len() - 16..]
                        .try_into()
                        .map_err(|_| CommitError::InvalidIndex)?;
                    if row_ids.contains(&row_id) || removals.contains(stored_key.as_slice()) {
                        continue;
                    }
                    let mut candidate_row_key = [0; 32];
                    candidate_row_key[..16].copy_from_slice(table_id);
                    candidate_row_key[16..].copy_from_slice(&row_id);
                    if rows_by_key
                        .get(&candidate_row_key)
                        .is_some_and(|write| write.payload.is_none())
                    {
                        continue;
                    }
                    IndexLocator::decode_for_key(&stored_key, stored_value.value())
                        .map_err(|_| CommitError::InvalidIndex)?;
                    unique_violation = true;
                    break 'prefixes;
                }
            }
        }
    }
    if unique_violation {
        transaction.abort().map_err(storage_error)?;
        record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        return Err(CommitError::UniqueViolation);
    }

    let mut catalog_conflict = false;
    {
        let table = transaction.open_table(CATALOG).map_err(storage_error)?;
        for guard in catalog_guards {
            let current = table
                .get(&guard.key[..])
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if current.as_deref() != guard.expected_value.as_deref() {
                catalog_conflict = true;
                break;
            }
        }
        for write in catalog_writes {
            if catalog_conflict {
                break;
            }
            let current = table
                .get(&write.key[..])
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if let Some(bytes) = &current {
                decode_catalog_record(&write.key, bytes)?;
            }
            if current != write.expected_value {
                catalog_conflict = true;
                break;
            }
        }
    }
    if catalog_conflict {
        transaction.abort().map_err(storage_error)?;
        record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        return Err(CommitError::Conflict);
    }
    match validate_foreign_key_constraints(&transaction, writes, foreign_keys, false) {
        Ok(()) => {}
        Err(
            error @ (CommitError::InvalidCatalog
            | CommitError::InvalidRow
            | CommitError::ForeignKeyViolation
            | CommitError::ForeignKeyValidationLimitExceeded),
        ) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    match validate_final_membership_graph(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_dependency_edges(
        &transaction,
        catalog_writes,
        current_metadata.database_id,
    ) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_object_owners(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_principal_names(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_typed_object_mappings(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_bootstrap_builtin_targets(
        &transaction,
        catalog_writes,
        current_metadata.database_id,
    ) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    let (current_index_definitions, final_index_definitions) =
        load_index_definitions(&transaction, catalog_writes)?;
    match validate_final_catalog_index_definitions(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(error @ (CommitError::InvalidCatalog | CommitError::InvalidRow)) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    match validate_index_definition_changes(&transaction, catalog_writes, writes, index_ddl_plan) {
        Ok(()) => {}
        Err(error @ (CommitError::InvalidCatalog | CommitError::InvalidRow)) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    let new_foreign_keys = {
        let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
        load_new_foreign_key_checks(&catalog, catalog_writes)
    };
    let new_foreign_keys = match new_foreign_keys {
        Ok(checks) => checks,
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    };
    match validate_foreign_key_constraints(&transaction, writes, &new_foreign_keys, true) {
        Ok(()) => {}
        Err(
            error @ (CommitError::InvalidCatalog
            | CommitError::InvalidRow
            | CommitError::ForeignKeyViolation
            | CommitError::ForeignKeyValidationLimitExceeded),
        ) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    for write in index_writes {
        let index_id: [u8; 16] = write.key[..16]
            .try_into()
            .map_err(|_| CommitError::InvalidIndex)?;
        let definitions = if write.present {
            &final_index_definitions
        } else {
            &current_index_definitions
        };
        let table_id: [u8; 16] = write.row_key[..16]
            .try_into()
            .map_err(|_| CommitError::InvalidIndex)?;
        if definitions.get(&index_id) != Some(&table_id) {
            return Err(CommitError::InvalidIndex);
        }
    }
    let retained_index_mutations = retained_index_mutations(
        &transaction,
        writes,
        index_writes,
        &current_index_definitions,
        &final_index_definitions,
        next_sequence,
    )?;
    {
        let table = transaction.open_table(AUDIT).map_err(storage_error)?;
        for (event_id, _) in &encoded_audit {
            if table.get(&event_id[..]).map_err(storage_error)?.is_some() {
                return Err(CommitError::AuditAlreadyExists);
            }
        }
    }

    {
        let mut table = transaction.open_table(ROWS).map_err(storage_error)?;
        for write in writes {
            match &write.payload {
                Some(payload) => {
                    let record = RowRecord {
                        row_version: next_sequence,
                        payload: payload.clone(),
                    }
                    .encode()
                    .map_err(|_| CommitError::InvalidWrite)?;
                    table
                        .insert(&write.key[..], &record[..])
                        .map_err(storage_error)?;
                }
                None => {
                    table.remove(&write.key[..]).map_err(storage_error)?;
                }
            }
        }
    }
    {
        let mut table = transaction.open_table(INDEXES).map_err(storage_error)?;
        let mut reverse_table = transaction.open_table(ROW_INDEXES).map_err(storage_error)?;
        for mutation in &retained_index_mutations {
            match &mutation.locator {
                Some(locator) => {
                    table
                        .insert(&mutation.index_key[..], &locator[..])
                        .map_err(storage_error)?;
                }
                None => {
                    table
                        .remove(&mutation.index_key[..])
                        .map_err(storage_error)?;
                    reverse_table
                        .remove(&mutation.row_index_key[..])
                        .map_err(storage_error)?;
                }
            }
        }
        for write in index_writes {
            if write.present {
                let row_version = if rows_by_key.contains_key(&write.row_key) {
                    next_sequence
                } else {
                    index_ddl_plan
                        .and_then(|plan| plan.row_versions.get(&write.row_key).copied())
                        .ok_or(CommitError::InvalidIndex)?
                };
                let locator = IndexLocator {
                    row_id: write.row_key[16..].try_into().unwrap(),
                    row_version,
                }
                .encode();
                table
                    .insert(&write.key[..], &locator[..])
                    .map_err(storage_error)?;
                let reverse_key = reverse_index_key(&write.row_key, &write.key);
                reverse_table
                    .insert(&reverse_key[..], &b""[..])
                    .map_err(storage_error)?;
            } else {
                table.remove(&write.key[..]).map_err(storage_error)?;
                let reverse_key = reverse_index_key(&write.row_key, &write.key);
                reverse_table
                    .remove(&reverse_key[..])
                    .map_err(storage_error)?;
            }
        }
    }
    {
        let mut table = transaction.open_table(CATALOG).map_err(storage_error)?;
        for write in catalog_writes {
            match &write.payload {
                Some(payload) => {
                    table
                        .insert(&write.key[..], &payload[..])
                        .map_err(storage_error)?;
                }
                None => {
                    table.remove(&write.key[..]).map_err(storage_error)?;
                }
            }
        }
    }
    {
        let mut table = transaction.open_table(AUDIT).map_err(storage_error)?;
        for (event_id, payload) in &encoded_audit {
            table
                .insert(&event_id[..], &payload[..])
                .map_err(storage_error)?;
        }
    }

    let mut next_metadata = current_metadata;
    next_metadata.commit_sequence = next_sequence;
    let encoded_metadata = next_metadata
        .encode()
        .map_err(|_| CommitError::InvalidMetadata)?;
    {
        let mut table = transaction.open_table(ENGINE_META).map_err(storage_error)?;
        table
            .insert(&b"format"[..], &encoded_metadata[..])
            .map_err(storage_error)?;
    }
    {
        let mut table = transaction.open_table(OUTCOMES).map_err(storage_error)?;
        table
            .insert(&invocation_id[..], &outcome[..])
            .map_err(storage_error)?;
    }
    transaction.commit().map_err(storage_error)?;
    Ok(next_sequence)
}

/// Atomically publish catalog changes and required audit events. Membership
/// graphs, dependency endpoints, and changed constraint references are checked
/// at commit. This helper neither authorizes the actor nor acquires DDL fences;
/// it is trusted-only and must not be exposed directly to control clients.
pub fn commit_catalog_and_audit(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_storage_changes(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows: &[],
            indexes: &[],
            foreign_keys: &[],
            catalog_guards: &[],
            catalog: catalog_writes,
            audit: audit_events,
        },
    )
}

fn commit_catalog_and_audit_with_index_ddl(
    database: &Database,
    request: CatalogCommitRequest<'_>,
    catalog_guards: &[CatalogGuard],
    backfill: &IndexDdlPlan,
) -> Result<u64, CommitError> {
    let CatalogCommitRequest {
        invocation_id,
        principal_id,
        retention_deadline_us,
        catalog_writes,
        audit_events,
    } = request;
    if backfill.index_tables.is_empty() && catalog_guards.is_empty() {
        return commit_catalog_and_audit(
            database,
            invocation_id,
            principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        );
    }
    commit_storage_changes_with_index_ddl(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows: &[],
            indexes: &backfill.writes,
            foreign_keys: &[],
            catalog_guards,
            catalog: catalog_writes,
            audit: audit_events,
        },
        backfill,
    )
}

/// Trusted coordinated catalog publication that does not authorize the actor.
/// Use [`commit_authorized_index_ddl_with_fences`] for ordinary index DDL.
/// This path exclusively fences every affected table.
/// The same `DdlFenceCoordinator` must be passed to invocation commits through
/// `InvocationTransaction::begin_with_fences`.
pub fn commit_catalog_and_audit_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::Trusted,
    )
}

/// Authorize and publish ordinary index DDL for a principal that has already
/// been authenticated by the trusted administrative channel. This entry point
/// accepts index-definition and matching index-object records only. Other
/// catalog changes still require their own authorization rules.
pub fn commit_authorized_index_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::IndexDdl,
    )
}

/// Authorize and publish role-membership changes for a principal already
/// authenticated by the trusted administrative channel. `MANAGE_ROLES` is
/// sufficient except when the reserved database-administrator role is a
/// membership target, which requires an existing database administrator.
pub fn commit_authorized_membership_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::MembershipDdl,
    )
}

/// Authorize and publish database-scoped system privilege grant or revoke
/// operations. Only a current database administrator may use this route.
pub fn commit_authorized_system_privilege_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::SystemPrivilegeDdl,
    )
}

/// Authorize and publish object-scoped grant/revoke records. The actor must
/// own each target object, hold `MANAGE_GRANTS` on it, or be a database
/// administrator. System privileges use the separate admin-only route.
pub fn commit_authorized_object_grant_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::ObjectGrantDdl,
    )
}

fn commit_catalog_and_audit_with_fences_inner(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    request: CatalogCommitRequest<'_>,
    authorization_mode: CatalogAuthorizationMode,
) -> Result<u64, CommitError> {
    let CatalogCommitRequest {
        invocation_id,
        principal_id,
        retention_deadline_us,
        catalog_writes,
        audit_events,
        ..
    } = request;
    let affected_tables = match affected_table_ids_for_catalog_writes(database, catalog_writes) {
        Ok(tables) => tables,
        Err(error) => {
            if !matches!(error, CommitError::Storage(_)) {
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
            }
            return Err(error);
        }
    };
    let _fence_guard = match ddl_fences.acquire_exclusive(&affected_tables) {
        Ok(guard) => guard,
        Err(_) => {
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::DdlFenceUnavailable);
        }
    };
    let catalog_guards = match authorization_mode {
        CatalogAuthorizationMode::Trusted => Vec::new(),
        CatalogAuthorizationMode::IndexDdl => {
            match index_ddl_authority_guards(database, principal_id, catalog_writes) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::MembershipDdl => {
            match membership_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::SystemPrivilegeDdl => {
            match system_privilege_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::ObjectGrantDdl => {
            match object_grant_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
    };
    let backfill = match build_index_ddl_plan(database, catalog_writes) {
        Ok(plan) => plan,
        Err(error) => {
            if !matches!(error, CommitError::Storage(_)) {
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
            }
            return Err(error);
        }
    };
    commit_catalog_and_audit_with_index_ddl(database, request, &catalog_guards, &backfill)
}

fn index_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut changed_index_ids = HashSet::<[u8; 16]>::new();
    let mut index_definition_states = HashMap::<[u8; 16], (bool, bool)>::new();
    let mut affected_tables = HashSet::<[u8; 16]>::new();
    let mut changed_object_ids = HashSet::<[u8; 16]>::new();
    let mut object_record_states = HashMap::<[u8; 16], (bool, bool)>::new();
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() {
            continue;
        }
        match family {
            2 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                let object_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                changed_object_ids.insert(object_id);
                object_record_states
                    .insert(object_id, (current.is_some(), write.payload.is_some()));
            }
            9 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                let index_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if !changed_index_ids.insert(index_id) {
                    return Err(CommitError::InvalidCatalog);
                }
                index_definition_states
                    .insert(index_id, (current.is_some(), write.payload.is_some()));
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    let (decoded_id, table_id) = decode_index_definition(&write.key, bytes)?;
                    if decoded_id != index_id {
                        return Err(CommitError::InvalidCatalog);
                    }
                    affected_tables.insert(table_id);
                }
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if changed_index_ids.is_empty()
        || changed_index_ids.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
        || changed_object_ids
            .iter()
            .any(|object_id| !changed_index_ids.contains(object_id))
        || index_definition_states
            .iter()
            .any(|(index_id, state)| match state {
                (false, true) => object_record_states.get(index_id) != Some(&(false, true)),
                (true, false) => object_record_states.get(index_id) != Some(&(true, false)),
                (true, true) => object_record_states.contains_key(index_id),
                _ => true,
            })
    {
        return Err(CommitError::InvalidCatalog);
    }

    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    let principal_key = id_catalog_key(10, principal_id);
    let principal_bytes = catalog
        .get(principal_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    let principal_kind = decode_principal_kind(&principal_key, &principal_bytes)?;
    let principal_login = catalog_field_bytes(&principal_key, &principal_bytes, 10, 3)?;
    if principal_kind != 1 || principal_login != [1] {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    principal_authority_epoch(&principal_key, &principal_bytes)?;
    add_catalog_guard(&mut guards, principal_key, Some(principal_bytes));

    let mut memberships = Vec::new();
    let mut membership_count = 0usize;
    let membership_prefix = 11u16.to_be_bytes();
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        membership_count += 1;
        if membership_count > MAX_MEMBERSHIP_EDGES || key[2] != 1 || key[19] != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let membership = decode_membership_record(&key, &bytes)?;
        if membership.member_id == [0; 16]
            || membership.role_id == [0; 16]
            || catalog_field_bytes(&key, &bytes, 11, 1)? == [0; 16]
            || catalog_field_u64(&key, &bytes, 11, 2)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        memberships.push(membership);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let role_ids = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut authority_subjects = role_ids.clone();
    authority_subjects.insert(principal_id);
    let mut database_admin_role = [0x80; 16];
    database_admin_role[15] = 3;
    let is_database_admin = role_ids.contains(&database_admin_role);
    for role_id in &role_ids {
        let role_key = id_catalog_key(10, *role_id);
        let role_bytes = catalog
            .get(role_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if decode_principal_kind(&role_key, &role_bytes)? != 2
            || catalog_field_bytes(&role_key, &role_bytes, 10, 3)? != [0]
        {
            return Err(CommitError::InvalidCatalog);
        }
        principal_authority_epoch(&role_key, &role_bytes)?;
        add_catalog_guard(&mut guards, role_key, Some(role_bytes));
    }

    let mut table_owners = HashMap::new();
    for table_id in &affected_tables {
        if *table_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
        let object_key = id_catalog_key(2, *table_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&object_key, &object_bytes, 2, 1)? != 3 {
            return Err(CommitError::InvalidCatalog);
        }
        let owner_id: [u8; 16] = catalog_field_bytes(&object_key, &object_bytes, 2, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if owner_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
        table_owners.insert(*table_id, owner_id);
        add_catalog_guard(&mut guards, object_key, Some(object_bytes));
    }

    let mut granted_tables = HashSet::new();
    let grant_prefix = 12u16.to_be_bytes();
    let mut grant_count = 0usize;
    for entry in catalog
        .range(grant_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&grant_prefix) {
            break;
        }
        grant_count += 1;
        if grant_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN
            || key.len() != 59
            || [key[2], key[19], key[36], key[39], key[42]] != [1; 5]
        {
            return Err(CommitError::InvalidCatalog);
        }
        let grantee_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let object_id: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let privilege = u16::from_be_bytes([key[37], key[38]]);
        let scope = u16::from_be_bytes([key[40], key[41]]);
        let target_id: [u8; 16] = key[43..59]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if !authority_subjects.contains(&grantee_id)
            || privilege != 9
            || scope != 1
            || object_id != target_id
            || !affected_tables.contains(&target_id)
        {
            continue;
        }
        let bytes = value.value().to_vec();
        if catalog_field_u64(&key, &bytes, 12, 2)? == 0
            || catalog_field_bytes(&key, &bytes, 12, 1)? == [0; 16]
        {
            return Err(CommitError::InvalidCatalog);
        }
        granted_tables.insert(target_id);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }

    for (table_id, owner_id) in table_owners {
        if !is_database_admin
            && !authority_subjects.contains(&owner_id)
            && !granted_tables.contains(&table_id)
        {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }
    let mut guards = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    guards.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(guards)
}

fn membership_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    if audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::AuthorityChange
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(CommitError::InvalidAudit);
    }
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let principal_key = id_catalog_key(10, principal_id);
    let principal_bytes = catalog
        .get(principal_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&principal_key, &principal_bytes)? != 1
        || catalog_field_bytes(&principal_key, &principal_bytes, 10, 3)? != [1]
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    principal_authority_epoch(&principal_key, &principal_bytes)?;

    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, principal_key, Some(principal_bytes));
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut principal_ids = HashSet::new();
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        if key.len() != 36 || key[2] != 1 || key[19] != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let membership = decode_membership_record(&key, &bytes)?;
        if membership.member_id == [0; 16]
            || membership.role_id == [0; 16]
            || catalog_field_bytes(&key, &bytes, 11, 1)? == [0; 16]
            || catalog_field_u64(&key, &bytes, 11, 2)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        principal_ids.insert(membership.member_id);
        principal_ids.insert(membership.role_id);
        memberships.push(membership);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    principal_ids.insert(principal_id);
    for id in principal_ids {
        let key = id_catalog_key(10, id);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if catalog_field_u64(&key, &bytes, 10, 4)? == 0
            || (id == principal_id
                && (kind != 1 || catalog_field_bytes(&key, &bytes, 10, 3)? != [1]))
            || (memberships.iter().any(|edge| edge.role_id == id) && kind != 2)
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }

    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let is_database_admin = roles.contains(&admin_role_id);
    let mut authority_subjects = roles;
    authority_subjects.insert(principal_id);
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;
    let mut has_manage_roles = false;
    for subject in authority_subjects {
        let key = grant_key(subject, database_id, 13, 1, database_id);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if let Some(bytes) = &bytes {
            if catalog_field_bytes(&key, bytes, 12, 1)? == [0; 16]
                || catalog_field_u64(&key, bytes, 12, 2)? == 0
            {
                return Err(CommitError::InvalidCatalog);
            }
            has_manage_roles = true;
        }
        add_catalog_guard(&mut guards, key, bytes);
    }

    let mut membership_pairs = HashSet::new();
    let mut membership_updates = HashMap::new();
    let mut changed_members = HashSet::new();
    let mut dependency_pairs = HashSet::new();
    let mut principal_updates = HashSet::new();
    let mut targets_admin_role = false;
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() {
            continue;
        }
        match family {
            11 => {
                if write.key.len() != 36 || current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let pair = if let Some(bytes) = current.as_deref() {
                    decode_membership_record(&write.key, bytes)?
                } else {
                    decode_membership_record(
                        &write.key,
                        write
                            .payload
                            .as_deref()
                            .ok_or(CommitError::InvalidCatalog)?,
                    )?
                };
                if let Some(bytes) = &write.payload
                    && catalog_field_bytes(&write.key, bytes, 11, 1)? != principal_id
                {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
                if !membership_pairs.insert((pair.member_id, pair.role_id)) {
                    return Err(CommitError::InvalidCatalog);
                }
                membership_updates.insert((pair.member_id, pair.role_id), write.payload.is_some());
                changed_members.insert(pair.member_id);
                targets_admin_role |= pair.role_id == admin_role_id;
            }
            15 => {
                if current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let bytes = current
                    .as_deref()
                    .or(write.payload.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let (dependent_id, referenced_id, kind) =
                    decode_dependency_edge(&write.key, bytes)?;
                if kind != 5 {
                    return Err(CommitError::InvalidCatalog);
                }
                dependency_pairs.insert((dependent_id, referenced_id));
            }
            10 => {
                if write.key.len() != 19 || current.is_none() || write.payload.is_none() {
                    return Err(CommitError::InvalidCatalog);
                }
                let member_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let old = current.as_deref().ok_or(CommitError::InvalidCatalog)?;
                let next = write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                for tag in 1..=3 {
                    if catalog_field_bytes(&write.key, old, 10, tag)?
                        != catalog_field_bytes(&write.key, next, 10, tag)?
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                }
                if catalog_field_u64(&write.key, next, 10, 4)?
                    <= catalog_field_u64(&write.key, old, 10, 4)?
                {
                    return Err(CommitError::InvalidCatalog);
                }
                principal_updates.insert(member_id);
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if membership_pairs.is_empty()
        || membership_pairs != dependency_pairs
        || changed_members != principal_updates
    {
        return Err(CommitError::InvalidCatalog);
    }
    let current_edges = memberships
        .iter()
        .map(|edge| (edge.member_id, edge.role_id))
        .collect::<HashSet<_>>();
    let mut final_edges = current_edges.clone();
    for (pair, present) in membership_updates {
        if present {
            final_edges.insert(pair);
        } else {
            final_edges.remove(&pair);
        }
    }
    let current_admin_members = members_in_role_closure(&current_edges, admin_role_id);
    let final_admin_members = members_in_role_closure(&final_edges, admin_role_id);
    if !is_database_admin
        && (!has_manage_roles || targets_admin_role || current_admin_members != final_admin_members)
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let principal_prefix = 10u16.to_be_bytes();
    let mut principal_count = 0usize;
    let mut has_login_enabled_admin = false;
    for entry in catalog
        .range(principal_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&principal_prefix) {
            break;
        }
        principal_count += 1;
        if principal_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if key.len() != 19 || key[2] != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        let id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if final_admin_members.contains(&id)
            && decode_principal_kind(key, value.value())? == 1
            && catalog_field_bytes(key, value.value(), 10, 3)? == [1]
        {
            has_login_enabled_admin = true;
            break;
        }
    }
    if !has_login_enabled_admin {
        return Err(CommitError::DdlAuthorizationDenied);
    }

    let mut guards = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    guards.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(guards)
}

fn members_in_role_closure(
    memberships: &HashSet<([u8; 16], [u8; 16])>,
    target_role_id: [u8; 16],
) -> HashSet<[u8; 16]> {
    let mut members_by_role = HashMap::<[u8; 16], Vec<[u8; 16]>>::new();
    for (member_id, role_id) in memberships {
        members_by_role
            .entry(*role_id)
            .or_default()
            .push(*member_id);
    }
    let mut closure = HashSet::from([target_role_id]);
    let mut pending = vec![target_role_id];
    while let Some(role_id) = pending.pop() {
        for member_id in members_by_role.get(&role_id).into_iter().flatten() {
            if closure.insert(*member_id) {
                pending.push(*member_id);
            }
        }
    }
    closure
}

fn system_privilege_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16]
        || audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::AuthorityChange
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(if principal_id == [0; 16] {
            CommitError::DdlAuthorizationDenied
        } else {
            CommitError::InvalidAudit
        });
    }
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let principal_key = id_catalog_key(10, principal_id);
    let principal_bytes = catalog
        .get(principal_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&principal_key, &principal_bytes)? != 1
        || catalog_field_bytes(&principal_key, &principal_bytes, 10, 3)? != [1]
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    principal_authority_epoch(&principal_key, &principal_bytes)?;
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, principal_key, Some(principal_bytes));

    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut principal_ids = HashSet::new();
    let mut membership_count = 0usize;
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        membership_count += 1;
        if membership_count > MAX_MEMBERSHIP_EDGES || key.len() != 36 || key[2] != 1 || key[19] != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let membership = decode_membership_record(&key, &bytes)?;
        if membership.member_id == [0; 16]
            || membership.role_id == [0; 16]
            || catalog_field_bytes(&key, &bytes, 11, 1)? == [0; 16]
            || catalog_field_u64(&key, &bytes, 11, 2)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        principal_ids.insert(membership.member_id);
        principal_ids.insert(membership.role_id);
        memberships.push(membership);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    principal_ids.insert(principal_id);
    for id in principal_ids {
        let key = id_catalog_key(10, id);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if catalog_field_u64(&key, &bytes, 10, 4)? == 0
            || (id == principal_id
                && (kind != 1 || catalog_field_bytes(&key, &bytes, 10, 3)? != [1]))
            || (memberships.iter().any(|edge| edge.role_id == id) && kind != 2)
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    if !roles.contains(&admin_role_id) {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;

    let mut changed_grantees = HashSet::new();
    let mut changed_authority_pairs = HashSet::new();
    let mut dependency_pairs = HashSet::new();
    let mut principal_updates = HashSet::new();
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() {
            continue;
        }
        match family {
            12 => {
                if write.key.len() != 59
                    || [
                        write.key[2],
                        write.key[19],
                        write.key[36],
                        write.key[39],
                        write.key[42],
                    ] != [1; 5]
                    || current.is_some() == write.payload.is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let grantee_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let object_id: [u8; 16] = write.key[20..36]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let privilege = u16::from_be_bytes([write.key[37], write.key[38]]);
                let scope = u16::from_be_bytes([write.key[40], write.key[41]]);
                let target_id: [u8; 16] = write.key[43..59]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if !matches!(privilege, 12..=14)
                    || scope != 1
                    || object_id != database_id
                    || target_id != database_id
                    || grantee_id == [0; 16]
                {
                    return Err(CommitError::InvalidCatalog);
                }
                if let Some(bytes) = &write.payload
                    && (catalog_field_bytes(&write.key, bytes, 12, 1)? != principal_id
                        || catalog_field_u64(&write.key, bytes, 12, 2)? == 0)
                {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
                changed_grantees.insert(grantee_id);
                changed_authority_pairs.insert((grantee_id, database_id));
            }
            15 => {
                if current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let bytes = current
                    .as_deref()
                    .or(write.payload.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let (dependent_id, referenced_id, kind) =
                    decode_dependency_edge(&write.key, bytes)?;
                if kind != 5 {
                    return Err(CommitError::InvalidCatalog);
                }
                dependency_pairs.insert((dependent_id, referenced_id));
            }
            10 => {
                if write.key.len() != 19 || current.is_none() || write.payload.is_none() {
                    return Err(CommitError::InvalidCatalog);
                }
                let grantee_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let old = current.as_deref().ok_or(CommitError::InvalidCatalog)?;
                let next = write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                for tag in 1..=3 {
                    if catalog_field_bytes(&write.key, old, 10, tag)?
                        != catalog_field_bytes(&write.key, next, 10, tag)?
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                }
                if catalog_field_u64(&write.key, next, 10, 4)?
                    <= catalog_field_u64(&write.key, old, 10, 4)?
                {
                    return Err(CommitError::InvalidCatalog);
                }
                principal_updates.insert(grantee_id);
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if changed_grantees.is_empty()
        || changed_grantees != principal_updates
        || dependency_pairs
            .iter()
            .any(|pair| !changed_authority_pairs.contains(pair))
    {
        return Err(CommitError::InvalidCatalog);
    }
    let mut guards = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    guards.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(guards)
}

fn object_grant_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16]
        || audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::AuthorityChange
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(if principal_id == [0; 16] {
            CommitError::DdlAuthorizationDenied
        } else {
            CommitError::InvalidAudit
        });
    }
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let principal_key = id_catalog_key(10, principal_id);
    let principal_bytes = catalog
        .get(principal_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&principal_key, &principal_bytes)? != 1
        || catalog_field_bytes(&principal_key, &principal_bytes, 10, 3)? != [1]
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    principal_authority_epoch(&principal_key, &principal_bytes)?;
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, principal_key, Some(principal_bytes));

    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut principal_ids = HashSet::new();
    let mut membership_count = 0usize;
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        membership_count += 1;
        if membership_count > MAX_MEMBERSHIP_EDGES || key.len() != 36 || key[2] != 1 || key[19] != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let membership = decode_membership_record(&key, &bytes)?;
        if membership.member_id == [0; 16]
            || membership.role_id == [0; 16]
            || catalog_field_bytes(&key, &bytes, 11, 1)? == [0; 16]
            || catalog_field_u64(&key, &bytes, 11, 2)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        principal_ids.insert(membership.member_id);
        principal_ids.insert(membership.role_id);
        memberships.push(membership);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    principal_ids.insert(principal_id);
    for id in principal_ids {
        let key = id_catalog_key(10, id);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if catalog_field_u64(&key, &bytes, 10, 4)? == 0
            || (id == principal_id
                && (kind != 1 || catalog_field_bytes(&key, &bytes, 10, 3)? != [1]))
            || (memberships.iter().any(|edge| edge.role_id == id) && kind != 2)
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let is_database_admin = roles.contains(&admin_role_id);
    let mut authority_subjects = roles;
    authority_subjects.insert(principal_id);

    let mut changed_grantees = HashSet::new();
    let mut changed_objects = HashSet::new();
    let mut changed_authority_pairs = HashSet::new();
    let mut dependency_pairs = HashSet::new();
    let mut principal_updates = HashSet::new();
    let mut object_owners = HashMap::<[u8; 16], [u8; 16]>::new();
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() {
            continue;
        }
        match family {
            12 => {
                if write.key.len() != 59
                    || [
                        write.key[2],
                        write.key[19],
                        write.key[36],
                        write.key[39],
                        write.key[42],
                    ] != [1; 5]
                    || current.is_some() == write.payload.is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let grantee_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let object_id: [u8; 16] = write.key[20..36]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let privilege = u16::from_be_bytes([write.key[37], write.key[38]]);
                let scope = u16::from_be_bytes([write.key[40], write.key[41]]);
                let target_id: [u8; 16] = write.key[43..59]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if !(1..=11).contains(&privilege)
                    || grantee_id == [0; 16]
                    || object_id == [0; 16]
                    || target_id == [0; 16]
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let object_key = id_catalog_key(2, object_id);
                let object_bytes = catalog
                    .get(object_key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or(CommitError::InvalidCatalog)?;
                let object_kind = catalog_field_u16(&object_key, &object_bytes, 2, 1)?;
                let owner_id: [u8; 16] = catalog_field_bytes(&object_key, &object_bytes, 2, 4)?
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let valid_scope = match privilege {
                    1 | 7 => scope == 1 && object_kind == 1 && target_id == object_id,
                    2..=4 | 6 => {
                        object_kind == 3
                            && ((scope == 1 && target_id == object_id)
                                || (scope == 2 && target_id != object_id))
                    }
                    5 => scope == 1 && object_kind == 3 && target_id == object_id,
                    8..=11 => scope == 1 && matches!(object_kind, 1 | 3) && target_id == object_id,
                    _ => false,
                };
                if !valid_scope || owner_id == [0; 16] {
                    return Err(CommitError::InvalidCatalog);
                }
                if let Some(bytes) = &write.payload
                    && (catalog_field_bytes(&write.key, bytes, 12, 1)? != principal_id
                        || catalog_field_u64(&write.key, bytes, 12, 2)? == 0)
                {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
                changed_grantees.insert(grantee_id);
                changed_objects.insert(object_id);
                changed_authority_pairs.insert((grantee_id, target_id));
                object_owners.insert(object_id, owner_id);
                add_catalog_guard(&mut guards, object_key, Some(object_bytes));
            }
            15 => {
                if current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let bytes = current
                    .as_deref()
                    .or(write.payload.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let (dependent_id, referenced_id, kind) =
                    decode_dependency_edge(&write.key, bytes)?;
                if kind != 5 {
                    return Err(CommitError::InvalidCatalog);
                }
                dependency_pairs.insert((dependent_id, referenced_id));
            }
            10 => {
                if write.key.len() != 19 || current.is_none() || write.payload.is_none() {
                    return Err(CommitError::InvalidCatalog);
                }
                let grantee_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let old = current.as_deref().ok_or(CommitError::InvalidCatalog)?;
                let next = write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                for tag in 1..=3 {
                    if catalog_field_bytes(&write.key, old, 10, tag)?
                        != catalog_field_bytes(&write.key, next, 10, tag)?
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                }
                if catalog_field_u64(&write.key, next, 10, 4)?
                    <= catalog_field_u64(&write.key, old, 10, 4)?
                {
                    return Err(CommitError::InvalidCatalog);
                }
                principal_updates.insert(grantee_id);
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if changed_grantees.is_empty()
        || changed_grantees != principal_updates
        || dependency_pairs
            .iter()
            .any(|pair| !changed_authority_pairs.contains(pair))
    {
        return Err(CommitError::InvalidCatalog);
    }
    for object_id in changed_objects {
        let owner_id = object_owners
            .get(&object_id)
            .copied()
            .ok_or(CommitError::InvalidCatalog)?;
        if is_database_admin || authority_subjects.contains(&owner_id) {
            continue;
        }
        let mut has_manage_grants = false;
        for subject in &authority_subjects {
            let key = grant_key(*subject, object_id, 11, 1, object_id);
            let bytes = catalog
                .get(key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if let Some(bytes) = &bytes {
                if catalog_field_bytes(&key, bytes, 12, 1)? == [0; 16]
                    || catalog_field_u64(&key, bytes, 12, 2)? == 0
                {
                    return Err(CommitError::InvalidCatalog);
                }
                has_manage_grants = true;
            }
            add_catalog_guard(&mut guards, key, bytes);
        }
        if !has_manage_grants {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }
    let mut guards = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    guards.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(guards)
}

fn grant_key(
    grantee_id: [u8; 16],
    object_id: [u8; 16],
    privilege: u16,
    target_scope: u16,
    target_id: [u8; 16],
) -> Vec<u8> {
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
    key
}

fn decode_dependency_edge(
    key: &[u8],
    bytes: &[u8],
) -> Result<([u8; 16], [u8; 16], u16), CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 15 || key.len() != 39 || key[2] != 1 || key[19] != 1 || key[36] != 1 {
        return Err(CommitError::InvalidCatalog);
    }
    let dependent_id = key[3..19]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let referenced_id = key[20..36]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let kind = u16::from_be_bytes([key[37], key[38]]);
    if dependent_id == [0; 16] || referenced_id == [0; 16] {
        return Err(CommitError::InvalidCatalog);
    }
    Ok((dependent_id, referenced_id, kind))
}

fn add_catalog_guard(
    guards: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
    key: Vec<u8>,
    expected_value: Option<Vec<u8>>,
) {
    guards.entry(key).or_insert(expected_value);
}

fn affected_table_ids_for_catalog_writes(
    database: &Database,
    writes: &[CatalogWrite],
) -> Result<Vec<[u8; 16]>, CommitError> {
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut affected = HashSet::new();
    let mut changed_expressions = HashSet::new();
    for write in writes {
        let family = u16::from_be_bytes(
            write
                .key
                .get(..2)
                .ok_or(CommitError::InvalidCatalog)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        match family {
            5 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                affected.insert(
                    write.key[3..19]
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                );
            }
            6 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    affected.insert(column_table_id(&write.key, bytes)?);
                }
            }
            8 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    affected.insert(constraint_table_id(&write.key, bytes)?);
                    if u16::from_le_bytes(
                        catalog_field_bytes(&write.key, bytes, 8, 2)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    ) == 5
                    {
                        let target_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 5)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                        let target_key = id_catalog_key(8, target_id);
                        if let Some(target_bytes) =
                            read_final_catalog_value(&catalog, writes, &target_key)?
                        {
                            affected.insert(constraint_table_id(&target_key, &target_bytes)?);
                        }
                    }
                }
            }
            9 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    affected.insert(decode_index_definition(&write.key, bytes)?.1);
                }
            }
            13 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    affected.insert(policy_table_id(&write.key, bytes)?);
                }
            }
            14 => {
                if write.key.len() != 39 {
                    return Err(CommitError::InvalidCatalog);
                }
                let policy_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let policy_key = id_catalog_key(13, policy_id);
                if let Some(policy_bytes) = read_final_catalog_value(&catalog, writes, &policy_key)?
                {
                    affected.insert(policy_table_id(&policy_key, &policy_bytes)?);
                }
            }
            7 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                changed_expressions.insert(
                    write.key[3..19]
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                );
            }
            _ => {}
        }
    }
    if !changed_expressions.is_empty() {
        add_expression_dependent_tables(&catalog, writes, &changed_expressions, &mut affected)?;
    }
    let mut affected = affected.into_iter().collect::<Vec<_>>();
    affected.sort_unstable();
    Ok(affected)
}

fn read_final_catalog_value(
    catalog: &ReadOnlyTable<&[u8], &[u8]>,
    writes: &[CatalogWrite],
    key: &[u8],
) -> Result<Option<Vec<u8>>, CommitError> {
    if let Some(write) = writes.iter().find(|write| write.key == key) {
        return Ok(write.payload.clone());
    }
    Ok(catalog
        .get(key)
        .map_err(storage_error)?
        .map(|value| value.value().to_vec()))
}

fn add_expression_dependent_tables(
    catalog: &ReadOnlyTable<&[u8], &[u8]>,
    writes: &[CatalogWrite],
    expression_ids: &HashSet<[u8; 16]>,
    affected: &mut HashSet<[u8; 16]>,
) -> Result<(), CommitError> {
    for family in [6u16, 8, 13] {
        let prefix = family.to_be_bytes();
        let tags: &[u16] = match family {
            6 => &[7, 10],
            8 => &[4],
            13 => &[5, 6],
            _ => unreachable!(),
        };
        let mut count = 0usize;
        for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if writes.iter().any(|write| write.key == key) {
                continue;
            }
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            if catalog_record_references_expression(
                key,
                value.value(),
                family,
                tags,
                expression_ids,
            )? {
                affected.insert(match family {
                    6 => column_table_id(key, value.value())?,
                    8 => constraint_table_id(key, value.value())?,
                    13 => policy_table_id(key, value.value())?,
                    _ => unreachable!(),
                });
            }
        }
        for write in writes.iter().filter(|write| write.key.starts_with(&prefix)) {
            let Some(bytes) = write.payload.as_deref() else {
                continue;
            };
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            if catalog_record_references_expression(
                &write.key,
                bytes,
                family,
                tags,
                expression_ids,
            )? {
                affected.insert(match family {
                    6 => column_table_id(&write.key, bytes)?,
                    8 => constraint_table_id(&write.key, bytes)?,
                    13 => policy_table_id(&write.key, bytes)?,
                    _ => unreachable!(),
                });
            }
        }
    }
    Ok(())
}

fn catalog_record_references_expression(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tags: &[u16],
    expression_ids: &HashSet<[u8; 16]>,
) -> Result<bool, CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.family_code != family || record.logical_key != key {
        return Err(CommitError::InvalidCatalog);
    }
    for field in record
        .fields
        .iter()
        .filter(|field| tags.contains(&field.tag))
    {
        let id: [u8; 16] = field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if expression_ids.contains(&id) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn record_aborted_outcome(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
) -> Result<(), CommitError> {
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Aborted,
        invocation_id,
        principal_id,
        commit_sequence: 0,
        retention_deadline_us,
    }
    .encode()
    .map_err(|_| CommitError::InvalidWrite)?;
    let mut transaction = database.begin_write().map_err(storage_error)?;
    transaction
        .set_durability(redb::Durability::Immediate)
        .map_err(storage_error)?;
    transaction.set_two_phase_commit(true);
    {
        let table = transaction.open_table(ENGINE_META).map_err(storage_error)?;
        let metadata = table
            .get(&b"format"[..])
            .map_err(storage_error)?
            .ok_or(CommitError::InvalidMetadata)?;
        let metadata =
            EngineMetadata::decode(metadata.value()).map_err(|_| CommitError::InvalidMetadata)?;
        if !metadata.supports_current_formats() {
            return Err(CommitError::UnsupportedFormat);
        }
        if metadata.lifecycle != Lifecycle::Ready {
            return Err(CommitError::NotReady);
        }
    }
    {
        let table = transaction.open_table(OUTCOMES).map_err(storage_error)?;
        if table
            .get(&invocation_id[..])
            .map_err(storage_error)?
            .is_some()
        {
            return Err(CommitError::OutcomeAlreadyExists);
        }
    }
    {
        let mut table = transaction.open_table(OUTCOMES).map_err(storage_error)?;
        table
            .insert(&invocation_id[..], &outcome[..])
            .map_err(storage_error)?;
    }
    transaction.commit().map_err(storage_error)
}
