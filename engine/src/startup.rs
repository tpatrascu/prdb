//! Read-only integrity checks for a recovered redb image.

use crate::audit::AuditEvent;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, Membership, validate_membership_graph};
use crate::catalog::{
    CatalogRecord, decode_builtin_scalar_type, decode_registered_envelope,
    validate_builtin_type_profile, validate_owned_child_graph,
};
use crate::credentials::{CredentialVerifier, decode_verifier_record};
use crate::expression::decode_catalog_expression_record;
use crate::index_key::{DerivedIndexKey, derive_index_key, encode_sql_tuple};
use crate::index_locator::IndexLocator;
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::outcome::{InvocationOutcome, OutcomeStatus};
use crate::row_record::RowRecord;
use crate::schema::{AUDIT, CATALOG, ENGINE_META, INDEXES, OUTCOMES, ROW_INDEXES, ROWS, STATS};
use crate::tuple::{ColumnSchema, decode_tuple};
use redb::{Database, ReadTransaction, ReadableDatabase, ReadableTable};
use std::collections::{HashMap, HashSet};

const METADATA_KEY: &[u8] = b"format";
const MAX_CATALOG_RECORDS_PER_FAMILY: usize = 65_536;
const MAX_FOREIGN_KEYS_PER_DATABASE: usize = 256;
const MAX_FOREIGN_KEY_ROW_VISITS_PER_STARTUP: usize = 65_536;
type IndexDefinitions = HashMap<[u8; 16], [u8; 16]>;
type IndexColumns = HashMap<[u8; 16], Vec<[u8; 16]>>;
type UniqueIndexIds = HashSet<[u8; 16]>;

#[derive(Clone, Debug)]
struct PersistedTableSchema {
    schema_version: u64,
    column_ids: Vec<[u8; 16]>,
    columns: Vec<ColumnSchema>,
}

#[derive(Clone, Debug)]
struct PersistedColumnSchema {
    column_id: [u8; 16],
    ordinal: u32,
    type_id: [u8; 16],
    parameters: Vec<u8>,
    nullable: bool,
    introduced_schema_version: u64,
}

type PersistedTableSchemas = HashMap<[u8; 16], PersistedTableSchema>;

#[derive(Clone, Debug)]
struct PersistedForeignKey {
    source_table_id: [u8; 16],
    source_ordinals: Vec<usize>,
    target_index_id: [u8; 16],
    comparison_columns: Vec<ColumnSchema>,
}

struct PersistedCatalog {
    record_count: usize,
    index_definitions: IndexDefinitions,
    index_columns: IndexColumns,
    unique_index_ids: UniqueIndexIds,
    table_schemas: PersistedTableSchemas,
    foreign_keys: Vec<PersistedForeignKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageHealthReport {
    pub metadata: EngineMetadata,
    pub catalog_records: usize,
    pub rows: usize,
    pub index_entries: usize,
    pub outcomes: usize,
    pub audit_events: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupError {
    Storage,
    InvalidMetadata,
    UnsupportedFormat,
    NotReady,
    UnexpectedMetadataKey,
    CatalogLimit,
    InvalidCatalog,
    InvalidRow,
    InvalidForeignKey,
    ForeignKeyValidationLimitExceeded,
    SequenceAhead,
    InvalidIndex,
    InvalidOutcome,
    InvalidAudit,
    CountOverflow,
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "storage startup validation failed: {self:?}")
    }
}

impl std::error::Error for StartupError {}

/// Validate the recovered image's table set, version state, bootstrap mapping,
/// record envelopes, and implemented references against one pinned redb
/// snapshot. This check does not certify authenticated service admission.
pub fn validate_persisted_records(
    database: &Database,
) -> Result<StorageHealthReport, StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    validate_tables(&read)?;
    let metadata = read_metadata(&read)?;
    if !metadata.supports_current_formats() {
        return Err(StartupError::UnsupportedFormat);
    }
    if metadata.lifecycle != Lifecycle::Ready {
        return Err(StartupError::NotReady);
    }

    let catalog = validate_catalog(&read, &metadata)?;
    let rows = validate_rows(&read, metadata.commit_sequence, &catalog.table_schemas)?;
    let index_entries = validate_indexes(
        &read,
        metadata.commit_sequence,
        &catalog.index_definitions,
        &catalog.index_columns,
        &catalog.unique_index_ids,
        &catalog.table_schemas,
    )?;
    validate_foreign_key_rows(&read, &catalog.foreign_keys, &catalog.table_schemas)?;
    let outcomes = validate_outcomes(&read, metadata.commit_sequence)?;
    let audit_events = validate_audit(&read)?;

    Ok(StorageHealthReport {
        metadata,
        catalog_records: catalog.record_count,
        rows,
        index_entries,
        outcomes,
        audit_events,
    })
}

fn storage_error(_error: impl std::fmt::Display) -> StartupError {
    StartupError::Storage
}

fn insert_expression_reference_edge(
    edges: &mut std::collections::HashSet<([u8; 16], [u8; 16])>,
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
) -> Result<(), StartupError> {
    edges.insert((dependent_id, referenced_id));
    if edges.len() > MAX_CATALOG_RECORDS_PER_FAMILY {
        return Err(StartupError::InvalidCatalog);
    }
    Ok(())
}

fn insert_authority_edge(
    edges: &mut std::collections::HashSet<([u8; 16], [u8; 16])>,
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
) -> Result<(), StartupError> {
    edges.insert((dependent_id, referenced_id));
    if edges.len() > MAX_CATALOG_RECORDS_PER_FAMILY {
        return Err(StartupError::InvalidCatalog);
    }
    Ok(())
}

fn validate_tables(read: &ReadTransaction) -> Result<(), StartupError> {
    drop(read.open_table(ENGINE_META).map_err(storage_error)?);
    drop(read.open_table(ROWS).map_err(storage_error)?);
    drop(read.open_table(INDEXES).map_err(storage_error)?);
    drop(read.open_table(ROW_INDEXES).map_err(storage_error)?);
    drop(read.open_table(CATALOG).map_err(storage_error)?);
    drop(read.open_table(OUTCOMES).map_err(storage_error)?);
    drop(read.open_table(AUDIT).map_err(storage_error)?);
    drop(read.open_table(STATS).map_err(storage_error)?);
    Ok(())
}

fn read_metadata(read: &ReadTransaction) -> Result<EngineMetadata, StartupError> {
    let table = read.open_table(ENGINE_META).map_err(storage_error)?;
    let mut metadata_bytes = None;
    for entry in table.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        if key.value() != METADATA_KEY {
            return Err(StartupError::UnexpectedMetadataKey);
        }
        if metadata_bytes.replace(value.value().to_vec()).is_some() {
            return Err(StartupError::InvalidMetadata);
        }
    }
    let bytes = metadata_bytes.ok_or(StartupError::InvalidMetadata)?;
    EngineMetadata::decode(&bytes).map_err(|_| StartupError::InvalidMetadata)
}

fn validate_catalog(
    read: &ReadTransaction,
    metadata: &EngineMetadata,
) -> Result<PersistedCatalog, StartupError> {
    let table = read.open_table(CATALOG).map_err(storage_error)?;
    let mut family_counts = [0usize; 17];
    let mut count = 0usize;
    let mut index_definitions = HashMap::new();
    let mut builtin_mappings = None;
    let mut principal_kinds = HashMap::<[u8; 16], u16>::new();
    let mut principal_login_enabled = HashMap::<[u8; 16], bool>::new();
    let mut credential_verifiers = Vec::<CredentialVerifier>::new();
    let mut principal_names = HashMap::<[u8; 16], Vec<u8>>::new();
    let mut principal_name_ids = HashMap::<Vec<u8>, [u8; 16]>::new();
    let mut object_kinds = HashMap::<[u8; 16], u16>::new();
    let mut object_owners = Vec::<[u8; 16]>::new();
    let mut typed_object_kinds = HashMap::<[u8; 16], u16>::new();
    let mut builtin_type_kinds = HashMap::<[u8; 16], u16>::new();
    let mut builtin_type_ids = HashMap::<u16, [u8; 16]>::new();
    let mut type_records = HashMap::<[u8; 16], CatalogRecord>::new();
    let mut table_schema_versions = HashMap::<[u8; 16], u64>::new();
    let mut columns_by_table = HashMap::<[u8; 16], Vec<PersistedColumnSchema>>::new();
    let mut column_tables = HashMap::<[u8; 16], [u8; 16]>::new();
    let mut column_nullable = HashMap::<[u8; 16], bool>::new();
    let mut index_columns = HashMap::<[u8; 16], Vec<[u8; 16]>>::new();
    let mut unique_index_ids = HashSet::new();
    let mut policy_tables = HashMap::<[u8; 16], [u8; 16]>::new();
    let mut policy_kinds = HashMap::<[u8; 16], u16>::new();
    let mut expression_reference_edges = std::collections::HashSet::<([u8; 16], [u8; 16])>::new();
    let mut typed_name_records = Vec::<(u16, [u8; 16])>::new();
    let mut constraint_kinds = HashMap::<[u8; 16], u16>::new();
    let mut constraint_tables = HashMap::<[u8; 16], [u8; 16]>::new();
    let mut constraint_source_columns = HashMap::<[u8; 16], Vec<[u8; 16]>>::new();
    let mut constraint_referenced_columns = HashMap::<[u8; 16], Vec<[u8; 16]>>::new();
    let mut foreign_key_targets = HashMap::<[u8; 16], [u8; 16]>::new();
    let mut backing_index_targets = HashMap::<[u8; 16], [u8; 16]>::new();
    let mut index_owners = HashMap::<[u8; 16], [u8; 16]>::new();
    let mut memberships = Vec::<Membership>::new();
    let mut membership_grantors = Vec::<[u8; 16]>::new();
    let mut object_parents = HashMap::<[u8; 16], Option<[u8; 16]>>::new();
    let mut dependencies = Vec::<([u8; 16], [u8; 16], u16)>::new();
    let mut authority_edges = std::collections::HashSet::<([u8; 16], [u8; 16])>::new();
    let mut grantors = Vec::<[u8; 16]>::new();
    let mut database_records = 0usize;
    let mut database_id = None;
    for entry in table.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if key.len() < 2 {
            return Err(StartupError::InvalidCatalog);
        }
        let family_code = u16::from_be_bytes([key[0], key[1]]) as usize;
        if !(1..=16).contains(&family_code) {
            return Err(StartupError::InvalidCatalog);
        }
        family_counts[family_code] = family_counts[family_code]
            .checked_add(1)
            .ok_or(StartupError::CountOverflow)?;
        if family_counts[family_code] > MAX_CATALOG_RECORDS_PER_FAMILY {
            return Err(StartupError::CatalogLimit);
        }
        count = count.checked_add(1).ok_or(StartupError::CountOverflow)?;

        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.logical_key != key || record.family_code as usize != family_code {
            return Err(StartupError::InvalidCatalog);
        }
        if record.family_code == 16 {
            credential_verifiers
                .push(decode_verifier_record(&record).map_err(|_| StartupError::InvalidCatalog)?);
        }
        if record.family_code == 1 {
            if key.len() != 19
                || database_id
                    .replace(key[3..19].try_into().unwrap())
                    .is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            database_records += 1;
            builtin_mappings = Some(validate_database_record(&record, metadata)?);
        }
        if record.family_code == 2 {
            if key.len() != 19 {
                return Err(StartupError::InvalidCatalog);
            }
            let object_id: [u8; 16] = key
                .get(3..19)
                .ok_or(StartupError::InvalidCatalog)?
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let kind = field_u16(&record, 1)?;
            let parent_id = record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .map(|field| {
                    field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| StartupError::InvalidCatalog)
                })
                .transpose()?;
            if !matches!(kind, 1..=10)
                || object_parents.insert(object_id, parent_id).is_some()
                || object_kinds.insert(object_id, kind).is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            object_owners.push(field_id(&record, 4)?);
        }
        if let Some(object_kind) = typed_family_object_kind(&record)? {
            let object_id: [u8; 16] = key
                .get(3..19)
                .ok_or(StartupError::InvalidCatalog)?
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            if typed_object_kinds.insert(object_id, object_kind).is_some() {
                return Err(StartupError::InvalidCatalog);
            }
            if matches!(record.family_code, 6 | 10) {
                typed_name_records.push((record.family_code, object_id));
            }
        }
        if record.family_code == 4 {
            let type_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let builtin_kind =
                validate_builtin_type_profile(&record).map_err(|_| StartupError::InvalidCatalog)?;
            if !(1..=11).contains(&builtin_kind)
                || builtin_type_kinds.insert(type_id, builtin_kind).is_some()
                || builtin_type_ids.insert(builtin_kind, type_id).is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            type_records.insert(type_id, record.clone());
        }
        if record.family_code == 5 {
            let table_id: [u8; 16] = key
                .get(3..19)
                .ok_or(StartupError::InvalidCatalog)?
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let schema_version = field_u64(&record, 2)?;
            if key.len() != 19
                || schema_version == 0
                || field_u64(&record, 3)? == 0
                || field_u16(&record, 5)? != 1
                || table_schema_versions
                    .insert(table_id, schema_version)
                    .is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
        }
        if record.family_code == 6 {
            let column_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let table_id = field_id(&record, 1)?;
            if column_tables.insert(column_id, table_id).is_some() {
                return Err(StartupError::InvalidCatalog);
            }
            if column_nullable
                .insert(column_id, field_bool(&record, 6)?)
                .is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            columns_by_table
                .entry(table_id)
                .or_default()
                .push(PersistedColumnSchema {
                    column_id,
                    ordinal: field_u32(&record, 2)?,
                    type_id: field_id(&record, 4)?,
                    parameters: field_payload(&record, 5)?.to_vec(),
                    nullable: field_bool(&record, 6)?,
                    introduced_schema_version: field_u64(&record, 9)?,
                });
        }
        if record.family_code == 7 {
            let expression = decode_catalog_expression_record(&record)
                .map_err(|_| StartupError::InvalidCatalog)?;
            let mut unique_dependencies = std::collections::HashSet::new();
            for dependency_id in expression.dependencies {
                if dependency_id == [0; 16] || !unique_dependencies.insert(dependency_id) {
                    return Err(StartupError::InvalidCatalog);
                }
                insert_expression_reference_edge(
                    &mut expression_reference_edges,
                    expression.expression_id,
                    dependency_id,
                )?;
            }
        }
        if record.family_code == 6 {
            let column_id: [u8; 16] = key
                .get(3..19)
                .ok_or(StartupError::InvalidCatalog)?
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            for tag in [7, 10] {
                if let Some(field) = record.fields.iter().find(|field| field.tag == tag) {
                    let expression_id: [u8; 16] = field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| StartupError::InvalidCatalog)?;
                    insert_expression_reference_edge(
                        &mut expression_reference_edges,
                        column_id,
                        expression_id,
                    )?;
                }
            }
        }
        if record.family_code == 9 {
            let index_id: [u8; 16] = key
                .get(3..19)
                .ok_or(StartupError::InvalidCatalog)?
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let table_id = field_id(&record, 1)?;
            let columns = field_ids(&record, 2)?;
            let unique = field_bool(&record, 4)?;
            let schema_version = table_schema_versions
                .get(&table_id)
                .copied()
                .ok_or(StartupError::InvalidCatalog)?;
            let mut unique_columns = std::collections::HashSet::new();
            if index_id == [0; 16]
                || table_id == [0; 16]
                || field_u16(&record, 3)? != 1
                || field_u16(&record, 5)? != 1
                || field_optional_id(&record, 6)?.is_some_and(|id| id == [0; 16])
                || field_id(&record, 7)? == [0; 16]
                || field_u16(&record, 8)? != 1
                || field_u32(&record, 9)? != 1
                || field_u64(&record, 10)? != schema_version
                || index_definitions.insert(index_id, table_id).is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            if unique {
                unique_index_ids.insert(index_id);
            }
            if columns.is_empty()
                || columns
                    .iter()
                    .any(|column_id| !unique_columns.insert(*column_id))
                || index_columns.insert(index_id, columns).is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            if let Some(field) = record.fields.iter().find(|field| field.tag == 6) {
                let owner_id = field
                    .payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| StartupError::InvalidCatalog)?;
                if index_owners.insert(index_id, owner_id).is_some() {
                    return Err(StartupError::InvalidCatalog);
                }
            }
        }
        if record.family_code == 8 {
            if key.len() != 19 {
                return Err(StartupError::InvalidCatalog);
            }
            let constraint_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let kind = field_u16(&record, 2)?;
            let table_id = field_id(&record, 1)?;
            let source_columns = field_ids(&record, 3)?;
            if kind == 5 {
                let referenced_columns = field_ids(&record, 6)?;
                if source_columns.is_empty()
                    || referenced_columns.is_empty()
                    || referenced_columns.len() != source_columns.len()
                    || field_optional_id(&record, 4)?.is_some()
                    || field_optional_id(&record, 11)?.is_some()
                    || field_u16(&record, 7)? != 1
                    || field_u16(&record, 8)? != 1
                    || field_u16(&record, 9)? != 1
                    || field_u16(&record, 10)? != 1
                {
                    return Err(StartupError::InvalidCatalog);
                }
            }
            if !matches!(kind, 1..=5) || constraint_kinds.insert(constraint_id, kind).is_some() {
                return Err(StartupError::InvalidCatalog);
            }
            if constraint_tables.insert(constraint_id, table_id).is_some()
                || constraint_source_columns
                    .insert(constraint_id, source_columns)
                    .is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            if kind == 5
                && (foreign_key_targets
                    .insert(constraint_id, field_id(&record, 5)?)
                    .is_some()
                    || constraint_referenced_columns
                        .insert(constraint_id, field_ids(&record, 6)?)
                        .is_some())
            {
                return Err(StartupError::InvalidCatalog);
            }
            if matches!(kind, 2 | 3)
                && backing_index_targets
                    .insert(constraint_id, field_id(&record, 11)?)
                    .is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            if kind == 4 {
                insert_expression_reference_edge(
                    &mut expression_reference_edges,
                    constraint_id,
                    field_id(&record, 4)?,
                )?;
            } else if record.fields.iter().any(|field| field.tag == 4) {
                return Err(StartupError::InvalidCatalog);
            }
        }
        if record.family_code == 13 {
            let policy_id: [u8; 16] = key
                .get(3..19)
                .ok_or(StartupError::InvalidCatalog)?
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let policy_kind = field_u16(&record, 2)?;
            if !matches!(policy_kind, 1 | 2) {
                return Err(StartupError::InvalidCatalog);
            }
            if policy_tables
                .insert(policy_id, field_id(&record, 1)?)
                .is_some()
                || policy_kinds.insert(policy_id, policy_kind).is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
            for tag in [5, 6] {
                if let Some(field) = record.fields.iter().find(|field| field.tag == tag) {
                    let expression_id: [u8; 16] = field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| StartupError::InvalidCatalog)?;
                    insert_expression_reference_edge(
                        &mut expression_reference_edges,
                        policy_id,
                        expression_id,
                    )?;
                }
            }
        }
        if record.family_code == 10 {
            let principal_id: [u8; 16] = key
                .get(3..19)
                .ok_or(StartupError::InvalidCatalog)?
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let kind = field_u16(&record, 1)?;
            let login_enabled = field_bool(&record, 3)?;
            let name = record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .ok_or(StartupError::InvalidCatalog)?
                .payload
                .clone();
            if !matches!(kind, 1 | 2)
                || (kind == 2 && login_enabled)
                || field_u64(&record, 4)? == 0
                || principal_kinds.insert(principal_id, kind).is_some()
                || principal_login_enabled
                    .insert(principal_id, login_enabled)
                    .is_some()
                || principal_names.insert(principal_id, name.clone()).is_some()
                || principal_name_ids.insert(name, principal_id).is_some()
            {
                return Err(StartupError::InvalidCatalog);
            }
        }
        if record.family_code == 11 {
            if key.len() != 36 || memberships.len() == MAX_MEMBERSHIP_EDGES {
                return Err(StartupError::InvalidCatalog);
            }
            let member_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let role_id: [u8; 16] = key[20..36]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let grantor_id = field_id(&record, 1)?;
            if field_u64(&record, 2)? == 0 {
                return Err(StartupError::InvalidCatalog);
            }
            memberships.push(Membership { member_id, role_id });
            membership_grantors.push(grantor_id);
            insert_authority_edge(&mut authority_edges, member_id, role_id)?;
        }
        if record.family_code == 12 {
            if key.len() != 59
                || key[2] != 1
                || key[19] != 1
                || key[36] != 1
                || key[39] != 1
                || key[42] != 1
                || field_u64(&record, 2)? == 0
            {
                return Err(StartupError::InvalidCatalog);
            }
            let grantee_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let object_id: [u8; 16] = key[20..36]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let privilege = u16::from_be_bytes([key[37], key[38]]);
            let scope = u16::from_be_bytes([key[40], key[41]]);
            let target_id: [u8; 16] = key[43..59]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let grantor_id = field_id(&record, 1)?;
            grantors.push(grantor_id);
            let target_kind = object_kinds.get(&target_id).copied();
            let valid_scope = match (privilege, scope) {
                (1 | 7, 1) => object_id == target_id && target_kind == Some(1),
                (2..=4 | 6, 1) => object_id == target_id && target_kind == Some(3),
                (5, 1) => object_id == target_id && target_kind == Some(3),
                (8..=11, 1) => object_id == target_id && matches!(target_kind, Some(1 | 3)),
                (2..=4 | 6, 2) => {
                    object_kinds.get(&object_id) == Some(&3)
                        && target_kind == Some(4)
                        && column_tables.get(&target_id) == Some(&object_id)
                }
                (12..=14, 1) => object_id == target_id && Some(target_id) == database_id,
                _ => false,
            };
            if !matches!(principal_kinds.get(&grantee_id), Some(1 | 2)) || !valid_scope {
                return Err(StartupError::InvalidCatalog);
            }
            insert_authority_edge(&mut authority_edges, grantee_id, target_id)?;
        }
        if record.family_code == 14 {
            if key.len() != 39 || key[2] != 1 || key[19] != 1 || key[22] != 1 {
                return Err(StartupError::InvalidCatalog);
            }
            let policy_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let target_kind = u16::from_be_bytes([key[20], key[21]]);
            let target_id: [u8; 16] = key[23..39]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            if typed_object_kinds.get(&policy_id) != Some(&10) {
                return Err(StartupError::InvalidCatalog);
            }
            match target_kind {
                1 if principal_kinds.get(&target_id) == Some(&1) => {
                    insert_authority_edge(&mut authority_edges, policy_id, target_id)?;
                }
                2 if principal_kinds.get(&target_id) == Some(&2) => {
                    insert_authority_edge(&mut authority_edges, policy_id, target_id)?;
                }
                3 if policy_kinds.get(&policy_id) == Some(&2)
                    && typed_object_kinds.get(&target_id) == Some(&4)
                    && column_tables.get(&target_id) == policy_tables.get(&policy_id)
                    && policy_tables.get(&policy_id).is_some_and(|table_id| {
                        object_kinds.get(table_id) == Some(&3)
                            && typed_object_kinds.get(table_id) == Some(&3)
                    }) => {}
                _ => return Err(StartupError::InvalidCatalog),
            }
        }
        if record.family_code == 15 {
            if key.len() != 39 {
                return Err(StartupError::InvalidCatalog);
            }
            let dependent_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let referenced_id: [u8; 16] = key[20..36]
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let kind = u16::from_be_bytes(
                key[37..39]
                    .try_into()
                    .map_err(|_| StartupError::InvalidCatalog)?,
            );
            if !matches!(kind, 1..=5) {
                return Err(StartupError::InvalidCatalog);
            }
            dependencies.push((dependent_id, referenced_id, kind));
        }
    }
    if database_records != 1 {
        return Err(StartupError::InvalidCatalog);
    }
    if credential_verifiers.iter().any(|credential| {
        principal_kinds.get(&credential.user_id) != Some(&1)
            || (!credential.revoked
                && principal_login_enabled.get(&credential.user_id) != Some(&true))
    }) {
        return Err(StartupError::InvalidCatalog);
    }
    if database_id != Some(metadata.database_id)
        || dependencies
            .iter()
            .any(|(dependent_id, referenced_id, kind)| {
                !object_parents.contains_key(dependent_id)
                    || !(object_parents.contains_key(referenced_id)
                        || *kind == 5 && Some(*referenced_id) == database_id)
            })
    {
        return Err(StartupError::InvalidCatalog);
    }
    let builtin_mappings = builtin_mappings.ok_or(StartupError::InvalidCatalog)?;
    for schema_code in [1u16, 2] {
        let schema_id = builtin_mappings
            .get(&schema_code)
            .ok_or(StartupError::InvalidCatalog)?;
        if object_kinds.get(schema_id) != Some(&1) || typed_object_kinds.get(schema_id) != Some(&1)
        {
            return Err(StartupError::InvalidCatalog);
        }
    }
    let admin_role_id = builtin_mappings
        .get(&3)
        .ok_or(StartupError::InvalidCatalog)?;
    if principal_kinds.get(admin_role_id) != Some(&2)
        || object_kinds.get(admin_role_id) != Some(&9)
        || typed_object_kinds.get(admin_role_id) != Some(&9)
        || principal_names
            .get(admin_role_id)
            .is_none_or(|name| name.as_slice() != b"database_admin")
    {
        return Err(StartupError::InvalidCatalog);
    }
    for code in 101u16..=111 {
        let type_id = builtin_mappings
            .get(&code)
            .ok_or(StartupError::InvalidCatalog)?;
        if builtin_type_kinds.get(type_id) != Some(&(code - 100))
            || object_kinds.get(type_id) != Some(&2)
            || typed_object_kinds.get(type_id) != Some(&2)
        {
            return Err(StartupError::InvalidCatalog);
        }
    }
    let actual_expression_reference_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 2)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<std::collections::HashSet<_>>();
    if actual_expression_reference_edges != expression_reference_edges
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
        return Err(StartupError::InvalidCatalog);
    }
    let actual_authority_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 5)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<std::collections::HashSet<_>>();
    if actual_authority_edges != authority_edges
        || authority_edges.iter().any(|(dependent_id, referenced_id)| {
            (!principal_kinds.contains_key(dependent_id)
                && object_kinds.get(dependent_id) != Some(&10))
                || (!principal_kinds.contains_key(referenced_id)
                    && !object_parents.contains_key(referenced_id)
                    && Some(*referenced_id) != database_id)
        })
    {
        return Err(StartupError::InvalidCatalog);
    }
    let foreign_key_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 3)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<std::collections::HashSet<_>>();
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
        return Err(StartupError::InvalidCatalog);
    }
    let backing_index_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 4)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<std::collections::HashSet<_>>();
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
        return Err(StartupError::InvalidCatalog);
    }
    if !validate_owned_child_graph(&object_parents, &dependencies) {
        return Err(StartupError::InvalidCatalog);
    }
    for (constraint_id, table_id) in &constraint_tables {
        let source_columns = constraint_source_columns
            .get(constraint_id)
            .ok_or(StartupError::InvalidCatalog)?;
        if object_kinds.get(table_id) != Some(&3) || typed_object_kinds.get(table_id) != Some(&3) {
            return Err(StartupError::InvalidCatalog);
        }
        let mut unique_source_columns = std::collections::HashSet::new();
        for column_id in source_columns {
            if !unique_source_columns.insert(*column_id)
                || object_kinds.get(column_id) != Some(&4)
                || typed_object_kinds.get(column_id) != Some(&4)
                || column_tables.get(column_id) != Some(table_id)
            {
                return Err(StartupError::InvalidCatalog);
            }
        }
        if constraint_kinds.get(constraint_id) == Some(&1)
            && (source_columns.len() != 1
                || column_nullable.get(&source_columns[0]) != Some(&false))
        {
            return Err(StartupError::InvalidCatalog);
        }
        if matches!(constraint_kinds.get(constraint_id), Some(2 | 3)) {
            let index_id = backing_index_targets
                .get(constraint_id)
                .ok_or(StartupError::InvalidCatalog)?;
            if source_columns.is_empty()
                || index_columns.get(index_id) != Some(source_columns)
                || index_owners.get(index_id) != Some(constraint_id)
                || !unique_index_ids.contains(index_id)
                || (constraint_kinds.get(constraint_id) == Some(&2)
                    && source_columns
                        .iter()
                        .any(|column_id| column_nullable.get(column_id) != Some(&false)))
            {
                return Err(StartupError::InvalidCatalog);
            }
        }
    }
    for (constraint_id, target_id) in &foreign_key_targets {
        let source_columns = constraint_source_columns
            .get(constraint_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let referenced_columns = constraint_referenced_columns
            .get(constraint_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let target_columns = constraint_source_columns
            .get(target_id)
            .ok_or(StartupError::InvalidCatalog)?;
        if !matches!(constraint_kinds.get(target_id), Some(2 | 3))
            || source_columns.is_empty()
            || referenced_columns.len() != source_columns.len()
            || referenced_columns != target_columns
        {
            return Err(StartupError::InvalidCatalog);
        }
    }
    for (index_id, table_id) in &index_definitions {
        if object_kinds.get(table_id) != Some(&3) || typed_object_kinds.get(table_id) != Some(&3) {
            return Err(StartupError::InvalidCatalog);
        }
        let columns = index_columns
            .get(index_id)
            .ok_or(StartupError::InvalidCatalog)?;
        for column_id in columns {
            if object_kinds.get(column_id) != Some(&4)
                || typed_object_kinds.get(column_id) != Some(&4)
                || column_tables.get(column_id) != Some(table_id)
            {
                return Err(StartupError::InvalidCatalog);
            }
        }
    }
    if typed_object_kinds
        .iter()
        .any(|(object_id, expected_kind)| object_kinds.get(object_id) != Some(expected_kind))
        || object_kinds.iter().any(|(object_id, expected_kind)| {
            typed_object_kinds.get(object_id) != Some(expected_kind)
        })
    {
        return Err(StartupError::InvalidCatalog);
    }
    for (family, object_id) in typed_name_records {
        let typed_key = catalog_id_key(family, object_id);
        let typed_value = table
            .get(typed_key.as_slice())
            .map_err(storage_error)?
            .ok_or(StartupError::InvalidCatalog)?;
        let typed_record = decode_registered_envelope(typed_value.value())
            .map_err(|_| StartupError::InvalidCatalog)?;
        let name_tag = if family == 6 { 3 } else { 2 };
        let object_key = catalog_id_key(2, object_id);
        let object_value = table
            .get(object_key.as_slice())
            .map_err(storage_error)?
            .ok_or(StartupError::InvalidCatalog)?;
        let object_record = decode_registered_envelope(object_value.value())
            .map_err(|_| StartupError::InvalidCatalog)?;
        if field_payload(&typed_record, name_tag)? != field_payload(&object_record, 3)? {
            return Err(StartupError::InvalidCatalog);
        }
    }
    if object_owners
        .iter()
        .any(|owner_id| !principal_kinds.contains_key(owner_id))
    {
        return Err(StartupError::InvalidCatalog);
    }
    if membership_grantors
        .iter()
        .any(|grantor_id| !principal_kinds.contains_key(grantor_id))
        || grantors
            .iter()
            .any(|grantor_id| !principal_kinds.contains_key(grantor_id))
    {
        return Err(StartupError::InvalidCatalog);
    }
    for membership in &memberships {
        if !principal_kinds.contains_key(&membership.member_id)
            || principal_kinds.get(&membership.role_id) != Some(&2)
        {
            return Err(StartupError::InvalidCatalog);
        }
    }
    validate_membership_graph(&memberships).map_err(|_| StartupError::InvalidCatalog)?;
    let mut table_schemas = HashMap::with_capacity(columns_by_table.len());
    for (table_id, mut columns_by_ordinal) in columns_by_table {
        let schema_version = *table_schema_versions
            .get(&table_id)
            .ok_or(StartupError::InvalidCatalog)?;
        if columns_by_ordinal.len() > crate::tuple::MAX_COLUMNS {
            return Err(StartupError::InvalidCatalog);
        }
        columns_by_ordinal.sort_by_key(|column| column.ordinal);
        let mut columns = Vec::with_capacity(columns_by_ordinal.len());
        let mut column_ids = Vec::with_capacity(columns_by_ordinal.len());
        for (expected_ordinal, column) in columns_by_ordinal.into_iter().enumerate() {
            if column.ordinal as usize != expected_ordinal
                || column.introduced_schema_version == 0
                || column.introduced_schema_version > schema_version
            {
                return Err(StartupError::InvalidCatalog);
            }
            let type_record = type_records
                .get(&column.type_id)
                .ok_or(StartupError::InvalidCatalog)?;
            let scalar_type = decode_builtin_scalar_type(type_record, &column.parameters)
                .map_err(|_| StartupError::InvalidCatalog)?;
            column_ids.push(column.column_id);
            columns.push(ColumnSchema {
                scalar_type,
                nullable: column.nullable,
            });
        }
        if table_schemas
            .insert(
                table_id,
                PersistedTableSchema {
                    schema_version,
                    column_ids,
                    columns,
                },
            )
            .is_some()
        {
            return Err(StartupError::InvalidCatalog);
        }
    }
    if foreign_key_targets.len() > MAX_FOREIGN_KEYS_PER_DATABASE {
        return Err(StartupError::InvalidCatalog);
    }
    let mut foreign_keys = Vec::with_capacity(foreign_key_targets.len());
    for (foreign_key_id, target_constraint_id) in &foreign_key_targets {
        let source_table_id = *constraint_tables
            .get(foreign_key_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let target_table_id = *constraint_tables
            .get(target_constraint_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let source_column_ids = constraint_source_columns
            .get(foreign_key_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let target_column_ids = constraint_referenced_columns
            .get(foreign_key_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let source_schema = table_schemas
            .get(&source_table_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let target_schema = table_schemas
            .get(&target_table_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let source_ordinals = source_column_ids
            .iter()
            .map(|column_id| {
                source_schema
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                    .ok_or(StartupError::InvalidCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let target_ordinals = target_column_ids
            .iter()
            .map(|column_id| {
                target_schema
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                    .ok_or(StartupError::InvalidCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut comparison_columns = Vec::with_capacity(target_ordinals.len());
        for (source_ordinal, target_ordinal) in source_ordinals.iter().zip(&target_ordinals) {
            let source_column = source_schema
                .columns
                .get(*source_ordinal)
                .ok_or(StartupError::InvalidCatalog)?;
            let target_column = target_schema
                .columns
                .get(*target_ordinal)
                .ok_or(StartupError::InvalidCatalog)?;
            if source_column.scalar_type != target_column.scalar_type {
                return Err(StartupError::InvalidCatalog);
            }
            comparison_columns.push(ColumnSchema {
                scalar_type: target_column.scalar_type,
                nullable: true,
            });
        }
        let target_index_id = *backing_index_targets
            .get(target_constraint_id)
            .ok_or(StartupError::InvalidCatalog)?;
        foreign_keys.push(PersistedForeignKey {
            source_table_id,
            source_ordinals,
            target_index_id,
            comparison_columns,
        });
    }
    Ok(PersistedCatalog {
        record_count: count,
        index_definitions,
        index_columns,
        unique_index_ids,
        table_schemas,
        foreign_keys,
    })
}

fn typed_family_object_kind(record: &CatalogRecord) -> Result<Option<u16>, StartupError> {
    let kind = match record.family_code {
        3 => Some(1),
        4 => Some(2),
        5 => Some(3),
        6 => Some(4),
        7 => Some(5),
        8 => Some(6),
        9 => Some(7),
        10 => match field_u16(record, 1)? {
            1 => Some(8),
            2 => Some(9),
            _ => return Err(StartupError::InvalidCatalog),
        },
        13 => Some(10),
        _ => None,
    };
    Ok(kind)
}

fn validate_database_record(
    record: &CatalogRecord,
    metadata: &EngineMetadata,
) -> Result<HashMap<u16, [u8; 16]>, StartupError> {
    let key = &record.logical_key;
    if key.len() != 19
        || key[..3] != [0, 1, 1]
        || key[3..] != metadata.database_id
        || field_u32(record, 2)? != metadata.catalog_version
        || field_u32(record, 5)? != metadata.expression_version
        || field_u32(record, 6)? != metadata.builtin_semantics_version
    {
        return Err(StartupError::InvalidCatalog);
    }

    let encoded = record
        .fields
        .iter()
        .find(|field| field.tag == 3)
        .ok_or(StartupError::InvalidCatalog)?
        .payload
        .as_slice();
    let count_bytes: [u8; 4] = encoded
        .get(..4)
        .ok_or(StartupError::InvalidCatalog)?
        .try_into()
        .map_err(|_| StartupError::InvalidCatalog)?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    if count != 30 {
        return Err(StartupError::InvalidCatalog);
    }
    let expected_len = count
        .checked_mul(20)
        .and_then(|entries| entries.checked_add(4))
        .ok_or(StartupError::InvalidCatalog)?;
    if encoded.len() != expected_len {
        return Err(StartupError::InvalidCatalog);
    }

    let mut mappings = HashMap::<u16, [u8; 16]>::with_capacity(count);
    let mut ids = HashMap::<[u8; 16], ()>::with_capacity(count);
    let mut previous_code = None;
    for entry in encoded[4..].chunks_exact(20) {
        let code = u16::from_le_bytes([entry[0], entry[1]]);
        let reserved = u16::from_le_bytes([entry[2], entry[3]]);
        let id: [u8; 16] = entry[4..]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        if previous_code.is_some_and(|previous| code <= previous)
            || !is_required_bootstrap_code(code)
            || reserved != 0
            || id == [0; 16]
            || mappings.insert(code, id).is_some()
            || ids.insert(id, ()).is_some()
        {
            return Err(StartupError::InvalidCatalog);
        }
        previous_code = Some(code);
    }
    if required_bootstrap_codes().any(|code| !mappings.contains_key(&code)) {
        return Err(StartupError::InvalidCatalog);
    }
    Ok(mappings)
}

fn required_bootstrap_codes() -> impl Iterator<Item = u16> {
    [1, 2, 3].into_iter().chain(101..=111).chain(201..=216)
}

fn is_required_bootstrap_code(code: u16) -> bool {
    required_bootstrap_codes().any(|required| required == code)
}

fn field_id(record: &CatalogRecord, tag: u16) -> Result<[u8; 16], StartupError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .ok_or(StartupError::InvalidCatalog)?
        .payload
        .as_slice()
        .try_into()
        .map_err(|_| StartupError::InvalidCatalog)
}

fn field_optional_id(record: &CatalogRecord, tag: u16) -> Result<Option<[u8; 16]>, StartupError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| {
            field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)
        })
        .transpose()
}

fn field_ids(record: &CatalogRecord, tag: u16) -> Result<Vec<[u8; 16]>, StartupError> {
    let payload = field_payload(record, tag)?;
    if payload.len() < 4 {
        return Err(StartupError::InvalidCatalog);
    }
    let count = u32::from_le_bytes(
        payload[..4]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?,
    ) as usize;
    if count > 256 || payload.len() != 4 + count * 16 {
        return Err(StartupError::InvalidCatalog);
    }
    payload[4..]
        .chunks_exact(16)
        .map(|bytes| bytes.try_into().map_err(|_| StartupError::InvalidCatalog))
        .collect()
}

fn field_payload(record: &CatalogRecord, tag: u16) -> Result<&[u8], StartupError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.as_slice())
        .ok_or(StartupError::InvalidCatalog)
}

fn catalog_id_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

fn field_u16(record: &CatalogRecord, tag: u16) -> Result<u16, StartupError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .ok_or(StartupError::InvalidCatalog)?
        .payload
        .as_slice()
        .try_into()
        .map(u16::from_le_bytes)
        .map_err(|_| StartupError::InvalidCatalog)
}

fn field_u32(record: &CatalogRecord, tag: u16) -> Result<u32, StartupError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .ok_or(StartupError::InvalidCatalog)?
        .payload
        .as_slice()
        .try_into()
        .map(u32::from_le_bytes)
        .map_err(|_| StartupError::InvalidCatalog)
}

fn field_u64(record: &CatalogRecord, tag: u16) -> Result<u64, StartupError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .ok_or(StartupError::InvalidCatalog)?
        .payload
        .as_slice()
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| StartupError::InvalidCatalog)
}

fn field_bool(record: &CatalogRecord, tag: u16) -> Result<bool, StartupError> {
    match record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .ok_or(StartupError::InvalidCatalog)?
        .payload
        .as_slice()
    {
        [0] => Ok(false),
        [1] => Ok(true),
        _ => Err(StartupError::InvalidCatalog),
    }
}

fn validate_rows(
    read: &ReadTransaction,
    commit_sequence: u64,
    table_schemas: &PersistedTableSchemas,
) -> Result<usize, StartupError> {
    let table = read.open_table(ROWS).map_err(storage_error)?;
    let mut count = 0usize;
    for entry in table.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if key.len() != 32 || key[..16] == [0; 16] {
            return Err(StartupError::InvalidRow);
        }
        let record = RowRecord::decode(value.value()).map_err(|_| StartupError::InvalidRow)?;
        if record.row_version > commit_sequence {
            return Err(StartupError::SequenceAhead);
        }
        let table_id: [u8; 16] = key[..16].try_into().map_err(|_| StartupError::InvalidRow)?;
        let schema = table_schemas
            .get(&table_id)
            .ok_or(StartupError::InvalidRow)?;
        decode_tuple(&schema.columns, schema.schema_version, &record.payload)
            .map_err(|_| StartupError::InvalidRow)?;
        count = count.checked_add(1).ok_or(StartupError::CountOverflow)?;
    }
    Ok(count)
}

fn validate_foreign_key_rows(
    read: &ReadTransaction,
    foreign_keys: &[PersistedForeignKey],
    table_schemas: &PersistedTableSchemas,
) -> Result<(), StartupError> {
    let rows = read.open_table(ROWS).map_err(storage_error)?;
    let indexes = read.open_table(INDEXES).map_err(storage_error)?;
    let mut row_visits = 0usize;
    for foreign_key in foreign_keys {
        let source_schema = table_schemas
            .get(&foreign_key.source_table_id)
            .ok_or(StartupError::InvalidCatalog)?;
        let mut start = [0; 32];
        start[..16].copy_from_slice(&foreign_key.source_table_id);
        let mut end = [u8::MAX; 32];
        end[..16].copy_from_slice(&foreign_key.source_table_id);
        for entry in rows
            .range(start.as_slice()..=end.as_slice())
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            if key.value().len() != 32 || key.value()[..16] != foreign_key.source_table_id {
                return Err(StartupError::InvalidRow);
            }
            row_visits = row_visits
                .checked_add(1)
                .ok_or(StartupError::ForeignKeyValidationLimitExceeded)?;
            if row_visits > MAX_FOREIGN_KEY_ROW_VISITS_PER_STARTUP {
                return Err(StartupError::ForeignKeyValidationLimitExceeded);
            }
            let row = RowRecord::decode(value.value()).map_err(|_| StartupError::InvalidRow)?;
            let tuple = decode_tuple(
                &source_schema.columns,
                source_schema.schema_version,
                &row.payload,
            )
            .map_err(|_| StartupError::InvalidRow)?;
            let projected = foreign_key
                .source_ordinals
                .iter()
                .map(|ordinal| {
                    tuple
                        .values
                        .get(*ordinal)
                        .cloned()
                        .ok_or(StartupError::InvalidCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if projected.iter().any(Option::is_none) {
                continue;
            }
            let encoded_tuple = encode_sql_tuple(&foreign_key.comparison_columns, &projected)
                .map_err(|_| StartupError::InvalidRow)?;
            let mut index_prefix = Vec::with_capacity(16 + encoded_tuple.len());
            index_prefix.extend_from_slice(&foreign_key.target_index_id);
            index_prefix.extend_from_slice(&encoded_tuple);
            let mut matching = indexes
                .range(index_prefix.as_slice()..)
                .map_err(storage_error)?;
            let Some(entry) = matching.next() else {
                return Err(StartupError::InvalidForeignKey);
            };
            let (candidate, _) = entry.map_err(storage_error)?;
            if !candidate.value().starts_with(&index_prefix) {
                return Err(StartupError::InvalidForeignKey);
            }
        }
    }
    Ok(())
}

fn validate_indexes(
    read: &ReadTransaction,
    commit_sequence: u64,
    index_definitions: &IndexDefinitions,
    index_columns: &IndexColumns,
    unique_index_ids: &UniqueIndexIds,
    table_schemas: &PersistedTableSchemas,
) -> Result<usize, StartupError> {
    let indexes = read.open_table(INDEXES).map_err(storage_error)?;
    let rows = read.open_table(ROWS).map_err(storage_error)?;
    let reverse = read.open_table(ROW_INDEXES).map_err(storage_error)?;
    let mut indexes_by_table = HashMap::<[u8; 16], Vec<[u8; 16]>>::new();
    for (index_id, table_id) in index_definitions {
        indexes_by_table
            .entry(*table_id)
            .or_default()
            .push(*index_id);
    }
    let mut count = 0usize;
    for entry in indexes.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if key.len() < 33 {
            return Err(StartupError::InvalidIndex);
        }
        let index_id: [u8; 16] = key[..16]
            .try_into()
            .map_err(|_| StartupError::InvalidIndex)?;
        let table_id = index_definitions
            .get(&index_id)
            .ok_or(StartupError::InvalidIndex)?;
        let locator = IndexLocator::decode_for_key(key, value.value())
            .map_err(|_| StartupError::InvalidIndex)?;
        if locator.row_version > commit_sequence {
            return Err(StartupError::SequenceAhead);
        }
        let mut row_key = [0; 32];
        row_key[..16].copy_from_slice(table_id);
        row_key[16..].copy_from_slice(&locator.row_id);
        let row = rows
            .get(&row_key[..])
            .map_err(storage_error)?
            .ok_or(StartupError::InvalidIndex)?;
        let row = RowRecord::decode(row.value()).map_err(|_| StartupError::InvalidIndex)?;
        let expected_key = derive_persisted_index_key(
            index_id,
            locator.row_id,
            *table_id,
            &row.payload,
            index_columns,
            table_schemas,
        )?;
        if locator.row_version != row.row_version || expected_key.key.as_slice() != key {
            return Err(StartupError::InvalidIndex);
        }
        let mut reverse_key = row_key.to_vec();
        reverse_key.extend_from_slice(&key[..key.len() - 16]);
        let reverse_value = reverse.get(&reverse_key[..]).map_err(storage_error)?;
        if reverse_value.is_none_or(|value| !value.value().is_empty()) {
            return Err(StartupError::InvalidIndex);
        }
        count = count.checked_add(1).ok_or(StartupError::CountOverflow)?;
    }
    let mut expected_count = 0usize;
    let mut unique_keys = HashSet::<Vec<u8>>::new();
    for entry in rows.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let row_key: [u8; 32] = key
            .value()
            .try_into()
            .map_err(|_| StartupError::InvalidRow)?;
        let table_id: [u8; 16] = row_key[..16]
            .try_into()
            .map_err(|_| StartupError::InvalidRow)?;
        let row = RowRecord::decode(value.value()).map_err(|_| StartupError::InvalidRow)?;
        if let Some(index_ids) = indexes_by_table.get(&table_id) {
            for index_id in index_ids {
                let derived = derive_persisted_index_key(
                    *index_id,
                    row_key[16..]
                        .try_into()
                        .map_err(|_| StartupError::InvalidRow)?,
                    table_id,
                    &row.payload,
                    index_columns,
                    table_schemas,
                )?;
                if unique_index_ids.contains(index_id) && !derived.has_null {
                    let unique_key = derived.key[..derived.key.len() - 16].to_vec();
                    if !unique_keys.insert(unique_key) {
                        return Err(StartupError::InvalidIndex);
                    }
                }
                let index_key = derived.key;
                let locator = indexes
                    .get(index_key.as_slice())
                    .map_err(storage_error)?
                    .ok_or(StartupError::InvalidIndex)?;
                let locator = IndexLocator::decode_for_key(&index_key, locator.value())
                    .map_err(|_| StartupError::InvalidIndex)?;
                if locator.row_version != row.row_version || locator.row_id != row_key[16..] {
                    return Err(StartupError::InvalidIndex);
                }
                let mut reverse_key = row_key.to_vec();
                reverse_key.extend_from_slice(&index_key[..index_key.len() - 16]);
                let reverse_value = reverse.get(&reverse_key[..]).map_err(storage_error)?;
                if reverse_value.is_none_or(|value| !value.value().is_empty()) {
                    return Err(StartupError::InvalidIndex);
                }
                expected_count = expected_count
                    .checked_add(1)
                    .ok_or(StartupError::CountOverflow)?;
            }
        }
    }
    let mut reverse_count = 0usize;
    for entry in reverse.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if key.len() < 49 || !value.value().is_empty() {
            return Err(StartupError::InvalidIndex);
        }
        let row_key: [u8; 32] = key[..32]
            .try_into()
            .map_err(|_| StartupError::InvalidIndex)?;
        let index_id: [u8; 16] = key[32..48]
            .try_into()
            .map_err(|_| StartupError::InvalidIndex)?;
        if row_key[..16] == [0; 16] {
            return Err(StartupError::InvalidIndex);
        }
        let table_id = index_definitions
            .get(&index_id)
            .ok_or(StartupError::InvalidIndex)?;
        if table_id.as_slice() != &row_key[..16] {
            return Err(StartupError::InvalidIndex);
        }
        let mut index_key = key[32..].to_vec();
        index_key.extend_from_slice(&row_key[16..]);
        let locator = indexes
            .get(&index_key[..])
            .map_err(storage_error)?
            .ok_or(StartupError::InvalidIndex)?;
        let locator = IndexLocator::decode_for_key(&index_key, locator.value())
            .map_err(|_| StartupError::InvalidIndex)?;
        let row = rows
            .get(&row_key[..])
            .map_err(storage_error)?
            .ok_or(StartupError::InvalidIndex)?;
        let row = RowRecord::decode(row.value()).map_err(|_| StartupError::InvalidIndex)?;
        if locator.row_version != row.row_version {
            return Err(StartupError::InvalidIndex);
        }
        reverse_count = reverse_count
            .checked_add(1)
            .ok_or(StartupError::CountOverflow)?;
    }
    if reverse_count != count || expected_count != count {
        return Err(StartupError::InvalidIndex);
    }
    Ok(count)
}

fn derive_persisted_index_key(
    index_id: [u8; 16],
    row_id: [u8; 16],
    table_id: [u8; 16],
    payload: &[u8],
    index_columns: &IndexColumns,
    table_schemas: &PersistedTableSchemas,
) -> Result<DerivedIndexKey, StartupError> {
    let schema = table_schemas
        .get(&table_id)
        .ok_or(StartupError::InvalidCatalog)?;
    let index_column_ids = index_columns
        .get(&index_id)
        .ok_or(StartupError::InvalidCatalog)?;
    derive_index_key(
        index_id,
        row_id,
        schema.schema_version,
        &schema.column_ids,
        &schema.columns,
        payload,
        index_column_ids,
    )
    .map_err(|_| StartupError::InvalidIndex)
}

fn validate_outcomes(read: &ReadTransaction, commit_sequence: u64) -> Result<usize, StartupError> {
    let table = read.open_table(OUTCOMES).map_err(storage_error)?;
    let mut count = 0usize;
    for entry in table.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key: [u8; 16] = key
            .value()
            .try_into()
            .map_err(|_| StartupError::InvalidOutcome)?;
        let outcome =
            InvocationOutcome::decode(value.value()).map_err(|_| StartupError::InvalidOutcome)?;
        if outcome.invocation_id != key
            || (outcome.status == OutcomeStatus::Committed
                && outcome.commit_sequence > commit_sequence)
        {
            return Err(StartupError::InvalidOutcome);
        }
        count = count.checked_add(1).ok_or(StartupError::CountOverflow)?;
    }
    Ok(count)
}

fn validate_audit(read: &ReadTransaction) -> Result<usize, StartupError> {
    let table = read.open_table(AUDIT).map_err(storage_error)?;
    let mut count = 0usize;
    for entry in table.iter().map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key: [u8; 16] = key
            .value()
            .try_into()
            .map_err(|_| StartupError::InvalidAudit)?;
        let event = AuditEvent::decode(value.value()).map_err(|_| StartupError::InvalidAudit)?;
        if event.event_id != key {
            return Err(StartupError::InvalidAudit);
        }
        count = count.checked_add(1).ok_or(StartupError::CountOverflow)?;
    }
    Ok(count)
}
