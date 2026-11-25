//! Read-only integrity checks for a recovered redb image.

#[cfg(test)]
mod catalog_index_tests;
mod catalog_indexes;
mod catalog_validation;
mod row_index_validation;

use crate::audit::AuditEvent;
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::outcome::{InvocationOutcome, OutcomeStatus};
use crate::schema::{
    AUDIT, CATALOG, COLUMN_LOOKUPS, DEPENDENCIES, ENGINE_META, GRANTS, INDEX_NAMES, INDEXES,
    INTERACTIVE_TXN_RECOVERY_MAGIC, INTERACTIVE_TXN_RECOVERY_PREFIX, MEMBERSHIPS, OBJECT_OWNERS,
    OUTCOMES, POLICIES, PRINCIPAL_NAMES, RELATION_NAMES, ROW_INDEXES, ROWS, SCHEMA_NAMES, STATS,
    TABLE_CONSTRAINTS, TABLE_INDEXES,
};
use crate::tuple::ColumnSchema;
use catalog_indexes::*;
use catalog_validation::*;
use redb::{
    Database, ReadOnlyDatabase, ReadTransaction, ReadableDatabase, ReadableTable,
    ReadableTableMetadata,
};
use row_index_validation::*;
use std::collections::{HashMap, HashSet};

pub(super) const METADATA_KEY: &[u8] = b"format";
pub(super) const MAX_CATALOG_RECORDS_PER_FAMILY: usize = 65_536;
pub(super) const MAX_FOREIGN_KEYS_PER_DATABASE: usize = 256;
pub(super) const MAX_FOREIGN_KEY_ROW_VISITS_PER_STARTUP: usize = 65_536;
pub(super) type IndexDefinitions = HashMap<[u8; 16], [u8; 16]>;
pub(super) type IndexColumns = HashMap<[u8; 16], Vec<[u8; 16]>>;
pub(super) type UniqueIndexIds = HashSet<[u8; 16]>;

#[derive(Clone, Debug)]
pub(super) struct PersistedTableSchema {
    pub(super) schema_version: u64,
    pub(super) column_ids: Vec<[u8; 16]>,
    pub(super) columns: Vec<ColumnSchema>,
}

#[derive(Clone, Debug)]
pub(super) struct PersistedColumnSchema {
    pub(super) column_id: [u8; 16],
    pub(super) ordinal: u32,
    type_id: [u8; 16],
    parameters: Vec<u8>,
    pub(super) nullable: bool,
    introduced_schema_version: u64,
}

type PersistedTableSchemas = HashMap<[u8; 16], PersistedTableSchema>;

#[derive(Clone, Debug)]
pub(super) struct PersistedForeignKey {
    source_table_id: [u8; 16],
    source_ordinals: Vec<usize>,
    target_index_id: [u8; 16],
    comparison_columns: Vec<ColumnSchema>,
}

pub(super) struct PersistedCatalog {
    record_count: usize,
    index_definitions: IndexDefinitions,
    index_columns: IndexColumns,
    unique_index_ids: UniqueIndexIds,
    table_schemas: PersistedTableSchemas,
    pub(super) foreign_keys: Vec<PersistedForeignKey>,
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
    AuditCapacityExceeded,
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
/// snapshot. This may rebuild stale derived indexes from authoritative catalog
/// rows. It does not certify authenticated service admission.
pub fn validate_persisted_records(
    database: &Database,
) -> Result<StorageHealthReport, StartupError> {
    validate_persisted_records_for_lifecycle(database, Lifecycle::Ready)
}

/// Validate a READY image without rebuilding derived lookup tables or writing
/// to the redb file. This is used by offline backup verification.
pub fn validate_persisted_records_read_only(
    database: &ReadOnlyDatabase,
) -> Result<StorageHealthReport, StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    validate_persisted_records_snapshot(&read, Lifecycle::Ready, None)
}

/// Validate the exact catalog-v1 source format for an offline logical migration.
/// This never rebuilds derived indexes and does not admit the image for service use.
pub fn validate_v1_migration_source_read_only(
    database: &ReadOnlyDatabase,
) -> Result<StorageHealthReport, StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    validate_persisted_records_snapshot(&read, Lifecycle::Ready, Some(1))
}

/// Validate a catalog-v1 staging database without rebuilding derived indexes.
/// The writable handle is accepted only so migration can validate a cloned
/// target before applying its atomic format transaction.
pub fn validate_v1_migration_database(
    database: &Database,
) -> Result<StorageHealthReport, StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    validate_persisted_records_snapshot(&read, Lifecycle::Ready, Some(1))
}

/// Validate one READY image from a logical catalog version supported by an
/// offline migration. This does not admit the image for service use.
pub fn validate_migration_image_read_only(
    database: &ReadOnlyDatabase,
) -> Result<StorageHealthReport, StartupError> {
    match validate_persisted_records_read_only(database) {
        Err(StartupError::UnsupportedFormat) => validate_v1_migration_source_read_only(database),
        result => result,
    }
}

pub(crate) fn validate_initializing_records(
    database: &Database,
) -> Result<StorageHealthReport, StartupError> {
    validate_persisted_records_for_lifecycle(database, Lifecycle::Initializing)
}

fn validate_persisted_records_for_lifecycle(
    database: &Database,
    required_lifecycle: Lifecycle,
) -> Result<StorageHealthReport, StartupError> {
    // Compatibility and lifecycle gates must run before rebuilding disposable
    // indexes so an unsupported image is rejected without database writes.
    {
        let read = database.begin_read().map_err(storage_error)?;
        let metadata = read_metadata(&read)?;
        if !metadata.supports_current_formats() {
            return Err(StartupError::UnsupportedFormat);
        }
        if metadata.lifecycle != required_lifecycle {
            return Err(StartupError::NotReady);
        }
    }
    ensure_principal_name_index(database)?;
    ensure_schema_name_index(database)?;
    ensure_relation_name_index(database)?;
    ensure_column_lookup_index(database)?;
    ensure_index_name_index(database)?;
    ensure_table_index_index(database)?;
    ensure_table_constraint_index(database)?;
    ensure_object_owner_index(database)?;
    ensure_membership_index(database)?;
    ensure_grant_index(database)?;
    ensure_policy_index(database)?;
    ensure_dependency_index(database)?;
    let read = database.begin_read().map_err(storage_error)?;
    validate_persisted_records_snapshot(&read, required_lifecycle, None)
}

fn validate_persisted_records_snapshot(
    read: &ReadTransaction,
    required_lifecycle: Lifecycle,
    required_catalog_version: Option<u32>,
) -> Result<StorageHealthReport, StartupError> {
    validate_tables(read)?;
    validate_name_domains(read)?;
    let metadata = read_metadata(read)?;
    let supported_formats = if let Some(catalog_version) = required_catalog_version {
        metadata.catalog_version == catalog_version
            && metadata.expression_version == 1
            && metadata.builtin_semantics_version == 1
    } else {
        metadata.supports_current_formats()
    };
    if !supported_formats {
        return Err(StartupError::UnsupportedFormat);
    }
    if metadata.lifecycle != required_lifecycle {
        return Err(StartupError::NotReady);
    }

    let catalog = validate_catalog(read, &metadata)?;
    let rows = validate_rows(read, metadata.commit_sequence, &catalog.table_schemas)?;
    let index_entries = validate_indexes(
        read,
        metadata.commit_sequence,
        &catalog.index_definitions,
        &catalog.index_columns,
        &catalog.unique_index_ids,
        &catalog.table_schemas,
    )?;
    validate_foreign_key_rows(read, &catalog.foreign_keys, &catalog.table_schemas)?;
    let outcomes = validate_outcomes(read, metadata.commit_sequence)?;
    let audit_events = validate_audit(read)?;

    Ok(StorageHealthReport {
        metadata,
        catalog_records: catalog.record_count,
        rows,
        index_entries,
        outcomes,
        audit_events,
    })
}

pub(super) fn storage_error(_error: impl std::fmt::Display) -> StartupError {
    StartupError::Storage
}

fn validate_tables(read: &ReadTransaction) -> Result<(), StartupError> {
    drop(read.open_table(ENGINE_META).map_err(storage_error)?);
    drop(read.open_table(ROWS).map_err(storage_error)?);
    drop(read.open_table(INDEXES).map_err(storage_error)?);
    drop(read.open_table(ROW_INDEXES).map_err(storage_error)?);
    drop(read.open_table(CATALOG).map_err(storage_error)?);
    drop(read.open_table(PRINCIPAL_NAMES).map_err(storage_error)?);
    drop(read.open_table(SCHEMA_NAMES).map_err(storage_error)?);
    drop(read.open_table(RELATION_NAMES).map_err(storage_error)?);
    drop(read.open_table(COLUMN_LOOKUPS).map_err(storage_error)?);
    drop(read.open_table(INDEX_NAMES).map_err(storage_error)?);
    drop(read.open_table(TABLE_INDEXES).map_err(storage_error)?);
    drop(read.open_table(TABLE_CONSTRAINTS).map_err(storage_error)?);
    drop(read.open_table(OBJECT_OWNERS).map_err(storage_error)?);
    drop(read.open_table(MEMBERSHIPS).map_err(storage_error)?);
    drop(read.open_table(GRANTS).map_err(storage_error)?);
    drop(read.open_table(POLICIES).map_err(storage_error)?);
    drop(read.open_table(DEPENDENCIES).map_err(storage_error)?);
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
        if key.value() == METADATA_KEY {
            if metadata_bytes.replace(value.value().to_vec()).is_some() {
                return Err(StartupError::InvalidMetadata);
            }
        } else if key.value().starts_with(INTERACTIVE_TXN_RECOVERY_PREFIX) {
            validate_interactive_txn_recovery_record(key.value(), value.value())?;
        } else {
            return Err(StartupError::UnexpectedMetadataKey);
        }
    }
    let bytes = metadata_bytes.ok_or(StartupError::InvalidMetadata)?;
    EngineMetadata::decode(&bytes).map_err(|_| StartupError::InvalidMetadata)
}

fn validate_interactive_txn_recovery_record(key: &[u8], value: &[u8]) -> Result<(), StartupError> {
    const RECORD_BYTES: usize = 4 + 16 + 8;
    if key.len() != INTERACTIVE_TXN_RECOVERY_PREFIX.len() + 16
        || value.len() != RECORD_BYTES
        || value[..4] != INTERACTIVE_TXN_RECOVERY_MAGIC[..]
        || key[INTERACTIVE_TXN_RECOVERY_PREFIX.len()..]
            .iter()
            .all(|byte| *byte == 0)
        || value[4..20].iter().all(|byte| *byte == 0)
        || i64::from_le_bytes(value[20..28].try_into().expect("fixed recovery record")) <= 0
    {
        return Err(StartupError::InvalidMetadata);
    }
    Ok(())
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
    if table.len().map_err(storage_error)? > crate::audit::MAX_AUDIT_EVENTS as u64 {
        return Err(StartupError::AuditCapacityExceeded);
    }
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
