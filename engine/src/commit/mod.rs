//! Serialized row publication with optimistic row-version validation.

mod catalog_graph_validation;
mod catalog_invariant_validation;
mod constraint_validation;
#[cfg(test)]
mod durable_commit_gate_tests;
mod execution;
mod fenced_ddl;
mod guards_constraints;
mod guards_foreign_keys;
mod guards_identity;
mod guards_index_grant_policy;
mod guards_schema_table;
mod guards_table_column;
mod index_ddl;

pub use execution::{commit_rows, commit_rows_and_indexes, commit_storage_changes};
pub(crate) use execution::{commit_storage_changes_with_gate, record_aborted_outcome};
pub(crate) use fenced_ddl::commit_authorized_table_truncate_ddl_with_fences;
pub use fenced_ddl::{
    commit_authorized_column_drop_ddl_with_fences, commit_authorized_column_rename_ddl_with_fences,
    commit_authorized_constraint_ddl_with_fences, commit_authorized_foreign_key_ddl_with_fences,
    commit_authorized_identity_ddl_with_fences, commit_authorized_index_ddl_with_fences,
    commit_authorized_membership_ddl_with_fences, commit_authorized_object_grant_ddl_with_fences,
    commit_authorized_policy_ddl_with_fences, commit_authorized_schema_create_ddl_with_fences,
    commit_authorized_schema_drop_ddl_with_fences, commit_authorized_schema_owner_ddl_with_fences,
    commit_authorized_schema_rename_ddl_with_fences,
    commit_authorized_system_privilege_ddl_with_fences,
    commit_authorized_table_create_ddl_with_fences, commit_authorized_table_drop_ddl_with_fences,
    commit_authorized_table_owner_ddl_with_fences, commit_authorized_table_rename_ddl_with_fences,
    commit_catalog_and_audit, commit_catalog_and_audit_with_fences,
};

use crate::audit::AuditEvent;
use crate::authorization::Membership;
use crate::catalog::decode_registered_envelope;
use crate::expression::decode_catalog_expression_record;
use crate::tuple::ColumnSchema;
use catalog_invariant_validation::*;
use constraint_validation::*;
use guards_identity::*;
use guards_index_grant_policy::*;
use guards_schema_table::*;
use guards_table_column::*;
use index_ddl::*;
use redb::{Database, ReadableTable};
use std::collections::{HashMap, HashSet};

pub(super) type IndexTableMap = HashMap<[u8; 16], [u8; 16]>;
pub(super) type ExpectedTableRows = HashMap<[u8; 16], Vec<([u8; 32], u64)>>;
pub(super) const MAX_FOREIGN_KEY_ROW_VISITS_PER_COMMIT: usize = 65_536;
pub(super) const MAX_FOREIGN_KEY_COMPARISON_KEY_BYTES_PER_COMMIT: usize = 16 * 1024 * 1024;
pub(super) const MAX_INDEX_DDL_ROWS_PER_COMMIT: usize = 65_536;
pub(super) const MAX_INDEX_DDL_ENTRIES_PER_COMMIT: usize = 65_536;
pub(super) const MAX_INDEX_DDL_BYTES_PER_COMMIT: usize = 64 * 1024 * 1024;
pub(super) const MAX_NOT_NULL_VALIDATION_ROWS_PER_COMMIT: usize = 65_536;
pub(super) const MAX_CHECK_VALIDATION_EVALUATIONS_PER_COMMIT: usize = 65_536;

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
pub(super) struct RetainedIndexMutation {
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
pub(super) struct FinalTableSchema {
    pub(super) schema_version: u64,
    pub(super) column_ids: Vec<[u8; 16]>,
    pub(super) columns: Vec<ColumnSchema>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct IndexDdlPlan {
    index_tables: HashMap<[u8; 16], [u8; 16]>,
    created_indexes: HashSet<[u8; 16]>,
    dropped_indexes: HashSet<[u8; 16]>,
    rebuilt_indexes: HashSet<[u8; 16]>,
    expected_rows: ExpectedTableRows,
    row_versions: HashMap<[u8; 32], u64>,
    writes: Vec<IndexWrite>,
    schema_drop_id: Option<[u8; 16]>,
    table_drop_id: Option<[u8; 16]>,
    row_changes: Vec<RowWrite>,
    schema_rewrite_tables: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug)]
pub(super) struct IndexDdlDefinition {
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
pub(super) struct CatalogCommitRequest<'a> {
    pub(super) invocation_id: [u8; 16],
    pub(super) principal_id: [u8; 16],
    pub(super) retention_deadline_us: i64,
    pub(super) catalog_writes: &'a [CatalogWrite],
    pub(super) audit_events: &'a [AuditEvent],
}

pub(crate) struct TableTruncateCommitRequest<'a> {
    pub invocation_id: [u8; 16],
    pub principal_id: [u8; 16],
    pub table_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub rows: &'a [RowWrite],
    pub indexes: &'a [IndexWrite],
    pub catalog_writes: &'a [CatalogWrite],
    pub audit_events: &'a [AuditEvent],
}

pub(crate) fn validate_table_truncate_ddl_authority(
    database: &Database,
    principal_id: [u8; 16],
    table_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<(), CommitError> {
    if !catalog_writes
        .iter()
        .any(|write| write.key == id_catalog_key(5, table_id))
    {
        return Err(CommitError::InvalidCatalog);
    }
    table_rename_ddl_authority_guards(
        database,
        principal_id,
        catalog_writes,
        audit_events,
        TableObjectMutationKind::Truncate,
    )
    .map(|_| ())
}

#[derive(Clone, Copy)]
pub(super) enum CatalogAuthorizationMode {
    Trusted,
    IndexDdl,
    IdentityDdl,
    PolicyDdl,
    ConstraintDdl,
    ForeignKeyDdl,
    MembershipDdl,
    SystemPrivilegeDdl,
    SchemaCreateDdl,
    SchemaDropDdl,
    SchemaOwnerTransferDdl,
    SchemaRenameDdl,
    TableDropDdl,
    TableRenameDdl,
    TableOwnerTransferDdl,
    ColumnRenameDdl,
    ColumnDropDdl,
    TableCreateDdl,
    ObjectGrantDdl,
}

#[derive(Clone, Copy)]
pub(super) enum TableObjectMutationKind {
    Rename,
    OwnerTransfer,
    Truncate,
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
    NotNullViolation,
    NotNullValidationLimitExceeded,
    CheckViolation,
    ConstraintValidationLimitExceeded,
    ForeignKeyViolation,
    ForeignKeyValidationLimitExceeded,
    IndexDdlLimitExceeded,
    DdlFenceUnavailable,
    SchemaNotEmpty,
    TableHasDependencies,
    ColumnHasDependencies,
    TableDropLimitExceeded,
    TableTruncateLimitExceeded,
    ColumnAddLimitExceeded,
    DdlAuthorizationDenied,
    DuplicateConstraintName,
    InvocationExpired,
    InvocationCancelled,
    InvalidCatalog,
    InvalidAudit,
    AuditAlreadyExists,
    AuditCapacityExceeded,
    SequenceExhausted,
    Storage(String),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "storage commit failed: {self:?}")
    }
}

impl std::error::Error for CommitError {}

pub(super) fn storage_error(error: impl std::fmt::Display) -> CommitError {
    CommitError::Storage(error.to_string())
}

pub(super) fn decode_catalog_record(
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

pub(super) fn decode_membership_record(
    key: &[u8],
    bytes: &[u8],
) -> Result<Membership, CommitError> {
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

pub(super) fn final_catalog_value<T>(
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

pub(super) fn catalog_field_bytes(
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

pub(super) fn catalog_optional_field_bytes(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<Option<Vec<u8>>, CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.family_code != family || record.logical_key != key {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.clone()))
}

pub(super) fn catalog_id_list_field(
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

pub(super) fn id_catalog_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

pub(super) fn catalog_id_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

pub(super) fn catalog_field_u64(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u64, CommitError> {
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

pub(super) fn catalog_field_u16(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u16, CommitError> {
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

pub(super) fn catalog_field_u32(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u32, CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != family {
        return Err(CommitError::InvalidCatalog);
    }
    let field = record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .ok_or(CommitError::InvalidCatalog)?;
    Ok(u32::from_le_bytes(
        field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ))
}

pub(super) fn is_canonical_catalog_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && (name[0].is_ascii_lowercase() || name[0] == b'_')
        && name
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

pub(super) fn policy_table_id(key: &[u8], bytes: &[u8]) -> Result<[u8; 16], CommitError> {
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

pub(super) fn column_table_id(key: &[u8], bytes: &[u8]) -> Result<[u8; 16], CommitError> {
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

pub(super) fn constraint_table_id(key: &[u8], bytes: &[u8]) -> Result<[u8; 16], CommitError> {
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

pub(super) fn policy_authority_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    catalog_field_u64(key, bytes, 13, 7)
}

pub(super) fn table_write_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    catalog_field_u64(key, bytes, 5, 3)
}

pub(super) fn table_schema_version(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
    catalog_field_u64(key, bytes, 5, 2)
}

pub(super) fn membership_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
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

pub(super) fn grant_authority_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
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

pub(super) fn principal_authority_epoch(key: &[u8], bytes: &[u8]) -> Result<u64, CommitError> {
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
