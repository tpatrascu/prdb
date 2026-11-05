//! Read-only integrity checks for a recovered redb image.

use crate::audit::AuditEvent;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, Membership, validate_membership_graph};
use crate::catalog::{CatalogRecord, decode_registered_envelope};
use crate::expression::decode_catalog_expression_record;
use crate::index_locator::IndexLocator;
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::outcome::{InvocationOutcome, OutcomeStatus};
use crate::row_record::RowRecord;
use crate::schema::{AUDIT, CATALOG, ENGINE_META, INDEXES, OUTCOMES, ROW_INDEXES, ROWS, STATS};
use redb::{Database, ReadTransaction, ReadableDatabase, ReadableTable};
use std::collections::HashMap;

const METADATA_KEY: &[u8] = b"format";
const MAX_CATALOG_RECORDS_PER_FAMILY: usize = 65_536;
type IndexDefinitions = HashMap<[u8; 16], [u8; 16]>;

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

    let (catalog_records, index_definitions) = validate_catalog(&read, &metadata)?;
    let rows = validate_rows(&read, metadata.commit_sequence)?;
    let index_entries = validate_indexes(&read, metadata.commit_sequence, &index_definitions)?;
    let outcomes = validate_outcomes(&read, metadata.commit_sequence)?;
    let audit_events = validate_audit(&read)?;

    Ok(StorageHealthReport {
        metadata,
        catalog_records,
        rows,
        index_entries,
        outcomes,
        audit_events,
    })
}

fn storage_error(_error: impl std::fmt::Display) -> StartupError {
    StartupError::Storage
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
) -> Result<(usize, IndexDefinitions), StartupError> {
    let table = read.open_table(CATALOG).map_err(storage_error)?;
    let mut family_counts = [0usize; 17];
    let mut count = 0usize;
    let mut index_definitions = HashMap::new();
    let mut principal_kinds = HashMap::<[u8; 16], u16>::new();
    let mut memberships = Vec::<Membership>::new();
    let mut membership_grantors = Vec::<[u8; 16]>::new();
    let mut database_records = 0usize;
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
        if record.family_code == 1 {
            database_records += 1;
            validate_database_record(&record, metadata)?;
        }
        if record.family_code == 7 {
            decode_catalog_expression_record(&record).map_err(|_| StartupError::InvalidCatalog)?;
        }
        if record.family_code == 9 {
            let index_id: [u8; 16] = key
                .get(3..19)
                .ok_or(StartupError::InvalidCatalog)?
                .try_into()
                .map_err(|_| StartupError::InvalidCatalog)?;
            let table_id = field_id(&record, 1)?;
            if index_definitions.insert(index_id, table_id).is_some() {
                return Err(StartupError::InvalidCatalog);
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
            if !matches!(kind, 1 | 2)
                || (kind == 2 && login_enabled)
                || field_u64(&record, 4)? == 0
                || principal_kinds.insert(principal_id, kind).is_some()
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
        }
    }
    if database_records != 1 {
        return Err(StartupError::InvalidCatalog);
    }
    if membership_grantors
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
    Ok((count, index_definitions))
}

fn validate_database_record(
    record: &CatalogRecord,
    metadata: &EngineMetadata,
) -> Result<(), StartupError> {
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
    Ok(())
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

fn validate_rows(read: &ReadTransaction, commit_sequence: u64) -> Result<usize, StartupError> {
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
        count = count.checked_add(1).ok_or(StartupError::CountOverflow)?;
    }
    Ok(count)
}

fn validate_indexes(
    read: &ReadTransaction,
    commit_sequence: u64,
    index_definitions: &IndexDefinitions,
) -> Result<usize, StartupError> {
    let indexes = read.open_table(INDEXES).map_err(storage_error)?;
    let rows = read.open_table(ROWS).map_err(storage_error)?;
    let reverse = read.open_table(ROW_INDEXES).map_err(storage_error)?;
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
        if locator.row_version != row.row_version {
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
    if reverse_count != count {
        return Err(StartupError::InvalidIndex);
    }
    Ok(count)
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
