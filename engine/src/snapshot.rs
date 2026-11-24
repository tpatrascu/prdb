//! Read-only row access through an invocation's retained redb snapshot.
use crate::catalog::{CatalogRecord, InvalidCatalogRecord, decode_registered_envelope};
use crate::expression::decode_catalog_expression_record;
use crate::index_locator::{IndexLocator, LocatorError};
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::schema::{
    CATALOG, ENGINE_META, INDEXES, POLICIES, RELATION_NAMES, ROWS, SCHEMA_NAMES, STATS,
};
use crate::statistics::{COMMITTED_POPULATION_KEY_PREFIX, CommittedPopulationRecord};
use redb::ReadTransaction;

pub const MAX_CATALOG_FAMILY_SCAN: usize = 65_536;
pub(crate) const MAX_ROW_SCAN_PAGE: usize = 1_024;
const MAX_INDEX_SCAN_PAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SnapshotError {
    InvalidTableId,
    InvalidCursor,
    InvalidScanLimit,
    InvalidRowKey,
    InvalidIndexPrefix,
    InvalidIndexCursor,
    InvalidIndexKey,
    InvalidIndexLocator,
    InvalidCatalogRecord,
    RecordLimitExceeded,
    Storage(String),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "snapshot scan failed: {self:?}")
    }
}

impl std::error::Error for SnapshotError {}

/// Owns one redb read transaction for the lifetime of the invocation snapshot.
pub struct RowSnapshot {
    transaction: ReadTransaction,
}

/// One validated row/version candidate returned by an ordered index prefix scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexCandidate {
    pub key: Vec<u8>,
    pub locator: IndexLocator,
}

/// A bounded page from one pinned index prefix. A continuation is the last
/// complete key; pass it back to resume strictly after that key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexScanPage {
    pub candidates: Vec<IndexCandidate>,
    pub continuation: Option<Vec<u8>>,
}

/// One inclusive or exclusive bound expressed as the ordered SQL tuple bytes
/// following an index ID in a persisted index key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRangeBound {
    pub tuple: Vec<u8>,
    pub inclusive: bool,
}

impl RowSnapshot {
    pub(crate) fn read_transaction(&self) -> &ReadTransaction {
        &self.transaction
    }

    pub fn new(transaction: ReadTransaction) -> Self {
        Self { transaction }
    }

    /// Copy engine metadata from the same retained read transaction as catalog
    /// and row access.
    pub fn get_engine_metadata_value(&self) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let table = self.transaction.open_table(ENGINE_META)?;
        Ok(table
            .get(&b"format"[..])?
            .map(|value| value.value().to_vec()))
    }

    /// Read a protected committed-population record only when it is fresh for
    /// this exact pinned snapshot. Callers must still prove that the estimate
    /// matches the invocation's authorized view and contains no private writes.
    pub(crate) fn committed_population_record(
        &self,
        table_id: [u8; 16],
    ) -> Result<Option<CommittedPopulationRecord>, SnapshotError> {
        if table_id == [0; 16] {
            return Ok(None);
        }
        let metadata_bytes = self
            .get_engine_metadata_value()
            .map_err(|error| SnapshotError::Storage(error.to_string()))?
            .ok_or_else(|| SnapshotError::Storage("missing engine metadata".into()))?;
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?;
        if metadata.lifecycle != Lifecycle::Ready {
            return Ok(None);
        }

        let mut key = Vec::with_capacity(COMMITTED_POPULATION_KEY_PREFIX.len() + table_id.len());
        key.extend_from_slice(COMMITTED_POPULATION_KEY_PREFIX);
        key.extend_from_slice(&table_id);
        let stats = self
            .transaction
            .open_table(STATS)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?;
        let Some(value) = stats
            .get(key.as_slice())
            .map_err(|error| SnapshotError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let record = CommittedPopulationRecord::decode_record(table_id, value.value())
            .map_err(|error| SnapshotError::Storage(error.to_string()))?;
        Ok((record.snapshot_sequence() == metadata.commit_sequence).then_some(record))
    }

    /// Copy one row value out of its redb guard. No guard or database borrow escapes.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let table = self.transaction.open_table(ROWS)?;
        Ok(table.get(key)?.map(|value| value.value().to_vec()))
    }

    /// Return the roles granted directly to a member through the validated
    /// snapshot index. The lookup checks each derived entry against family 11.
    pub(crate) fn lookup_roles_for_member(
        &self,
        member_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, SnapshotError> {
        crate::catalog_index::lookup_roles_for_member(&self.transaction, member_id).map_err(
            |error| match error {
                crate::catalog_index::CatalogIndexError::Storage(error) => {
                    SnapshotError::Storage(error)
                }
                crate::catalog_index::CatalogIndexError::InvalidName
                | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                    SnapshotError::InvalidCatalogRecord
                }
                crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                    SnapshotError::InvalidCatalogRecord
                }
            },
        )
    }

    pub(crate) fn scan_relation_ids_for_schema(
        &self,
        schema_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<[u8; 16]>, SnapshotError> {
        crate::catalog_index::scan_relation_ids_for_schema(
            &self.transaction,
            schema_id,
            max_records,
        )
        .map_err(|error| match error {
            crate::catalog_index::CatalogIndexError::InvalidName
            | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                SnapshotError::InvalidCatalogRecord
            }
            crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                SnapshotError::RecordLimitExceeded
            }
            crate::catalog_index::CatalogIndexError::Storage(error) => {
                SnapshotError::Storage(error)
            }
        })
    }

    pub(crate) fn scan_object_ids_for_owner(
        &self,
        owner_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<[u8; 16]>, SnapshotError> {
        crate::catalog_index::scan_object_ids_for_owner(&self.transaction, owner_id, max_records)
            .map_err(|error| match error {
                crate::catalog_index::CatalogIndexError::InvalidName
                | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                    SnapshotError::InvalidCatalogRecord
                }
                crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                    SnapshotError::RecordLimitExceeded
                }
                crate::catalog_index::CatalogIndexError::Storage(error) => {
                    SnapshotError::Storage(error)
                }
            })
    }

    /// List one table's columns in ordinal order from this pinned snapshot.
    pub(crate) fn scan_column_ids_for_table(
        &self,
        table_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<[u8; 16]>, SnapshotError> {
        crate::catalog_index::scan_column_ids_for_table(&self.transaction, table_id, max_records)
            .map_err(|error| match error {
                crate::catalog_index::CatalogIndexError::InvalidName
                | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                    SnapshotError::InvalidCatalogRecord
                }
                crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                    SnapshotError::RecordLimitExceeded
                }
                crate::catalog_index::CatalogIndexError::Storage(error) => {
                    SnapshotError::Storage(error)
                }
            })
    }

    /// List one table's index definitions from this pinned snapshot.
    pub(crate) fn scan_index_ids_for_table(
        &self,
        table_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<[u8; 16]>, SnapshotError> {
        crate::catalog_index::scan_index_ids_for_table(&self.transaction, table_id, max_records)
            .map_err(|error| match error {
                crate::catalog_index::CatalogIndexError::InvalidName
                | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                    SnapshotError::InvalidCatalogRecord
                }
                crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                    SnapshotError::RecordLimitExceeded
                }
                crate::catalog_index::CatalogIndexError::Storage(error) => {
                    SnapshotError::Storage(error)
                }
            })
    }

    /// List one table's constraints from this pinned snapshot.
    pub(crate) fn scan_constraint_ids_for_table(
        &self,
        table_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<[u8; 16]>, SnapshotError> {
        crate::catalog_index::scan_constraint_ids_for_table(
            &self.transaction,
            table_id,
            max_records,
        )
        .map_err(|error| match error {
            crate::catalog_index::CatalogIndexError::InvalidName
            | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                SnapshotError::InvalidCatalogRecord
            }
            crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                SnapshotError::RecordLimitExceeded
            }
            crate::catalog_index::CatalogIndexError::Storage(error) => {
                SnapshotError::Storage(error)
            }
        })
    }

    pub(crate) fn lookup_dependents_for_referenced(
        &self,
        referenced_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<CatalogRecord>, SnapshotError> {
        crate::catalog_index::lookup_dependents_for_referenced_limited(
            &self.transaction,
            referenced_id,
            max_records,
        )
        .map_err(|error| match error {
            crate::catalog_index::CatalogIndexError::InvalidName
            | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                SnapshotError::InvalidCatalogRecord
            }
            crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                SnapshotError::RecordLimitExceeded
            }
            crate::catalog_index::CatalogIndexError::Storage(error) => {
                SnapshotError::Storage(error)
            }
        })
    }

    pub(crate) fn lookup_dependencies_for_dependent(
        &self,
        dependent_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<CatalogRecord>, SnapshotError> {
        crate::catalog_index::lookup_dependencies_for_dependent_limited(
            &self.transaction,
            dependent_id,
            max_records,
        )
        .map_err(|error| match error {
            crate::catalog_index::CatalogIndexError::InvalidName
            | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                SnapshotError::InvalidCatalogRecord
            }
            crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                SnapshotError::RecordLimitExceeded
            }
            crate::catalog_index::CatalogIndexError::Storage(error) => {
                SnapshotError::Storage(error)
            }
        })
    }

    /// Return grants indexed under one grantee from the pinned catalog
    /// snapshot. Each derived entry is checked against family 12.
    pub(crate) fn lookup_grants_for_grantee(
        &self,
        grantee_id: [u8; 16],
    ) -> Result<Vec<CatalogRecord>, SnapshotError> {
        crate::catalog_index::lookup_grants_for_grantee(&self.transaction, grantee_id).map_err(
            |error| match error {
                crate::catalog_index::CatalogIndexError::Storage(error) => {
                    SnapshotError::Storage(error)
                }
                crate::catalog_index::CatalogIndexError::InvalidName
                | crate::catalog_index::CatalogIndexError::InvalidEntry => {
                    SnapshotError::InvalidCatalogRecord
                }
                crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                    SnapshotError::InvalidCatalogRecord
                }
            },
        )
    }

    /// Return grants indexed under one object from the pinned catalog
    /// snapshot. Each derived entry is checked against family 12.
    pub(crate) fn lookup_grants_for_object(
        &self,
        object_id: [u8; 16],
    ) -> Result<Vec<CatalogRecord>, SnapshotError> {
        crate::catalog_index::lookup_grants_for_object(&self.transaction, object_id).map_err(
            |error| match error {
                crate::catalog_index::CatalogIndexError::Storage(error) => {
                    SnapshotError::Storage(error)
                }
                crate::catalog_index::CatalogIndexError::InvalidName
                | crate::catalog_index::CatalogIndexError::InvalidEntry
                | crate::catalog_index::CatalogIndexError::RecordLimitExceeded => {
                    SnapshotError::InvalidCatalogRecord
                }
            },
        )
    }

    /// Return a bounded, ordered page of row IDs from this pinned snapshot.
    /// A full page is reported as incomplete; callers can resume after its last
    /// row ID, and an empty follow-up page confirms completion.
    pub(crate) fn scan_row_ids_after(
        &self,
        table_id: [u8; 16],
        after_row_id: Option<[u8; 16]>,
        limit: usize,
    ) -> Result<(Vec<[u8; 16]>, bool), SnapshotError> {
        if table_id == [0; 16] {
            return Err(SnapshotError::InvalidTableId);
        }
        if after_row_id.is_some_and(|row_id| row_id == [0; 16]) {
            return Err(SnapshotError::InvalidCursor);
        }
        if limit == 0 || limit > MAX_ROW_SCAN_PAGE {
            return Err(SnapshotError::InvalidScanLimit);
        }

        let mut start_key = Vec::with_capacity(32);
        start_key.extend_from_slice(&table_id);
        if let Some(row_id) = after_row_id {
            start_key.extend_from_slice(&row_id);
        }
        let table = self
            .transaction
            .open_table(ROWS)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?;
        let mut row_ids = Vec::with_capacity(limit);
        for entry in table
            .range::<&[u8]>(start_key.as_slice()..)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?
        {
            let (key, _) = entry.map_err(|error| SnapshotError::Storage(error.to_string()))?;
            let key = key.value();
            if !key.starts_with(&table_id) {
                break;
            }
            if key.len() != 32 {
                return Err(SnapshotError::InvalidRowKey);
            }
            let row_id: [u8; 16] = key[16..]
                .try_into()
                .map_err(|_| SnapshotError::InvalidRowKey)?;
            if row_id == [0; 16] {
                return Err(SnapshotError::InvalidRowKey);
            }
            if after_row_id.is_some_and(|after| row_id <= after) {
                continue;
            }
            row_ids.push(row_id);
            if row_ids.len() == limit {
                break;
            }
        }
        let complete = row_ids.len() < limit;
        Ok((row_ids, complete))
    }

    /// Read one logical index locator from this invocation's pinned snapshot.
    pub fn get_index_version(&self, key: &[u8]) -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let table = self.transaction.open_table(INDEXES)?;
        let value = table.get(key)?.map(|value| value.value().to_vec());
        value
            .map(|bytes| {
                IndexLocator::decode_for_key(key, &bytes)
                    .map(|locator| locator.row_version)
                    .map_err(Into::into)
            })
            .transpose()
    }

    /// Return a bounded page from one ordered logical index range. Bounds are
    /// encoded SQL tuples, not complete keys; all row-ID suffixes for an
    /// inclusive tuple are included. The caller must resolve every candidate
    /// against the same snapshot/overlay and apply authorization.
    pub fn scan_index_range(
        &self,
        index_id: &[u8; 16],
        lower: Option<IndexRangeBound>,
        upper: Option<IndexRangeBound>,
        after_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<IndexScanPage, Box<dyn std::error::Error>> {
        if *index_id == [0; 16] {
            return Err(Box::new(SnapshotError::InvalidIndexPrefix));
        }
        if limit == 0 || limit > MAX_ROW_SCAN_PAGE {
            return Err(Box::new(SnapshotError::InvalidScanLimit));
        }
        let validate_bound = |bound: &IndexRangeBound| {
            !bound.tuple.is_empty()
                && 16usize.saturating_add(bound.tuple.len()).saturating_add(16)
                    <= crate::index_key::MAX_INDEX_KEY_BYTES
        };
        if lower.as_ref().is_some_and(|bound| !validate_bound(bound))
            || upper.as_ref().is_some_and(|bound| !validate_bound(bound))
        {
            return Err(Box::new(SnapshotError::InvalidIndexPrefix));
        }

        let mut start = match &lower {
            Some(bound) => {
                let mut key = Vec::with_capacity(16 + bound.tuple.len());
                key.extend_from_slice(index_id);
                key.extend_from_slice(&bound.tuple);
                if bound.inclusive {
                    key
                } else if let Some(successor) = crate::index_key::prefix_successor(&key) {
                    successor
                } else {
                    return Ok(IndexScanPage {
                        candidates: Vec::new(),
                        continuation: None,
                    });
                }
            }
            None => index_id.to_vec(),
        };
        let stop = match &upper {
            Some(bound) => {
                let mut key = Vec::with_capacity(16 + bound.tuple.len());
                key.extend_from_slice(index_id);
                key.extend_from_slice(&bound.tuple);
                if bound.inclusive {
                    crate::index_key::prefix_successor(&key)
                } else {
                    Some(key)
                }
            }
            None => crate::index_key::prefix_successor(index_id),
        };
        if stop.as_ref().is_some_and(|stop| start >= *stop) {
            return Ok(IndexScanPage {
                candidates: Vec::new(),
                continuation: None,
            });
        }
        if after_key.is_some_and(|key| {
            key.len() < 32
                || key.len() > crate::index_key::MAX_INDEX_KEY_BYTES
                || key[..16] != index_id[..]
                || key < start.as_slice()
                || stop.as_ref().is_some_and(|stop| key >= stop.as_slice())
        }) {
            return Err(Box::new(SnapshotError::InvalidIndexCursor));
        }
        if let Some(after_key) = after_key {
            start = after_key.to_vec();
        }

        let table = self.transaction.open_table(INDEXES)?;
        let mut candidates = Vec::with_capacity(limit);
        let mut page_bytes = 0usize;
        let mut has_more = false;
        for entry in table.range::<&[u8]>(start.as_slice()..)? {
            let (key, value) = entry?;
            let key = key.value();
            if key[..16] != index_id[..] || stop.as_ref().is_some_and(|stop| key >= stop.as_slice())
            {
                break;
            }
            if after_key.is_some_and(|after| key <= after) {
                continue;
            }
            if candidates.len() == limit
                || page_bytes.saturating_add(key.len()) > MAX_INDEX_SCAN_PAGE_BYTES
            {
                has_more = true;
                break;
            }
            if key.len() < 32 || key.len() > crate::index_key::MAX_INDEX_KEY_BYTES {
                return Err(Box::new(SnapshotError::InvalidIndexKey));
            }
            let locator = IndexLocator::decode_for_key(key, value.value())
                .map_err(|_: LocatorError| SnapshotError::InvalidIndexLocator)?;
            if locator.row_id == [0; 16] {
                return Err(Box::new(SnapshotError::InvalidIndexLocator));
            }
            candidates.push(IndexCandidate {
                key: key.to_vec(),
                locator,
            });
            page_bytes += key.len();
        }
        let continuation = has_more
            .then(|| candidates.last().map(|candidate| candidate.key.clone()))
            .flatten();
        Ok(IndexScanPage {
            candidates,
            continuation,
        })
    }

    /// Return a bounded page of validated locators whose complete keys share
    /// `prefix`. Prefixes are search bounds, not stored keys. The returned
    /// candidates are untrusted until the transaction layer resolves them
    /// against this same snapshot/overlay and applies authorization.
    pub fn scan_index_prefix(
        &self,
        prefix: &[u8],
        after_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<IndexScanPage, Box<dyn std::error::Error>> {
        if prefix.len() < 16
            || prefix.len().saturating_add(16) > crate::index_key::MAX_INDEX_KEY_BYTES
            || prefix[..16] == [0; 16]
        {
            return Err(Box::new(SnapshotError::InvalidIndexPrefix));
        }
        if limit == 0 || limit > MAX_ROW_SCAN_PAGE {
            return Err(Box::new(SnapshotError::InvalidScanLimit));
        }
        if after_key.is_some_and(|key| {
            !key.starts_with(prefix)
                || key.len() < prefix.len() + 16
                || key.len() > crate::index_key::MAX_INDEX_KEY_BYTES
        }) {
            return Err(Box::new(SnapshotError::InvalidIndexCursor));
        }

        let start_key = after_key.unwrap_or(prefix);
        let table = self.transaction.open_table(INDEXES)?;
        let mut candidates = Vec::with_capacity(limit);
        let mut page_bytes = 0usize;
        let mut has_more = false;
        for entry in table.range::<&[u8]>(start_key..)? {
            let (key, value) = entry?;
            let key = key.value();
            if !key.starts_with(prefix) {
                break;
            }
            if after_key.is_some_and(|after| key <= after) {
                continue;
            }
            if candidates.len() == limit
                || page_bytes.saturating_add(key.len()) > MAX_INDEX_SCAN_PAGE_BYTES
            {
                has_more = true;
                break;
            }
            if key.len() < prefix.len() + 16 || key.len() > crate::index_key::MAX_INDEX_KEY_BYTES {
                return Err(Box::new(SnapshotError::InvalidIndexKey));
            }
            let locator = IndexLocator::decode_for_key(key, value.value())
                .map_err(|_: LocatorError| SnapshotError::InvalidIndexLocator)?;
            if locator.row_id == [0; 16] {
                return Err(Box::new(SnapshotError::InvalidIndexLocator));
            }
            candidates.push(IndexCandidate {
                key: key.to_vec(),
                locator,
            });
            page_bytes += key.len();
        }
        let continuation = has_more
            .then(|| candidates.last().map(|candidate| candidate.key.clone()))
            .flatten();
        Ok(IndexScanPage {
            candidates,
            continuation,
        })
    }

    /// Copy and validate one logical catalog record from this pinned snapshot.
    pub fn get_catalog_record(
        &self,
        key: &[u8],
    ) -> Result<Option<CatalogRecord>, Box<dyn std::error::Error>> {
        self.get_catalog_value(key)?
            .map(|bytes| {
                let record = decode_registered_envelope(&bytes)?;
                if record.logical_key != key {
                    return Err(InvalidCatalogRecord.into());
                }
                if record.family_code == 7 {
                    decode_catalog_expression_record(&record)?;
                }
                Ok(record)
            })
            .transpose()
    }

    /// Copy one raw catalog envelope from this pinned snapshot.
    pub fn get_catalog_value(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let table = self.transaction.open_table(CATALOG)?;
        Ok(table.get(key)?.map(|value| value.value().to_vec()))
    }

    /// Copy an exact schema-name index entry from this pinned snapshot.
    pub fn get_schema_name_index_value(
        &self,
        name: &[u8],
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let table = self.transaction.open_table(SCHEMA_NAMES)?;
        Ok(table.get(name)?.map(|value| value.value().to_vec()))
    }

    /// Copy one schema-scoped relation-name index entry from this snapshot.
    pub fn get_relation_name_index_value(
        &self,
        schema_id: [u8; 16],
        name: &[u8],
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut key = Vec::with_capacity(schema_id.len() + name.len());
        key.extend_from_slice(&schema_id);
        key.extend_from_slice(name);
        let table = self.transaction.open_table(RELATION_NAMES)?;
        Ok(table
            .get(key.as_slice())?
            .map(|value| value.value().to_vec()))
    }

    /// List policy IDs for one table in operation/policy order from this snapshot.
    pub fn scan_policy_ids_for_table(
        &self,
        table_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<[u8; 16]>, Box<dyn std::error::Error>> {
        self.scan_policy_ids_for_table_limited(table_id, max_records)
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
    }

    pub(crate) fn scan_policy_ids_for_table_limited(
        &self,
        table_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<[u8; 16]>, SnapshotError> {
        if table_id == [0; 16] || max_records == 0 || max_records > MAX_CATALOG_FAMILY_SCAN {
            return Err(SnapshotError::InvalidCatalogRecord);
        }
        let table = self
            .transaction
            .open_table(POLICIES)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?;
        let mut ids = Vec::new();
        for entry in table
            .range::<&[u8]>(table_id.as_slice()..)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?
        {
            let (key, value) = entry.map_err(|error| SnapshotError::Storage(error.to_string()))?;
            let key = key.value();
            if !key.starts_with(&table_id) {
                break;
            }
            if !value.value().is_empty()
                || key.len() != 34
                || !matches!(u16::from_be_bytes([key[16], key[17]]), 1..=4)
            {
                return Err(SnapshotError::InvalidCatalogRecord);
            }
            let policy_id: [u8; 16] = key[18..]
                .try_into()
                .map_err(|_| SnapshotError::InvalidCatalogRecord)?;
            if policy_id == [0; 16] {
                return Err(SnapshotError::InvalidCatalogRecord);
            }
            if ids.len() == max_records {
                return Err(SnapshotError::RecordLimitExceeded);
            }
            ids.push(policy_id);
        }
        Ok(ids)
    }

    /// Copy the bounded target records for one policy from this pinned
    /// snapshot. The family-14 key begins with the policy ID, so unrelated
    /// policies do not contribute to the scan limit.
    pub fn scan_policy_targets_for_policy(
        &self,
        policy_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<CatalogRecord>, Box<dyn std::error::Error>> {
        self.scan_policy_targets_for_policy_limited(policy_id, max_records)
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
    }

    pub(crate) fn scan_policy_targets_for_policy_limited(
        &self,
        policy_id: [u8; 16],
        max_records: usize,
    ) -> Result<Vec<CatalogRecord>, SnapshotError> {
        if policy_id == [0; 16] || max_records == 0 || max_records > MAX_CATALOG_FAMILY_SCAN {
            return Err(SnapshotError::InvalidCatalogRecord);
        }
        let mut prefix = 14u16.to_be_bytes().to_vec();
        prefix.push(1);
        prefix.extend_from_slice(&policy_id);
        let table = self
            .transaction
            .open_table(CATALOG)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?;
        let mut records = Vec::new();
        for entry in table
            .range(prefix.as_slice()..)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?
        {
            let (key, value) = entry.map_err(|error| SnapshotError::Storage(error.to_string()))?;
            let key = key.value().to_vec();
            if !key.starts_with(&prefix) {
                break;
            }
            if records.len() == max_records {
                return Err(SnapshotError::RecordLimitExceeded);
            }
            let record = decode_registered_envelope(value.value())
                .map_err(|_| SnapshotError::InvalidCatalogRecord)?;
            if record.family_code != 14
                || record.logical_key != key
                || key.len() != 39
                || key[19] != 1
                || !matches!(u16::from_be_bytes([key[20], key[21]]), 1..=3)
                || key[22] != 1
                || key[23..39] == [0; 16]
            {
                return Err(SnapshotError::InvalidCatalogRecord);
            }
            records.push(record);
        }
        Ok(records)
    }

    /// Copy a bounded family prefix from the pinned catalog snapshot. Callers
    /// that use returned records for authorization or writes must also record
    /// exact-key guards and guard the owning definition's epoch.
    pub fn scan_catalog_family(
        &self,
        family_code: u16,
        max_records: usize,
    ) -> Result<Vec<CatalogRecord>, Box<dyn std::error::Error>> {
        self.scan_catalog_family_limited(family_code, max_records)
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
    }

    pub(crate) fn scan_catalog_family_limited(
        &self,
        family_code: u16,
        max_records: usize,
    ) -> Result<Vec<CatalogRecord>, SnapshotError> {
        if !(1..=16).contains(&family_code)
            || max_records == 0
            || max_records > MAX_CATALOG_FAMILY_SCAN
        {
            return Err(SnapshotError::InvalidCatalogRecord);
        }
        let prefix = family_code.to_be_bytes();
        let table = self
            .transaction
            .open_table(CATALOG)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?;
        let mut records = Vec::new();
        for entry in table
            .range(prefix.as_slice()..)
            .map_err(|error| SnapshotError::Storage(error.to_string()))?
        {
            let (key, value) = entry.map_err(|error| SnapshotError::Storage(error.to_string()))?;
            let key = key.value().to_vec();
            if !key.starts_with(&prefix) {
                break;
            }
            if records.len() == max_records {
                return Err(SnapshotError::RecordLimitExceeded);
            }
            let bytes = value.value().to_vec();
            let record = decode_registered_envelope(&bytes)
                .map_err(|_| SnapshotError::InvalidCatalogRecord)?;
            if record.logical_key != key || record.family_code != family_code {
                return Err(SnapshotError::InvalidCatalogRecord);
            }
            if family_code == 7 {
                decode_catalog_expression_record(&record)
                    .map_err(|_| SnapshotError::InvalidCatalogRecord)?;
            }
            records.push(record);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_key::compose_key;
    use crate::index_locator::IndexLocator;
    use redb::{Database, ReadableDatabase};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn ordered_index_range_honors_inclusive_bounds_and_pages() {
        let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "prdb-index-range-{}-{fixture_id}.redb",
            std::process::id()
        ));
        let database = Database::create(&path).unwrap();
        let index_id = [0x31; 16];
        let write = database.begin_write().unwrap();
        {
            let mut indexes = write.open_table(INDEXES).unwrap();
            for (value, row_byte) in [(1u8, 0x41), (2, 0x42), (3, 0x43), (4, 0x44)] {
                let row_id = [row_byte; 16];
                let key = compose_key(index_id, &[value], row_id);
                let locator = IndexLocator {
                    row_id,
                    row_version: 1,
                };
                indexes
                    .insert(key.as_slice(), locator.encode().as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        let read = database.begin_read().unwrap();
        let snapshot = RowSnapshot::new(read);

        let first = snapshot
            .scan_index_range(
                &index_id,
                Some(IndexRangeBound {
                    tuple: vec![2],
                    inclusive: true,
                }),
                Some(IndexRangeBound {
                    tuple: vec![3],
                    inclusive: true,
                }),
                None,
                1,
            )
            .unwrap();
        assert_eq!(first.candidates.len(), 1);
        assert_eq!(first.candidates[0].key[16], 2);
        let continuation = first.continuation.as_deref().expect("second bound row");

        let second = snapshot
            .scan_index_range(
                &index_id,
                Some(IndexRangeBound {
                    tuple: vec![2],
                    inclusive: true,
                }),
                Some(IndexRangeBound {
                    tuple: vec![3],
                    inclusive: true,
                }),
                Some(continuation),
                1,
            )
            .unwrap();
        assert_eq!(second.candidates.len(), 1);
        assert_eq!(second.candidates[0].key[16], 3);
        assert!(second.continuation.is_none());

        drop(snapshot);
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ordered_index_range_excludes_boundary_tuples_and_rejects_foreign_cursors() {
        let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "prdb-index-range-exclusive-{}-{fixture_id}.redb",
            std::process::id()
        ));
        let database = Database::create(&path).unwrap();
        let index_id = [0x51; 16];
        let foreign_index_id = [0x52; 16];
        let write = database.begin_write().unwrap();
        {
            let mut indexes = write.open_table(INDEXES).unwrap();
            for (value, row_byte) in [(1u8, 0x61), (2, 0x62), (3, 0x63)] {
                let row_id = [row_byte; 16];
                let key = compose_key(index_id, &[value], row_id);
                let locator = IndexLocator {
                    row_id,
                    row_version: 1,
                };
                indexes
                    .insert(key.as_slice(), locator.encode().as_slice())
                    .unwrap();
            }
            let foreign_row_id = [0x64; 16];
            let foreign_key = compose_key(foreign_index_id, &[2], foreign_row_id);
            let foreign_locator = IndexLocator {
                row_id: foreign_row_id,
                row_version: 1,
            };
            indexes
                .insert(foreign_key.as_slice(), foreign_locator.encode().as_slice())
                .unwrap();
        }
        write.commit().unwrap();
        let read = database.begin_read().unwrap();
        let snapshot = RowSnapshot::new(read);

        let page = snapshot
            .scan_index_range(
                &index_id,
                Some(IndexRangeBound {
                    tuple: vec![1],
                    inclusive: false,
                }),
                Some(IndexRangeBound {
                    tuple: vec![3],
                    inclusive: false,
                }),
                None,
                10,
            )
            .unwrap();
        assert_eq!(page.candidates.len(), 1);
        assert_eq!(page.candidates[0].key[16], 2);

        let empty = snapshot
            .scan_index_range(
                &index_id,
                Some(IndexRangeBound {
                    tuple: vec![3],
                    inclusive: false,
                }),
                Some(IndexRangeBound {
                    tuple: vec![2],
                    inclusive: false,
                }),
                None,
                10,
            )
            .unwrap();
        assert!(empty.candidates.is_empty());
        assert!(empty.continuation.is_none());

        let foreign_key = compose_key(foreign_index_id, &[2], [0x64; 16]);
        assert!(
            snapshot
                .scan_index_range(&index_id, None, None, Some(&foreign_key), 10)
                .is_err()
        );

        drop(snapshot);
        drop(database);
        std::fs::remove_file(path).unwrap();
    }
}
