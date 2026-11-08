//! Read-only row access through an invocation's retained redb snapshot.
use crate::catalog::{CatalogRecord, InvalidCatalogRecord, decode_registered_envelope};
use crate::expression::decode_catalog_expression_record;
use crate::index_locator::IndexLocator;
use crate::schema::{CATALOG, ENGINE_META, INDEXES, ROWS};
use redb::ReadTransaction;

pub const MAX_CATALOG_FAMILY_SCAN: usize = 65_536;
pub(crate) const MAX_ROW_SCAN_PAGE: usize = 1_024;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SnapshotError {
    InvalidTableId,
    InvalidCursor,
    InvalidScanLimit,
    InvalidRowKey,
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

impl RowSnapshot {
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

    /// Copy one row value out of its redb guard. No guard or database borrow escapes.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let table = self.transaction.open_table(ROWS)?;
        Ok(table.get(key)?.map(|value| value.value().to_vec()))
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
