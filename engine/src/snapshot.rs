//! Read-only row access through an invocation's retained redb snapshot.
use crate::catalog::{CatalogRecord, InvalidCatalogRecord, decode_registered_envelope};
use crate::expression::decode_catalog_expression_record;
use crate::index_locator::IndexLocator;
use crate::schema::{CATALOG, INDEXES, ROWS};
use redb::ReadTransaction;

pub const MAX_CATALOG_FAMILY_SCAN: usize = 65_536;

/// Owns one redb read transaction for the lifetime of the invocation snapshot.
pub struct RowSnapshot {
    transaction: ReadTransaction,
}

impl RowSnapshot {
    pub fn new(transaction: ReadTransaction) -> Self {
        Self { transaction }
    }

    /// Copy one row value out of its redb guard. No guard or database borrow escapes.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let table = self.transaction.open_table(ROWS)?;
        Ok(table.get(key)?.map(|value| value.value().to_vec()))
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
        if !(1..=16).contains(&family_code)
            || max_records == 0
            || max_records > MAX_CATALOG_FAMILY_SCAN
        {
            return Err(InvalidCatalogRecord.into());
        }
        let prefix = family_code.to_be_bytes();
        let table = self.transaction.open_table(CATALOG)?;
        let mut records = Vec::new();
        for entry in table.range(prefix.as_slice()..)? {
            let (key, value) = entry?;
            let key = key.value().to_vec();
            if !key.starts_with(&prefix) {
                break;
            }
            if records.len() == max_records {
                return Err(InvalidCatalogRecord.into());
            }
            let bytes = value.value().to_vec();
            let record = decode_registered_envelope(&bytes)?;
            if record.logical_key != key || record.family_code != family_code {
                return Err(InvalidCatalogRecord.into());
            }
            if family_code == 7 {
                decode_catalog_expression_record(&record)?;
            }
            records.push(record);
        }
        Ok(records)
    }
}
