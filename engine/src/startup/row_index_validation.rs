use super::*;
use crate::index_key::{DerivedIndexKey, derive_index_key, encode_sql_tuple};
use crate::index_locator::IndexLocator;
use crate::row_record::RowRecord;
use crate::schema::{INDEX_NAMES, INDEXES, RELATION_NAMES, ROW_INDEXES, ROWS};
use crate::tuple::decode_tuple;
use redb::{ReadTransaction, ReadableTable};
use std::collections::{HashMap, HashSet};

pub(super) fn validate_name_domains(read: &ReadTransaction) -> Result<(), StartupError> {
    let relation_names = read.open_table(RELATION_NAMES).map_err(storage_error)?;
    let index_names = read.open_table(INDEX_NAMES).map_err(storage_error)?;
    for entry in index_names.iter().map_err(storage_error)? {
        let (key, _) = entry.map_err(storage_error)?;
        if relation_names
            .get(key.value())
            .map_err(storage_error)?
            .is_some()
        {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(())
}

pub(super) fn validate_rows(
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

pub(super) fn validate_foreign_key_rows(
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

pub(super) fn validate_indexes(
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

pub(super) fn derive_persisted_index_key(
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
