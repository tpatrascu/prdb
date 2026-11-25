use super::constraint_validation::*;
use super::*;
use crate::catalog::decode_registered_envelope;
use crate::expression::decode_catalog_expression_record;
use crate::index_locator::IndexLocator;
use crate::row_record::RowRecord;
use crate::schema::{CATALOG, INDEXES, ROW_INDEXES, ROWS};
use redb::{Database, ReadableDatabase, ReadableTable, WriteTransaction};
use std::collections::{HashMap, HashSet};

pub(super) fn decode_principal_kind(key: &[u8], bytes: &[u8]) -> Result<u16, CommitError> {
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

pub(super) fn final_principal_kind(
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

pub(super) fn decode_index_definition(
    key: &[u8],
    bytes: &[u8],
) -> Result<([u8; 16], [u8; 16]), CommitError> {
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

pub(super) fn load_index_definitions(
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

pub(super) fn validate_index_definition_changes(
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
                && !backfill.is_some_and(|plan| plan.schema_rewrite_tables.contains(&table_id))
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

pub(super) fn decode_index_ddl_definition(
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

pub(super) fn map_index_key_error(error: crate::index_key::DeriveIndexKeyError) -> CommitError {
    match error {
        crate::index_key::DeriveIndexKeyError::InvalidDefinition => CommitError::InvalidCatalog,
        crate::index_key::DeriveIndexKeyError::Tuple(_) => CommitError::InvalidRow,
    }
}

pub(super) fn validate_final_catalog_index_definitions(
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

pub(super) fn build_index_ddl_plan(
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
        schema_drop_id: None,
        table_drop_id: None,
        row_changes: Vec::new(),
        schema_rewrite_tables: HashSet::new(),
    };
    let dropped_tables = catalog_writes
        .iter()
        .filter(|write| {
            write.key.starts_with(&5u16.to_be_bytes())
                && write.expected_value.is_some()
                && write.payload.is_none()
        })
        .map(|write| {
            write
                .key
                .get(3..19)
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                .ok_or(CommitError::InvalidCatalog)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if dropped_tables.len() > 1 {
        return Err(CommitError::InvalidCatalog);
    }
    plan.table_drop_id = dropped_tables.first().copied();
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
                let table_write_is_epoch_only =
                    match (write.expected_value.as_deref(), write.payload.as_deref()) {
                        (Some(current), Some(next)) => {
                            let current_record = decode_catalog_record(&write.key, current)?;
                            let next_record = decode_catalog_record(&write.key, next)?;
                            catalog_fields_equal_except(&current_record, &next_record, &[3])
                                && table_schema_version(&write.key, current)?
                                    == table_schema_version(&write.key, next)?
                        }
                        _ => false,
                    };
                if !table_write_is_epoch_only {
                    schema_changed = true;
                }
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

    if let Some(table_id) = plan.table_drop_id {
        let mut expected_rows = Vec::new();
        let rows_already_accounted = plan.expected_rows.contains_key(&table_id);
        for entry in rows.range(table_id.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&table_id) {
                break;
            }
            if key.len() != 32 {
                return Err(CommitError::InvalidRow);
            }
            let bytes = value.value().to_vec();
            let row = RowRecord::decode(&bytes).map_err(|_| CommitError::InvalidRow)?;
            if !rows_already_accounted {
                total_rows = total_rows
                    .checked_add(1)
                    .ok_or(CommitError::TableDropLimitExceeded)?;
                if total_rows > MAX_INDEX_DDL_ROWS_PER_COMMIT {
                    return Err(CommitError::TableDropLimitExceeded);
                }
                total_bytes = total_bytes
                    .checked_add(row.payload.len())
                    .ok_or(CommitError::TableDropLimitExceeded)?;
                if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                    return Err(CommitError::TableDropLimitExceeded);
                }
            }
            let row_key: [u8; 32] = key.try_into().map_err(|_| CommitError::InvalidRow)?;
            if row_key[16..] == [0; 16] {
                return Err(CommitError::InvalidRow);
            }
            expected_rows.push((row_key, row.row_version));
            plan.row_versions.insert(row_key, row.row_version);
            plan.row_changes.push(RowWrite {
                key: row_key,
                expected_version: Some(row.row_version),
                payload: None,
            });
        }
        plan.expected_rows.insert(table_id, expected_rows);
    }

    let column_deletes = catalog_writes
        .iter()
        .filter(|write| {
            write.key.starts_with(&6u16.to_be_bytes())
                && write.expected_value.is_some()
                && write.payload.is_none()
        })
        .collect::<Vec<_>>();
    if plan.table_drop_id.is_none() && column_deletes.len() == 1 {
        let column_write = column_deletes[0];
        let column_id: [u8; 16] = column_write
            .key
            .get(3..19)
            .ok_or(CommitError::InvalidCatalog)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let table_id = column_table_id(
            &column_write.key,
            column_write
                .expected_value
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?,
        )?;
        let table_key = id_catalog_key(5, table_id);
        let table_write = catalog_writes
            .iter()
            .find(|write| write.key == table_key)
            .ok_or(CommitError::InvalidCatalog)?;
        let (Some(old_table_bytes), Some(new_table_bytes)) = (
            table_write.expected_value.as_deref(),
            table_write.payload.as_deref(),
        ) else {
            return Err(CommitError::InvalidCatalog);
        };
        let old_schema = load_final_table_schema(&catalog, &[], table_id)?;
        let new_schema = load_final_table_schema(&catalog, catalog_writes, table_id)?;
        let ordinal = old_schema
            .column_ids
            .iter()
            .position(|candidate| *candidate == column_id)
            .ok_or(CommitError::InvalidCatalog)?;
        let mut expected_column_ids = old_schema.column_ids.clone();
        expected_column_ids.remove(ordinal);
        let mut expected_columns = old_schema.columns.clone();
        expected_columns.remove(ordinal);
        if expected_column_ids != new_schema.column_ids
            || expected_columns != new_schema.columns
            || old_schema.schema_version.checked_add(1) != Some(new_schema.schema_version)
            || table_schema_version(&table_key, new_table_bytes)? != new_schema.schema_version
            || table_schema_version(&table_key, old_table_bytes)? != old_schema.schema_version
            || table_write_epoch(&table_key, new_table_bytes)?
                != table_write_epoch(&table_key, old_table_bytes)?
                    .checked_add(1)
                    .ok_or(CommitError::InvalidCatalog)?
        {
            return Err(CommitError::InvalidCatalog);
        }
        let mut expected_rows = Vec::new();
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
                .ok_or(CommitError::TableDropLimitExceeded)?;
            if total_rows > MAX_INDEX_DDL_ROWS_PER_COMMIT {
                return Err(CommitError::TableDropLimitExceeded);
            }
            let old_bytes = value.value().to_vec();
            let row = RowRecord::decode(&old_bytes).map_err(|_| CommitError::InvalidRow)?;
            total_bytes = total_bytes
                .checked_add(row.payload.len())
                .ok_or(CommitError::TableDropLimitExceeded)?;
            if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                return Err(CommitError::TableDropLimitExceeded);
            }
            let row_key: [u8; 32] = key.try_into().map_err(|_| CommitError::InvalidRow)?;
            if row_key[16..] == [0; 16] {
                return Err(CommitError::InvalidRow);
            }
            let tuple = crate::tuple::decode_tuple(
                &old_schema.columns,
                old_schema.schema_version,
                &row.payload,
            )
            .map_err(|_| CommitError::InvalidRow)?;
            let mut values = tuple.values;
            values.remove(ordinal);
            let payload =
                crate::tuple::encode_tuple(&new_schema.columns, new_schema.schema_version, &values)
                    .map_err(|_| CommitError::InvalidRow)?;
            total_bytes = total_bytes
                .checked_add(payload.len())
                .ok_or(CommitError::TableDropLimitExceeded)?;
            if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                return Err(CommitError::TableDropLimitExceeded);
            }
            expected_rows.push((row_key, row.row_version));
            plan.row_versions.insert(row_key, row.row_version);
            plan.row_changes.push(RowWrite {
                key: row_key,
                expected_version: Some(row.row_version),
                payload: Some(payload),
            });
        }
        plan.expected_rows.insert(table_id, expected_rows);
        plan.schema_rewrite_tables.insert(table_id);
    }

    let column_adds = catalog_writes
        .iter()
        .filter(|write| {
            write.key.starts_with(&6u16.to_be_bytes())
                && write.expected_value.is_none()
                && write.payload.is_some()
        })
        .collect::<Vec<_>>();
    let column_add_targets_existing_table = column_adds.iter().any(|write| {
        write
            .payload
            .as_deref()
            .and_then(|payload| column_table_id(&write.key, payload).ok())
            .is_some_and(|table_id| {
                let table_key = id_catalog_key(5, table_id);
                catalog_writes.iter().any(|table_write| {
                    table_write.key == table_key && table_write.expected_value.is_some()
                })
            })
    });
    if plan.table_drop_id.is_none() && column_adds.len() == 1 && column_add_targets_existing_table {
        let column_write = column_adds[0];
        let column_id: [u8; 16] = column_write
            .key
            .get(3..19)
            .ok_or(CommitError::InvalidCatalog)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let column_bytes = column_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        let table_id = column_table_id(&column_write.key, column_bytes)?;
        let table_key = id_catalog_key(5, table_id);
        let table_write = catalog_writes
            .iter()
            .find(|write| write.key == table_key)
            .ok_or(CommitError::InvalidCatalog)?;
        let (Some(old_table_bytes), Some(new_table_bytes)) = (
            table_write.expected_value.as_deref(),
            table_write.payload.as_deref(),
        ) else {
            return Err(CommitError::InvalidCatalog);
        };
        let old_schema = load_final_table_schema(&catalog, &[], table_id)?;
        let new_schema = load_final_table_schema(&catalog, catalog_writes, table_id)?;
        let ordinal = catalog_field_u32(&column_write.key, column_bytes, 6, 2)? as usize;
        if ordinal != old_schema.column_ids.len()
            || new_schema.column_ids.len() != old_schema.column_ids.len() + 1
            || new_schema.column_ids[..old_schema.column_ids.len()] != old_schema.column_ids
            || new_schema.columns[..old_schema.columns.len()] != old_schema.columns
            || new_schema.column_ids.last() != Some(&column_id)
            || old_schema.schema_version.checked_add(1) != Some(new_schema.schema_version)
            || table_schema_version(&table_key, new_table_bytes)? != new_schema.schema_version
            || table_schema_version(&table_key, old_table_bytes)? != old_schema.schema_version
            || table_write_epoch(&table_key, new_table_bytes)?
                != table_write_epoch(&table_key, old_table_bytes)?
                    .checked_add(1)
                    .ok_or(CommitError::InvalidCatalog)?
        {
            return Err(CommitError::InvalidCatalog);
        }
        let nullable = new_schema
            .columns
            .last()
            .ok_or(CommitError::InvalidCatalog)?
            .nullable;
        let add_default_id = catalog_field_bytes(&column_write.key, column_bytes, 6, 10)
            .ok()
            .map(|bytes| bytes.try_into().map_err(|_| CommitError::InvalidCatalog))
            .transpose()?;
        let current_default_id = catalog_field_bytes(&column_write.key, column_bytes, 6, 7)
            .ok()
            .map(|bytes| bytes.try_into().map_err(|_| CommitError::InvalidCatalog))
            .transpose()?;
        if add_default_id != current_default_id {
            return Err(CommitError::InvalidCatalog);
        }
        let backfill_value = if let Some(expression_id) = add_default_id {
            let expression_key = id_catalog_key(7, expression_id);
            let expression_bytes = final_catalog_value(&catalog, catalog_writes, &expression_key)?
                .ok_or(CommitError::InvalidCatalog)?;
            let expression = decode_catalog_expression_record(&decode_catalog_record(
                &expression_key,
                &expression_bytes,
            )?)
            .map_err(|_| CommitError::InvalidCatalog)?;
            if expression.purpose != crate::expression::ExpressionPurpose::Default
                || !expression.dependencies.is_empty()
            {
                return Err(CommitError::InvalidCatalog);
            }
            let crate::authorization::Expr::Literal(value) = expression.expression else {
                return Err(CommitError::InvalidCatalog);
            };
            value
        } else if nullable {
            None
        } else {
            return Err(CommitError::InvalidCatalog);
        };
        let mut expected_rows = Vec::new();
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
                .ok_or(CommitError::ColumnAddLimitExceeded)?;
            if total_rows > MAX_INDEX_DDL_ROWS_PER_COMMIT {
                return Err(CommitError::ColumnAddLimitExceeded);
            }
            let old_bytes = value.value().to_vec();
            total_bytes = total_bytes
                .checked_add(old_bytes.len())
                .ok_or(CommitError::ColumnAddLimitExceeded)?;
            if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                return Err(CommitError::ColumnAddLimitExceeded);
            }
            let row = RowRecord::decode(&old_bytes).map_err(|_| CommitError::InvalidRow)?;
            let row_key: [u8; 32] = key.try_into().map_err(|_| CommitError::InvalidRow)?;
            if row_key[16..] == [0; 16] {
                return Err(CommitError::InvalidRow);
            }
            let mut tuple = crate::tuple::decode_tuple(
                &old_schema.columns,
                old_schema.schema_version,
                &row.payload,
            )
            .map_err(|_| CommitError::InvalidRow)?;
            tuple.values.push(backfill_value.clone());
            let payload = crate::tuple::encode_tuple(
                &new_schema.columns,
                new_schema.schema_version,
                &tuple.values,
            )
            .map_err(|_| CommitError::InvalidRow)?;
            total_bytes = total_bytes
                .checked_add(payload.len())
                .ok_or(CommitError::ColumnAddLimitExceeded)?;
            if total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT {
                return Err(CommitError::ColumnAddLimitExceeded);
            }
            expected_rows.push((row_key, row.row_version));
            plan.row_versions.insert(row_key, row.row_version);
            plan.row_changes.push(RowWrite {
                key: row_key,
                expected_version: Some(row.row_version),
                payload: Some(payload),
            });
        }
        plan.expected_rows.insert(table_id, expected_rows);
        plan.schema_rewrite_tables.insert(table_id);
    }

    let indexes = read.open_table(INDEXES).map_err(storage_error)?;
    let reverse_indexes = read.open_table(ROW_INDEXES).map_err(storage_error)?;
    for (index_id, table_id, key, old_bytes, new_bytes) in updated_indexes {
        if !seen_index_ids.insert(index_id) {
            return Err(CommitError::InvalidCatalog);
        }
        let table_key = id_catalog_key(5, table_id);
        let old_schema = load_final_table_schema(&catalog, &[], table_id)?;
        let new_schema = load_final_table_schema(&catalog, catalog_writes, table_id)?;
        let old_definition = decode_index_ddl_definition(&key, &old_bytes, &old_schema)?;
        let new_definition = decode_index_ddl_definition(&key, &new_bytes, &new_schema)?;
        plan.index_tables.insert(index_id, table_id);
        plan.rebuilt_indexes.insert(index_id);
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
            let index_prefix = index_id.to_vec();
            if indexes
                .range(index_prefix.as_slice()..)
                .map_err(storage_error)?
                .next()
                .transpose()
                .map_err(storage_error)?
                .is_some_and(|(key, _)| key.value().starts_with(&index_prefix))
            {
                return Err(CommitError::InvalidIndex);
            }
            plan.expected_rows.insert(table_id, Vec::new());
            continue;
        }
        for write in catalog_writes {
            let family = write
                .key
                .get(..2)
                .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
                .map(u16::from_be_bytes);
            if family == Some(5)
                && write.key == table_key
                && write.expected_value != write.payload
                && !plan.schema_rewrite_tables.contains(&table_id)
            {
                return Err(CommitError::InvalidCatalog);
            }
            if family == Some(6) && write.expected_value != write.payload {
                for bytes in [write.expected_value.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    if column_table_id(&write.key, bytes)? == table_id
                        && !plan.schema_rewrite_tables.contains(&table_id)
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                }
            }
        }
        let mut expected_rows = Vec::with_capacity(persisted_rows.len());
        for (row_key, row) in persisted_rows {
            expected_rows.push((row_key, row.row_version));
            plan.row_versions.insert(row_key, row.row_version);
            let row_id = row_key[16..].try_into().unwrap();
            let new_payload = plan
                .row_changes
                .iter()
                .find(|write| write.key == row_key)
                .and_then(|write| write.payload.as_deref())
                .unwrap_or(&row.payload);
            let old_key = crate::index_key::derive_index_key(
                old_definition.index_id,
                row_id,
                old_schema.schema_version,
                &old_schema.column_ids,
                &old_schema.columns,
                &row.payload,
                &old_definition.column_ids,
            )
            .map_err(map_index_key_error)?;
            let new_key = crate::index_key::derive_index_key(
                new_definition.index_id,
                row_id,
                new_schema.schema_version,
                &new_schema.column_ids,
                &new_schema.columns,
                new_payload,
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

pub(super) fn validate_index_ddl_snapshot(
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
            let row = RowRecord::decode(value.value()).map_err(|_| CommitError::InvalidRow)?;
            let accounted_row_bytes = if plan.table_drop_id == Some(*table_id) {
                row.payload.len()
            } else {
                value.value().len()
            };
            total_bytes = total_bytes
                .checked_add(accounted_row_bytes)
                .ok_or(CommitError::Conflict)?;
            if total_rows > MAX_INDEX_DDL_ROWS_PER_COMMIT
                || total_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT
            {
                return Err(CommitError::Conflict);
            }
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

pub(super) fn reverse_index_key(row_key: &[u8; 32], index_key: &[u8]) -> Vec<u8> {
    let mut key = row_key.to_vec();
    key.extend_from_slice(&index_key[..index_key.len() - 16]);
    key
}

pub(super) fn retained_index_mutations(
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
