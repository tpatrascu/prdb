use super::*;
use crate::catalog::decode_registered_envelope;
use crate::schema::{
    CATALOG, COLUMN_LOOKUPS, DEPENDENCIES, GRANTS, INDEX_NAMES, MEMBERSHIPS, OBJECT_OWNERS,
    POLICIES, PRINCIPAL_NAMES, RELATION_NAMES, SCHEMA_NAMES, TABLE_CONSTRAINTS, TABLE_INDEXES,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableHandle};
use std::collections::{HashMap, HashSet};

pub(super) fn collect_principal_name_entries<T>(
    catalog: &T,
) -> Result<HashMap<Vec<u8>, [u8; 16]>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 10u16.to_be_bytes();
    let mut entries = HashMap::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if key.len() != 19 || key[2] != 1 {
            return Err(StartupError::InvalidCatalog);
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 10 || record.logical_key != key {
            return Err(StartupError::InvalidCatalog);
        }
        let name = record
            .fields
            .iter()
            .find(|field| field.tag == 2)
            .ok_or(StartupError::InvalidCatalog)?;
        let principal_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        if name.payload.is_empty()
            || name.payload.len() > 4096
            || entries.insert(name.payload.clone(), principal_id).is_some()
        {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(entries)
}

/// Rebuild the derived principal-name lookup if it is missing or stale. The
/// family-10 records remain authoritative; this never changes catalog data.
pub(super) fn ensure_principal_name_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == PRINCIPAL_NAMES.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_principal_name_entries(&catalog)?;
        let index = read.open_table(PRINCIPAL_NAMES).map_err(storage_error)?;
        let mut actual = HashMap::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (name, id) = entry.map_err(storage_error)?;
            match id.value().try_into() {
                Ok(id) => {
                    actual.insert(name.value().to_vec(), id);
                }
                Err(_) => well_formed = false,
            }
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);

    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let entries = collect_principal_name_entries(&catalog)?;
    let mut name_index = write.open_table(PRINCIPAL_NAMES).map_err(storage_error)?;
    let stale_keys = name_index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(name, _)| name.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for name in stale_keys {
        name_index.remove(name.as_slice()).map_err(storage_error)?;
    }
    for (name, id) in entries {
        name_index
            .insert(name.as_slice(), &id[..])
            .map_err(storage_error)?;
    }
    drop(name_index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_schema_name_entries<T>(
    catalog: &T,
) -> Result<HashMap<Vec<u8>, [u8; 16]>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 2u16.to_be_bytes();
    let mut entries = HashMap::new();
    let mut count = 0usize;
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if key.len() != 19 || key[2] != 1 {
            continue;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 2 || record.logical_key != key {
            return Err(StartupError::InvalidCatalog);
        }
        if field_u16(&record, 1)? != 1 {
            continue;
        }
        count += 1;
        if count > MAX_CATALOG_RECORDS_PER_FAMILY {
            return Err(StartupError::InvalidCatalog);
        }
        let name = record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or(StartupError::InvalidCatalog)?
            .payload
            .clone();
        let schema_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        if name.is_empty() || name.len() > 4096 || entries.insert(name, schema_id).is_some() {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(entries)
}

/// Rebuild the derived schema-name lookup from authoritative family-2 rows.
pub(super) fn ensure_schema_name_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == SCHEMA_NAMES.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_schema_name_entries(&catalog)?;
        let index = read.open_table(SCHEMA_NAMES).map_err(storage_error)?;
        let mut actual = HashMap::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (name, id) = entry.map_err(storage_error)?;
            match <[u8; 16]>::try_from(id.value()) {
                Ok(id) => {
                    actual.insert(name.value().to_vec(), id);
                }
                Err(_) => well_formed = false,
            }
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);

    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let entries = collect_schema_name_entries(&catalog)?;
    let mut name_index = write.open_table(SCHEMA_NAMES).map_err(storage_error)?;
    let stale_keys = name_index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(name, _)| name.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for name in stale_keys {
        name_index.remove(name.as_slice()).map_err(storage_error)?;
    }
    for (name, id) in entries {
        name_index
            .insert(name.as_slice(), &id[..])
            .map_err(storage_error)?;
    }
    drop(name_index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_relation_name_entries<T>(
    catalog: &T,
) -> Result<HashMap<Vec<u8>, [u8; 16]>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 5u16.to_be_bytes();
    let mut entries = HashMap::new();
    let mut count = 0usize;
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let table_record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if table_record.family_code != 5 || table_record.logical_key != key || key.len() != 19 {
            return Err(StartupError::InvalidCatalog);
        }
        count += 1;
        if count > MAX_CATALOG_RECORDS_PER_FAMILY {
            return Err(StartupError::InvalidCatalog);
        }
        let relation_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        let schema_id = field_id(&table_record, 1)?;
        let object_key = {
            let mut object_key = 2u16.to_be_bytes().to_vec();
            object_key.push(1);
            object_key.extend_from_slice(&relation_id);
            object_key
        };
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(storage_error)?
            .ok_or(StartupError::InvalidCatalog)?;
        let object = decode_registered_envelope(object_bytes.value())
            .map_err(|_| StartupError::InvalidCatalog)?;
        if object.family_code != 2
            || object.logical_key != object_key
            || field_u16(&object, 1)? != 3
            || object
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .is_some_and(|parent| parent.payload.as_slice() != schema_id)
        {
            return Err(StartupError::InvalidCatalog);
        }
        let name = object
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or(StartupError::InvalidCatalog)?
            .payload
            .clone();
        if name.is_empty() || name.len() > 4096 {
            return Err(StartupError::InvalidCatalog);
        }
        let mut index_key = Vec::with_capacity(16 + name.len());
        index_key.extend_from_slice(&schema_id);
        index_key.extend_from_slice(&name);
        if entries.insert(index_key, relation_id).is_some() {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(entries)
}

/// Rebuild schema-scoped relation-name entries from authoritative table and object rows.
pub(super) fn ensure_relation_name_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == RELATION_NAMES.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_relation_name_entries(&catalog)?;
        let index = read.open_table(RELATION_NAMES).map_err(storage_error)?;
        let mut actual = HashMap::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, id) = entry.map_err(storage_error)?;
            match <[u8; 16]>::try_from(id.value()) {
                Ok(id) => {
                    actual.insert(key.value().to_vec(), id);
                }
                Err(_) => well_formed = false,
            }
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let entries = collect_relation_name_entries(&catalog)?;
    let mut index = write.open_table(RELATION_NAMES).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for (key, id) in entries {
        index
            .insert(key.as_slice(), &id[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_column_lookup_entries<T>(
    catalog: &T,
) -> Result<HashMap<Vec<u8>, [u8; 16]>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 6u16.to_be_bytes();
    let mut entries = HashMap::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 6 || record.logical_key != key || key.len() != 19 || key[2] != 1 {
            return Err(StartupError::InvalidCatalog);
        }
        let column_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        let table_id = field_id(&record, 1)?;
        let ordinal = field_u32(&record, 2)?;
        let name = field_payload(&record, 3)?;
        if column_id == [0; 16] || table_id == [0; 16] || name.is_empty() || name.len() > 4096 {
            return Err(StartupError::InvalidCatalog);
        }
        let mut by_name = vec![1];
        by_name.extend_from_slice(&table_id);
        by_name.extend_from_slice(name);
        let mut by_ordinal = vec![2];
        by_ordinal.extend_from_slice(&table_id);
        by_ordinal.extend_from_slice(&ordinal.to_be_bytes());
        if entries.insert(by_name, column_id).is_some()
            || entries.insert(by_ordinal, column_id).is_some()
        {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(entries)
}

/// Rebuild both table/name and table/ordinal lookups from family-6 records.
pub(super) fn ensure_column_lookup_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == COLUMN_LOOKUPS.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_column_lookup_entries(&catalog)?;
        let index = read.open_table(COLUMN_LOOKUPS).map_err(storage_error)?;
        let mut actual = HashMap::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if value.value().len() != 16 {
                well_formed = false;
                continue;
            }
            let column_id: [u8; 16] = value.value().try_into().unwrap();
            if actual.insert(key.value().to_vec(), column_id).is_some() {
                well_formed = false;
            }
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_column_lookup_entries(&catalog)?;
    let mut index = write.open_table(COLUMN_LOOKUPS).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for (key, column_id) in expected {
        index
            .insert(key.as_slice(), &column_id[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_index_name_entries<T>(
    catalog: &T,
) -> Result<HashMap<Vec<u8>, [u8; 16]>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 2u16.to_be_bytes();
    let mut entries = HashMap::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 2 || record.logical_key != key || key.len() != 19 || key[2] != 1 {
            return Err(StartupError::InvalidCatalog);
        }
        if field_u16(&record, 1)? != 7 {
            continue;
        }
        let index_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        let Some(schema_id) = field_optional_id(&record, 2)? else {
            continue;
        };
        let name = field_payload(&record, 3)?;
        if index_id == [0; 16] || schema_id == [0; 16] || name.is_empty() || name.len() > 4096 {
            return Err(StartupError::InvalidCatalog);
        }
        let mut name_key = Vec::with_capacity(16 + name.len());
        name_key.extend_from_slice(&schema_id);
        name_key.extend_from_slice(name);
        if entries.insert(name_key, index_id).is_some() {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(entries)
}

/// Rebuild schema-scoped index-name entries from authoritative family-2 objects.
pub(super) fn ensure_index_name_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == INDEX_NAMES.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_index_name_entries(&catalog)?;
        let index = read.open_table(INDEX_NAMES).map_err(storage_error)?;
        let mut actual = HashMap::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if value.value().len() != 16 {
                well_formed = false;
                continue;
            }
            let index_id: [u8; 16] = value.value().try_into().unwrap();
            if actual.insert(key.value().to_vec(), index_id).is_some() {
                well_formed = false;
            }
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_index_name_entries(&catalog)?;
    let mut index = write.open_table(INDEX_NAMES).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for (key, index_id) in expected {
        index
            .insert(key.as_slice(), &index_id[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_table_index_entries<T>(catalog: &T) -> Result<HashSet<Vec<u8>>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 9u16.to_be_bytes();
    let mut entries = HashSet::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 9 || record.logical_key != key || key.len() != 19 || key[2] != 1 {
            return Err(StartupError::InvalidCatalog);
        }
        let table_id = field_id(&record, 1)?;
        let index_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        if table_id == [0; 16] || index_id == [0; 16] {
            return Err(StartupError::InvalidCatalog);
        }
        let mut index_key = Vec::with_capacity(32);
        index_key.extend_from_slice(&table_id);
        index_key.extend_from_slice(&index_id);
        if !entries.insert(index_key) {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(entries)
}

/// Rebuild table-scoped family-9 definition lookups from authoritative rows.
pub(super) fn ensure_table_index_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == TABLE_INDEXES.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_table_index_entries(&catalog)?;
        let index = read.open_table(TABLE_INDEXES).map_err(storage_error)?;
        let mut actual = HashSet::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if key.value().len() != 32 || !value.value().is_empty() {
                well_formed = false;
            }
            actual.insert(key.value().to_vec());
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_table_index_entries(&catalog)?;
    let mut index = write.open_table(TABLE_INDEXES).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for key in expected {
        index
            .insert(key.as_slice(), &b""[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_table_constraint_entries<T>(
    catalog: &T,
) -> Result<HashSet<Vec<u8>>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 8u16.to_be_bytes();
    let mut entries = HashSet::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 8 || record.logical_key != key || key.len() != 19 || key[2] != 1 {
            return Err(StartupError::InvalidCatalog);
        }
        let table_id = field_id(&record, 1)?;
        let constraint_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        if table_id == [0; 16] || constraint_id == [0; 16] {
            return Err(StartupError::InvalidCatalog);
        }
        let mut index_key = Vec::with_capacity(32);
        index_key.extend_from_slice(&table_id);
        index_key.extend_from_slice(&constraint_id);
        if !entries.insert(index_key) {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(entries)
}

/// Rebuild table-scoped family-8 constraint lookups from authoritative rows.
pub(super) fn ensure_table_constraint_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == TABLE_CONSTRAINTS.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_table_constraint_entries(&catalog)?;
        let index = read.open_table(TABLE_CONSTRAINTS).map_err(storage_error)?;
        let mut actual = HashSet::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if key.value().len() != 32 || !value.value().is_empty() {
                well_formed = false;
            }
            actual.insert(key.value().to_vec());
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_table_constraint_entries(&catalog)?;
    let mut index = write.open_table(TABLE_CONSTRAINTS).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for key in expected {
        index
            .insert(key.as_slice(), &b""[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_object_owner_entries<T>(catalog: &T) -> Result<HashSet<Vec<u8>>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 2u16.to_be_bytes();
    let mut entries = HashSet::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 2 || record.logical_key != key || key.len() != 19 || key[2] != 1 {
            return Err(StartupError::InvalidCatalog);
        }
        let object_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        let owner_id = field_id(&record, 4)?;
        if object_id == [0; 16] || owner_id == [0; 16] {
            return Err(StartupError::InvalidCatalog);
        }
        let mut index_key = Vec::with_capacity(32);
        index_key.extend_from_slice(&owner_id);
        index_key.extend_from_slice(&object_id);
        if !entries.insert(index_key) {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(entries)
}

/// Rebuild family-2 owner lookups from authoritative object records.
pub(super) fn ensure_object_owner_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == OBJECT_OWNERS.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_object_owner_entries(&catalog)?;
        let index = read.open_table(OBJECT_OWNERS).map_err(storage_error)?;
        let mut actual = HashSet::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if key.value().len() != 32 || !value.value().is_empty() {
                well_formed = false;
            }
            actual.insert(key.value().to_vec());
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_object_owner_entries(&catalog)?;
    let mut index = write.open_table(OBJECT_OWNERS).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for key in expected {
        index
            .insert(key.as_slice(), &b""[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_membership_index_keys<T>(
    catalog: &T,
) -> Result<HashSet<Vec<u8>>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 11u16.to_be_bytes();
    let mut keys = HashSet::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 11
            || record.logical_key != key
            || key.len() != 36
            || key[2] != 1
            || key[19] != 1
        {
            return Err(StartupError::InvalidCatalog);
        }
        let member_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        let role_id: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        if member_id == [0; 16] || role_id == [0; 16] {
            return Err(StartupError::InvalidCatalog);
        }
        for (direction, first, second) in [(1u8, member_id, role_id), (2u8, role_id, member_id)] {
            let mut index_key = Vec::with_capacity(33);
            index_key.push(direction);
            index_key.extend_from_slice(&first);
            index_key.extend_from_slice(&second);
            if !keys.insert(index_key) {
                return Err(StartupError::InvalidCatalog);
            }
        }
    }
    Ok(keys)
}

/// Rebuild both direct-membership directions from authoritative family-11 rows.
pub(super) fn ensure_membership_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == MEMBERSHIPS.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_membership_index_keys(&catalog)?;
        let index = read.open_table(MEMBERSHIPS).map_err(storage_error)?;
        let mut actual = HashSet::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if !value.value().is_empty() {
                well_formed = false;
            }
            actual.insert(key.value().to_vec());
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_membership_index_keys(&catalog)?;
    let mut index = write.open_table(MEMBERSHIPS).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for key in expected {
        index
            .insert(key.as_slice(), &b""[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_grant_index_keys<T>(catalog: &T) -> Result<HashSet<Vec<u8>>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 12u16.to_be_bytes();
    let mut keys = HashSet::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 12
            || record.logical_key != key
            || key.len() != 59
            || key[2] != 1
            || key[19] != 1
            || key[36] != 1
            || key[39] != 1
            || key[42] != 1
        {
            return Err(StartupError::InvalidCatalog);
        }
        let grantee: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        let object: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        if grantee == [0; 16] || object == [0; 16] {
            return Err(StartupError::InvalidCatalog);
        }
        for (direction, indexed_id) in [(1u8, grantee), (2u8, object)] {
            let mut index_key = Vec::with_capacity(76);
            index_key.push(direction);
            index_key.extend_from_slice(&indexed_id);
            index_key.extend_from_slice(key);
            if !keys.insert(index_key) {
                return Err(StartupError::InvalidCatalog);
            }
        }
    }
    Ok(keys)
}

/// Rebuild grant indexes ordered by grantee and protected object.
pub(super) fn ensure_grant_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == GRANTS.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_grant_index_keys(&catalog)?;
        let index = read.open_table(GRANTS).map_err(storage_error)?;
        let mut actual = HashSet::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if !value.value().is_empty() {
                well_formed = false;
            }
            actual.insert(key.value().to_vec());
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_grant_index_keys(&catalog)?;
    let mut index = write.open_table(GRANTS).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for key in expected {
        index
            .insert(key.as_slice(), &b""[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_policy_index_keys<T>(catalog: &T) -> Result<HashSet<Vec<u8>>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 13u16.to_be_bytes();
    let mut keys = HashSet::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 13 || record.logical_key != key || key.len() != 19 || key[2] != 1 {
            return Err(StartupError::InvalidCatalog);
        }
        let table_id = field_id(&record, 1)?;
        let operation = field_u16(&record, 3)?;
        let policy_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        if table_id == [0; 16] || policy_id == [0; 16] || !matches!(operation, 1..=4) {
            return Err(StartupError::InvalidCatalog);
        }
        let mut index_key = Vec::with_capacity(34);
        index_key.extend_from_slice(&table_id);
        index_key.extend_from_slice(&operation.to_be_bytes());
        index_key.extend_from_slice(&policy_id);
        if !keys.insert(index_key) {
            return Err(StartupError::InvalidCatalog);
        }
    }
    Ok(keys)
}

/// Rebuild the table-and-operation policy index from authoritative family-13 rows.
pub(super) fn ensure_policy_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == POLICIES.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_policy_index_keys(&catalog)?;
        let index = read.open_table(POLICIES).map_err(storage_error)?;
        let mut actual = HashSet::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if !value.value().is_empty() {
                well_formed = false;
            }
            actual.insert(key.value().to_vec());
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_policy_index_keys(&catalog)?;
    let mut index = write.open_table(POLICIES).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for key in expected {
        index
            .insert(key.as_slice(), &b""[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}

pub(super) fn collect_dependency_index_keys<T>(
    catalog: &T,
) -> Result<HashSet<Vec<u8>>, StartupError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = 15u16.to_be_bytes();
    let mut keys = HashSet::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let record =
            decode_registered_envelope(value.value()).map_err(|_| StartupError::InvalidCatalog)?;
        if record.family_code != 15
            || record.logical_key != key
            || key.len() != 39
            || key[2] != 1
            || key[19] != 1
            || key[36] != 1
        {
            return Err(StartupError::InvalidCatalog);
        }
        let dependent: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        let referenced: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| StartupError::InvalidCatalog)?;
        let kind = u16::from_be_bytes([key[37], key[38]]);
        if dependent == [0; 16] || referenced == [0; 16] || !matches!(kind, 1..=6) {
            return Err(StartupError::InvalidCatalog);
        }
        for (direction, first, second) in
            [(1u8, dependent, referenced), (2u8, referenced, dependent)]
        {
            let mut index_key = Vec::with_capacity(35);
            index_key.push(direction);
            index_key.extend_from_slice(&first);
            index_key.extend_from_slice(&second);
            index_key.extend_from_slice(&kind.to_be_bytes());
            if !keys.insert(index_key) {
                return Err(StartupError::InvalidCatalog);
            }
        }
    }
    Ok(keys)
}

/// Rebuild dependency lookups in both directions from authoritative family-15 rows.
pub(super) fn ensure_dependency_index(database: &Database) -> Result<(), StartupError> {
    let read = database.begin_read().map_err(storage_error)?;
    let has_index = read
        .list_tables()
        .map_err(storage_error)?
        .any(|table| table.name() == DEPENDENCIES.name());
    if has_index {
        let catalog = read.open_table(CATALOG).map_err(storage_error)?;
        let expected = collect_dependency_index_keys(&catalog)?;
        let index = read.open_table(DEPENDENCIES).map_err(storage_error)?;
        let mut actual = HashSet::new();
        let mut well_formed = true;
        for entry in index.iter().map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            if !value.value().is_empty() {
                well_formed = false;
            }
            actual.insert(key.value().to_vec());
        }
        if well_formed && actual == expected {
            return Ok(());
        }
    }
    drop(read);
    let write = database.begin_write().map_err(storage_error)?;
    let catalog = write.open_table(CATALOG).map_err(storage_error)?;
    let expected = collect_dependency_index_keys(&catalog)?;
    let mut index = write.open_table(DEPENDENCIES).map_err(storage_error)?;
    let stale_keys = index
        .iter()
        .map_err(storage_error)?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(storage_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in stale_keys {
        index.remove(key.as_slice()).map_err(storage_error)?;
    }
    for key in expected {
        index
            .insert(key.as_slice(), &b""[..])
            .map_err(storage_error)?;
    }
    drop(index);
    drop(catalog);
    write.commit().map_err(storage_error)?;
    Ok(())
}
